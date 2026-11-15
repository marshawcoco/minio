// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	xioutil "github.com/minio/minio/internal/ioutil"
)

// ecDirectReadHedgeDelay bounds how long a spanned-shard direct read waits on a slow drive
// before abandoning the stalled reader and falling back to full Reed-Solomon reconstruction.
var ecDirectReadHedgeDelay = 100 * time.Millisecond

// Reads in parallel from readers.
type parallelReader struct {
	readers       []io.ReaderAt
	orgReaders    []io.ReaderAt
	dataBlocks    int
	offset        int64
	shardSize     int64
	shardFileSize int64
	buf           [][]byte
	readerToBuf   []int
	stashBuffer   []byte
}

// newParallelReader returns parallelReader.
func newParallelReader(readers []io.ReaderAt, e Erasure, offset, totalLength int64) *parallelReader {
	r2b := make([]int, len(readers))
	for i := range r2b {
		r2b[i] = i
	}
	bufs := make([][]byte, len(readers))
	shardSize := int(e.ShardSize())
	var b []byte

	// We should always have enough capacity, but older objects may be bigger
	// we do not need stashbuffer for them.
	if globalBytePoolCap.Load().WidthCap() >= len(readers)*shardSize {
		// Fill buffers
		b = globalBytePoolCap.Load().Get()
		// Seed the buffers.
		for i := range bufs {
			bufs[i] = b[i*shardSize : (i+1)*shardSize]
		}
	}

	return &parallelReader{
		readers:       readers,
		orgReaders:    readers,
		dataBlocks:    e.dataBlocks,
		offset:        (offset / e.blockSize) * e.ShardSize(),
		shardSize:     e.ShardSize(),
		shardFileSize: e.ShardFileSize(totalLength),
		buf:           make([][]byte, len(readers)),
		readerToBuf:   r2b,
		stashBuffer:   b,
	}
}

// Done will release any resources used by the parallelReader.
func (p *parallelReader) Done() {
	if p.stashBuffer != nil {
		globalBytePoolCap.Load().Put(p.stashBuffer)
		p.stashBuffer = nil
	}
}

// preferReaders can mark readers as preferred.
// These will be chosen before others.
func (p *parallelReader) preferReaders(prefer []bool) {
	if len(prefer) != len(p.orgReaders) {
		return
	}
	// Copy so we don't change our input.
	tmp := make([]io.ReaderAt, len(p.orgReaders))
	copy(tmp, p.orgReaders)
	p.readers = tmp
	// next is the next non-preferred index.
	next := 0
	for i, ok := range prefer {
		if !ok || p.readers[i] == nil {
			continue
		}
		if i == next {
			next++
			continue
		}
		// Move reader with index i to index next.
		// Do this by swapping next and i
		p.readers[next], p.readers[i] = p.readers[i], p.readers[next]
		p.readerToBuf[next] = i
		p.readerToBuf[i] = next
		next++
	}
}

// Returns if buf can be erasure decoded.
func (p *parallelReader) canDecode(buf [][]byte) bool {
	bufCount := 0
	for _, b := range buf {
		if len(b) > 0 {
			bufCount++
		}
	}
	return bufCount >= p.dataBlocks
}

// Read reads from readers in parallel. Returns p.dataBlocks number of bufs.
func (p *parallelReader) Read(dst [][]byte) ([][]byte, error) {
	newBuf := dst
	if len(dst) != len(p.readers) {
		newBuf = make([][]byte, len(p.readers))
	} else {
		for i := range newBuf {
			newBuf[i] = newBuf[i][:0]
		}
	}
	var newBufLK sync.RWMutex

	if p.offset+p.shardSize > p.shardFileSize {
		p.shardSize = p.shardFileSize - p.offset
	}
	if p.shardSize == 0 {
		return newBuf, nil
	}

	readTriggerCh := make(chan bool, len(p.readers))
	defer xioutil.SafeClose(readTriggerCh) // close the channel upon return

	for i := 0; i < p.dataBlocks; i++ {
		// Setup read triggers for p.dataBlocks number of reads so that it reads in parallel.
		readTriggerCh <- true
	}

	disksNotFound := int32(0)
	bitrotHeal := int32(0)       // Atomic bool flag.
	missingPartsHeal := int32(0) // Atomic bool flag.
	readerIndex := 0
	var wg sync.WaitGroup
	// if readTrigger is true, it implies next disk.ReadAt() should be tried
	// if readTrigger is false, it implies previous disk.ReadAt() was successful and there is no need
	// to try reading the next disk.
	for readTrigger := range readTriggerCh {
		newBufLK.RLock()
		canDecode := p.canDecode(newBuf)
		newBufLK.RUnlock()
		if canDecode {
			break
		}
		if readerIndex == len(p.readers) {
			break
		}
		if !readTrigger {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rr := p.readers[i]
			if rr == nil {
				// Since reader is nil, trigger another read.
				readTriggerCh <- true
				return
			}
			bufIdx := p.readerToBuf[i]
			if p.buf[bufIdx] == nil {
				// Reading first time on this disk, hence the buffer needs to be allocated.
				// Subsequent reads will reuse this buffer.
				p.buf[bufIdx] = make([]byte, p.shardSize)
			}
			// For the last shard, the shardsize might be less than previous shard sizes.
			// Hence the following statement ensures that the buffer size is reset to the right size.
			p.buf[bufIdx] = p.buf[bufIdx][:p.shardSize]
			n, err := rr.ReadAt(p.buf[bufIdx], p.offset)
			if err != nil {
				switch {
				case errors.Is(err, errFileNotFound):
					atomic.StoreInt32(&missingPartsHeal, 1)
				case errors.Is(err, errFileCorrupt):
					atomic.StoreInt32(&bitrotHeal, 1)
				case errors.Is(err, errDiskNotFound):
					atomic.AddInt32(&disksNotFound, 1)
				}

				// This will be communicated upstream.
				p.orgReaders[bufIdx] = nil
				if br, ok := p.readers[i].(io.Closer); ok {
					br.Close()
				}
				p.readers[i] = nil

				// Since ReadAt returned error, trigger another read.
				readTriggerCh <- true
				return
			}
			newBufLK.Lock()
			newBuf[bufIdx] = p.buf[bufIdx][:n]
			newBufLK.Unlock()
			// Since ReadAt returned success, there is no need to trigger another read.
			readTriggerCh <- false
		}(readerIndex)
		readerIndex++
	}
	wg.Wait()
	if p.canDecode(newBuf) {
		p.offset += p.shardSize
		if missingPartsHeal == 1 {
			return newBuf, errFileNotFound
		} else if bitrotHeal == 1 {
			return newBuf, errFileCorrupt
		}
		return newBuf, nil
	}

	// If we cannot decode, just return read quorum error.
	return nil, fmt.Errorf("%w (offline-disks=%d/%d)", errErasureReadQuorum, disksNotFound, len(p.readers))
}

func (p *parallelReader) markReaderFailed(origIdx int) {
	if origIdx >= 0 && origIdx < len(p.orgReaders) {
		p.orgReaders[origIdx] = nil
	}
	for i, bIdx := range p.readerToBuf {
		if bIdx == origIdx {
			p.readers[i] = nil
		}
	}
}

func canDirectReadSpan(readers []io.ReaderAt, startShard, endShard int) bool {
	if startShard < 0 || endShard < startShard || endShard >= len(readers) {
		return false
	}
	for j := startShard; j <= endShard; j++ {
		if readers[j] == nil {
			return false
		}
		if _, isWhole := readers[j].(*wholeBitrotReader); isWhole {
			return false
		}
	}
	return true
}

func recordHealError(derr *error, err error) {
	if *derr == nil && (errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt)) {
		*derr = err
	}
}

func failDirectReader(readers []io.ReaderAt, reader *parallelReader, shardIdx int, closeNow bool) {
	rr := readers[shardIdx]
	readers[shardIdx] = nil
	if reader != nil {
		reader.markReaderFailed(shardIdx)
	}
	if closeNow && rr != nil {
		if closer, ok := rr.(io.Closer); ok {
			_ = closer.Close()
		}
	}
}

// tryReadSpannedShardsDirect reads only the data shards [startShard..endShard] overlapped by a sub-stripe
// range request. Each spanned shard still verifies its per-shard HighwayHash256S bitrot frame via ReadAt.
// On any missing/corrupt shard or slow-drive hedge timeout, it returns ok=false without writing any bytes
// so the caller transparently falls back to full parallelReader Reed-Solomon reconstruction.
func (e Erasure) tryReadSpannedShardsDirect(
	ctx context.Context,
	readers []io.ReaderAt,
	reader *parallelReader,
	startShard, endShard int,
	shardOffset, curShardSize int64,
	derr *error,
) ([]byte, bool, error) {
	spannedCount := endShard - startShard + 1
	directBuf := make([]byte, int64(spannedCount)*curShardSize)
	states := make([]atomic.Int32, spannedCount) // 0=running, 1=abandoned, 2=done
	errs := make([]error, spannedCount)
	doneCh := make(chan int, spannedCount)

	for idx := 0; idx < spannedCount; idx++ {
		rr := readers[startShard+idx]
		dst := directBuf[int64(idx)*curShardSize : int64(idx+1)*curShardSize]
		go func(idx int, rr io.ReaderAt, dst []byte) {
			n, err := rr.ReadAt(dst, shardOffset)
			if err == nil && int64(n) != curShardSize {
				err = errLessData
			}
			errs[idx] = err
			if states[idx].CompareAndSwap(0, 2) {
				doneCh <- idx
				return
			}
			// Caller timed out or cancelled and transferred exclusive ownership of rr to this goroutine.
			if closer, ok := rr.(io.Closer); ok {
				_ = closer.Close()
			}
		}(idx, rr, dst)
	}

	timer := time.NewTimer(ecDirectReadHedgeDelay)
	defer timer.Stop()

	timedOut := false
	completed := 0
	for completed < spannedCount && !timedOut {
		select {
		case <-doneCh:
			completed++
		case <-timer.C:
			timedOut = true
		case <-ctx.Done():
			timedOut = true
		}
	}

	if timedOut {
		for idx := 0; idx < spannedCount; idx++ {
			shardIdx := startShard + idx
			if states[idx].CompareAndSwap(0, 1) {
				rr := readers[shardIdx]
				failDirectReader(readers, reader, shardIdx, false)
				if canceler, ok := rr.(interface{ Cancel() }); ok {
					canceler.Cancel()
				}
			} else if states[idx].Load() == 2 && errs[idx] != nil {
				recordHealError(derr, errs[idx])
				failDirectReader(readers, reader, shardIdx, true)
			}
		}
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return nil, false, nil
	}

	allOK := true
	for idx := 0; idx < spannedCount; idx++ {
		if errs[idx] != nil {
			allOK = false
			recordHealError(derr, errs[idx])
			failDirectReader(readers, reader, startShard+idx, true)
		}
	}
	if !allOK {
		return nil, false, nil
	}
	return directBuf, true, nil
}

// Decode reads from readers, reconstructs data if needed and writes the data to the writer.
// A set of preferred drives can be supplied. In that case they will be used and the data reconstructed.
func (e Erasure) Decode(ctx context.Context, writer io.Writer, readers []io.ReaderAt, offset, length, totalLength int64, prefer []bool) (written int64, derr error) {
	if offset < 0 || length < 0 {
		return -1, errInvalidArgument
	}
	if offset+length > totalLength {
		return -1, errInvalidArgument
	}

	if length == 0 {
		return 0, nil
	}

	var reader *parallelReader
	getParallelReader := func(block int64) *parallelReader {
		if reader == nil {
			reader = newParallelReader(readers, e, block*e.blockSize, totalLength)
			if len(prefer) == len(readers) {
				reader.preferReaders(prefer)
			}
		} else {
			reader.offset = block * e.ShardSize()
			reader.shardSize = e.ShardSize()
		}
		return reader
	}
	defer func() {
		if reader != nil {
			reader.Done()
		}
	}()

	startBlock := offset / e.blockSize
	endBlock := (offset + length) / e.blockSize
	directReadAllowed := ecDirectShardReadEnabled()

	var bytesWritten int64
	var bufs [][]byte
	for block := startBlock; block <= endBlock; block++ {
		var blockOffset, blockLength int64
		switch {
		case startBlock == endBlock:
			blockOffset = offset % e.blockSize
			blockLength = length
		case block == startBlock:
			blockOffset = offset % e.blockSize
			blockLength = e.blockSize - blockOffset
		case block == endBlock:
			blockOffset = 0
			blockLength = (offset + length) % e.blockSize
		default:
			blockOffset = 0
			blockLength = e.blockSize
		}
		if blockLength == 0 {
			break
		}

		if directReadAllowed && e.dataBlocks > 1 {
			stripeDataLen := min(e.blockSize, totalLength-block*e.blockSize)
			curShardSize := ceilFrac(stripeDataLen, int64(e.dataBlocks))
			if curShardSize > 0 {
				startShard := int(blockOffset / curShardSize)
				endShard := int((blockOffset + blockLength - 1) / curShardSize)
				spannedCount := endShard - startShard + 1
				if spannedCount < e.dataBlocks && canDirectReadSpan(readers, startShard, endShard) {
					shardOffset := block * e.ShardSize()
					directBuf, ok, ctxErr := e.tryReadSpannedShardsDirect(
						ctx, readers, reader, startShard, endShard, shardOffset, curShardSize, &derr,
					)
					if ctxErr != nil {
						return -1, ctxErr
					}
					if ok {
						if ctx.Err() != nil {
							return -1, ctx.Err()
						}
						sliceStart := blockOffset - int64(startShard)*curShardSize
						nw, werr := writer.Write(directBuf[sliceStart : sliceStart+blockLength])
						if werr != nil {
							return -1, werr
						}
						if int64(nw) != blockLength {
							return -1, io.ErrShortWrite
						}
						bytesWritten += int64(nw)
						continue
					}
				}
			}
		}

		var err error
		pr := getParallelReader(block)
		bufs, err = pr.Read(bufs)
		if len(bufs) > 0 {
			// Set only if there are be enough data for reconstruction.
			// and only for expected errors, also set once.
			if errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt) {
				if derr == nil {
					derr = err
				}
			}
		} else if err != nil {
			// For all errors that cannot be reconstructed fail the read operation.
			return -1, err
		}

		if err = e.DecodeDataBlocks(bufs); err != nil {
			return -1, err
		}

		n, err := writeDataBlocks(ctx, writer, bufs, e.dataBlocks, blockOffset, blockLength)
		if err != nil {
			return -1, err
		}

		bytesWritten += n
	}

	if bytesWritten != length {
		return bytesWritten, errLessData
	}

	return bytesWritten, derr
}

// Heal reads from readers, reconstruct shards and writes the data to the writers.
func (e Erasure) Heal(ctx context.Context, writers []io.Writer, readers []io.ReaderAt, totalLength int64, prefer []bool) (derr error) {
	if len(writers) != e.parityBlocks+e.dataBlocks {
		return errInvalidArgument
	}

	reader := newParallelReader(readers, e, 0, totalLength)
	if len(readers) == len(prefer) {
		reader.preferReaders(prefer)
	}
	defer reader.Done()

	startBlock := int64(0)
	endBlock := totalLength / e.blockSize
	if totalLength%e.blockSize != 0 {
		endBlock++
	}

	var bufs [][]byte
	for block := startBlock; block < endBlock; block++ {
		var err error
		bufs, err = reader.Read(bufs)
		if len(bufs) > 0 {
			if errors.Is(err, errFileNotFound) || errors.Is(err, errFileCorrupt) {
				if derr == nil {
					derr = err
				}
			}
		} else if err != nil {
			return err
		}

		if err = e.DecodeDataAndParityBlocks(ctx, bufs); err != nil {
			return err
		}

		w := multiWriter{
			writers:     writers,
			writeQuorum: 1,
			errs:        make([]error, len(writers)),
		}

		if err = w.Write(ctx, bufs); err != nil {
			return err
		}
	}

	return derr
}
