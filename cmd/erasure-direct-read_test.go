// Copyright (c) 2026 MinIO Contributors
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio/internal/bpool"
)

func init() {
	if pool := globalBytePoolCap.Load(); pool == nil || pool.Width() == 0 {
		globalBytePoolCap.Store(bpool.NewBytePoolCap(64, blockSizeV2, blockSizeV2*2))
	}
}

type countingBitrotReader struct {
	inner  io.ReaderAt
	reads  atomic.Int32
	closed atomic.Bool
}

func (c *countingBitrotReader) ReadAt(p []byte, off int64) (int, error) {
	c.reads.Add(1)
	return c.inner.ReadAt(p, off)
}

func (c *countingBitrotReader) Close() error {
	c.closed.Store(true)
	if closer, ok := c.inner.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (c *countingBitrotReader) Cancel() {
	if canceler, ok := c.inner.(interface{ Cancel() }); ok {
		canceler.Cancel()
	}
}

type slowBitrotReader struct {
	inner      io.ReaderAt
	delay      time.Duration
	cancelCh   chan struct{}
	cancelOnce sync.Once
	canceled   atomic.Bool
	closed     atomic.Bool
}

func newSlowBitrotReader(inner io.ReaderAt, delay time.Duration) *slowBitrotReader {
	return &slowBitrotReader{
		inner:    inner,
		delay:    delay,
		cancelCh: make(chan struct{}),
	}
}

func (s *slowBitrotReader) ReadAt(p []byte, off int64) (int, error) {
	select {
	case <-time.After(s.delay):
		return s.inner.ReadAt(p, off)
	case <-s.cancelCh:
		return 0, context.Canceled
	}
}

func (s *slowBitrotReader) Cancel() {
	s.canceled.Store(true)
	s.cancelOnce.Do(func() {
		close(s.cancelCh)
	})
	if canceler, ok := s.inner.(interface{ Cancel() }); ok {
		canceler.Cancel()
	}
}

func (s *slowBitrotReader) Close() error {
	s.closed.Store(true)
	if closer, ok := s.inner.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func deterministicPayload(size int, seed uint64) []byte {
	out := make([]byte, size)
	var blk [8]byte
	binary.LittleEndian.PutUint64(blk[:], seed)
	for off := 0; off < size; off += sha256.Size {
		var in [16]byte
		copy(in[:8], blk[:])
		binary.LittleEndian.PutUint64(in[8:], uint64(off))
		sum := sha256.Sum256(in[:])
		copy(out[off:], sum[:])
	}
	return out
}

type encodedTestStripeSet struct {
	disks    []StorageAPI
	diskDirs []string
	erasure  Erasure
	volume   string
	filePath string
	payload  []byte
}

func setupEncodedStripeSet(t *testing.T, dataBlocks, parityBlocks int, blockSize int64, payloadSize int) *encodedTestStripeSet {
	t.Helper()
	if pool := globalBytePoolCap.Load(); pool == nil || pool.Width() == 0 {
		globalBytePoolCap.Store(bpool.NewBytePoolCap(64, blockSizeV2, blockSizeV2*2))
	}
	ctx := context.Background()
	totalDisks := dataBlocks + parityBlocks
	disks := make([]StorageAPI, totalDisks)
	diskDirs := make([]string, totalDisks)
	volume := "testvol"
	filePath := "obj/part.1"

	for i := range totalDisks {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("d%02d", i))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		d, err := newLocalXLStorage(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.MakeVol(ctx, volume); err != nil {
			t.Fatal(err)
		}
		disks[i] = d
		diskDirs[i] = dir
	}

	erasure, err := NewErasure(ctx, dataBlocks, parityBlocks, blockSize)
	if err != nil {
		t.Fatal(err)
	}

	payload := deterministicPayload(payloadSize, uint64(blockSize)^uint64(payloadSize))
	shardFileSize := erasure.ShardFileSize(int64(len(payload)))
	writers := make([]io.Writer, totalDisks)
	for i, d := range disks {
		writers[i] = newBitrotWriter(d, volume, volume, filePath, shardFileSize, HighwayHash256S, erasure.ShardSize())
	}

	buf, release := acquireECBlockBuffer(blockSize)
	defer release()

	n, err := erasure.Encode(ctx, bytes.NewReader(payload), writers, buf, dataBlocks+1)
	closeErrs := closeBitrotWriters(writers)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	for i, cerr := range closeErrs {
		if cerr != nil {
			t.Fatalf("close writer %d failed: %v", i, cerr)
		}
	}
	if n != int64(len(payload)) {
		t.Fatalf("encoded %d bytes, want %d", n, len(payload))
	}

	return &encodedTestStripeSet{
		disks:    disks,
		diskDirs: diskDirs,
		erasure:  erasure,
		volume:   volume,
		filePath: filePath,
		payload:  payload,
	}
}

func (s *encodedTestStripeSet) makeCountingReaders(offset, length int64) ([]io.ReaderAt, []*countingBitrotReader) {
	tillOffset := s.erasure.ShardFileOffset(offset, length, int64(len(s.payload)))
	readers := make([]io.ReaderAt, len(s.disks))
	counters := make([]*countingBitrotReader, len(s.disks))
	for i, d := range s.disks {
		if d == nil {
			continue
		}
		br := newBitrotReader(d, nil, s.volume, s.filePath, tillOffset, HighwayHash256S, nil, s.erasure.ShardSize())
		cr := &countingBitrotReader{inner: br}
		readers[i] = cr
		counters[i] = cr
	}
	return readers, counters
}

func countTouchedShards(counters []*countingBitrotReader) (touched int, totalReads int) {
	for _, c := range counters {
		if c == nil {
			continue
		}
		r := int(c.reads.Load())
		if r > 0 {
			touched++
			totalReads += r
		}
	}
	return touched, totalReads
}

func TestSpannedShardDirectReadTouchCountsAndOddTail(t *testing.T) {
	defer RefreshECDirectShardReadFromEnv()

	layouts := []struct {
		name         string
		dataBlocks   int
		parityBlocks int
	}{
		{"12+4", 12, 4},
		{"8+8", 8, 8},
	}

	for _, layout := range layouts {
		t.Run(layout.name, func(t *testing.T) {
			// 199,999 bytes with B=64 KiB creates 3 full stripes (196,608 B) + 1 odd partial tail stripe (3,391 B).
			set := setupEncodedStripeSet(t, layout.dataBlocks, layout.parityBlocks, 64*1024, 199_999)
			ctx := context.Background()
			shardSize := set.erasure.ShardSize()

			// Case 1: Single-shard 4 KiB read at offset 0 with direct read ENABLED -> touches exactly 1 shard!
			SetECDirectShardReadEnabled(true)
			readers, counters := set.makeCountingReaders(0, 4096)
			var out bytes.Buffer
			written, derr := set.erasure.Decode(ctx, &out, readers, 0, 4096, int64(len(set.payload)), nil)
			closeBitrotReaders(readers)
			if derr != nil || written != 4096 || !bytes.Equal(out.Bytes(), set.payload[:4096]) {
				t.Fatalf("direct 4KiB read failed: written=%d err=%v match=%v", written, derr, bytes.Equal(out.Bytes(), set.payload[:4096]))
			}
			touched, _ := countTouchedShards(counters)
			if touched != 1 {
				t.Fatalf("expected exactly 1 shard touched on 4KiB read, got %d", touched)
			}

			// Case 2: Same 4 KiB read with direct read DISABLED (Plan A) -> touches all dataBlocks shards!
			SetECDirectShardReadEnabled(false)
			readers, counters = set.makeCountingReaders(0, 4096)
			out.Reset()
			written, derr = set.erasure.Decode(ctx, &out, readers, 0, 4096, int64(len(set.payload)), nil)
			closeBitrotReaders(readers)
			if derr != nil || written != 4096 || !bytes.Equal(out.Bytes(), set.payload[:4096]) {
				t.Fatalf("disabled-direct 4KiB read failed: written=%d err=%v", written, derr)
			}
			touched, _ = countTouchedShards(counters)
			if touched != layout.dataBlocks {
				t.Fatalf("expected %d shards touched when direct read disabled, got %d", layout.dataBlocks, touched)
			}

			// Case 3: Boundary-crossing 4 KiB read spanning shards 2 and 3 -> touches exactly 2 shards!
			SetECDirectShardReadEnabled(true)
			crossOff := 2*shardSize + shardSize - 512
			readers, counters = set.makeCountingReaders(crossOff, 4096)
			out.Reset()
			written, derr = set.erasure.Decode(ctx, &out, readers, crossOff, 4096, int64(len(set.payload)), nil)
			closeBitrotReaders(readers)
			if derr != nil || written != 4096 || !bytes.Equal(out.Bytes(), set.payload[crossOff:crossOff+4096]) {
				t.Fatalf("cross-shard 4KiB read failed: written=%d err=%v", written, derr)
			}
			touched, _ = countTouchedShards(counters)
			if touched != 2 {
				t.Fatalf("expected exactly 2 shards touched across shard boundary, got %d", touched)
			}

			// Case 4: Cross-stripe read spanning end of stripe 0 and start of stripe 1.
			stripeCrossOff := int64(64*1024) - 1000
			stripeCrossLen := int64(2500)
			readers, counters = set.makeCountingReaders(stripeCrossOff, stripeCrossLen)
			out.Reset()
			written, derr = set.erasure.Decode(ctx, &out, readers, stripeCrossOff, stripeCrossLen, int64(len(set.payload)), nil)
			closeBitrotReaders(readers)
			if derr != nil || written != stripeCrossLen || !bytes.Equal(out.Bytes(), set.payload[stripeCrossOff:stripeCrossOff+stripeCrossLen]) {
				t.Fatalf("cross-stripe read failed: written=%d err=%v", written, derr)
			}
			touched, _ = countTouchedShards(counters)
			if touched > 2 {
				t.Fatalf("expected <=2 shards touched across stripe boundary, got %d", touched)
			}

			// Case 5: Odd-tail final stripe sub-range read (reading inside the 3,391-byte final partial stripe).
			tailOff := int64(3*64*1024 + 37)
			tailLen := int64(777)
			readers, counters = set.makeCountingReaders(tailOff, tailLen)
			out.Reset()
			written, derr = set.erasure.Decode(ctx, &out, readers, tailOff, tailLen, int64(len(set.payload)), nil)
			closeBitrotReaders(readers)
			if derr != nil || written != tailLen || !bytes.Equal(out.Bytes(), set.payload[tailOff:tailOff+tailLen]) {
				t.Fatalf("odd-tail partial stripe read failed: written=%d err=%v", written, derr)
			}
			touched, _ = countTouchedShards(counters)
			if touched >= layout.dataBlocks {
				t.Fatalf("expected sub-stripe touch count <%d on odd tail, got %d", layout.dataBlocks, touched)
			}

			// Case 6: Full-object read still works identically.
			readers, _ = set.makeCountingReaders(0, int64(len(set.payload)))
			out.Reset()
			written, derr = set.erasure.Decode(ctx, &out, readers, 0, int64(len(set.payload)), int64(len(set.payload)), nil)
			closeBitrotReaders(readers)
			if derr != nil || written != int64(len(set.payload)) || !bytes.Equal(out.Bytes(), set.payload) {
				t.Fatalf("full decode failed: written=%d err=%v", written, derr)
			}
		})
	}
}

func TestSpannedShardDirectReadDegradedAndCorruptFallback(t *testing.T) {
	defer RefreshECDirectShardReadFromEnv()
	SetECDirectShardReadEnabled(true)

	set := setupEncodedStripeSet(t, 12, 4, 64*1024, 199_999)
	ctx := context.Background()

	// 1. Delete shard 0 and shard 1 on disk -> missing shard fallback must reconstruct cleanly and surface errFileNotFound for MRF heal.
	for _, idx := range []int{0, 1} {
		shardPath := filepath.Join(set.diskDirs[idx], set.volume, filepath.FromSlash(set.filePath))
		if err := os.Remove(shardPath); err != nil {
			t.Fatalf("remove shard %d: %v", idx, err)
		}
	}

	readers, _ := set.makeCountingReaders(0, 4096)
	var out bytes.Buffer
	written, derr := set.erasure.Decode(ctx, &out, readers, 0, 4096, int64(len(set.payload)), nil)
	closeBitrotReaders(readers)
	if written != 4096 || !bytes.Equal(out.Bytes(), set.payload[:4096]) {
		t.Fatalf("missing-shard fallback payload mismatch: written=%d err=%v", written, derr)
	}
	if !errors.Is(derr, errFileNotFound) {
		t.Fatalf("expected derr=errFileNotFound for MRF heal trigger, got %v", derr)
	}

	// 2. Corrupt shard 3 on disk (total 3 degraded shards <= 4 parity shards) -> bitrot check must catch corruption,
	// emit zero corrupt bytes, reconstruct from parity, and surface errFileCorrupt!
	corruptPath := filepath.Join(set.diskDirs[3], set.volume, filepath.FromSlash(set.filePath))
	rawShard, err := os.ReadFile(corruptPath)
	if err != nil {
		t.Fatal(err)
	}
	rawShard[40] ^= 0xff // Flip payload byte inside first HighwayHash256S frame
	if err := os.WriteFile(corruptPath, rawShard, 0o644); err != nil {
		t.Fatal(err)
	}

	shard3Off := 3 * set.erasure.ShardSize()
	readers, _ = set.makeCountingReaders(shard3Off, 4096)
	out.Reset()
	written, derr = set.erasure.Decode(ctx, &out, readers, shard3Off, 4096, int64(len(set.payload)), nil)
	closeBitrotReaders(readers)
	if written != 4096 || !bytes.Equal(out.Bytes(), set.payload[shard3Off:shard3Off+4096]) {
		t.Fatalf("corrupt-shard fallback payload mismatch: written=%d err=%v", written, derr)
	}
	if !errors.Is(derr, errFileCorrupt) {
		t.Fatalf("expected derr=errFileCorrupt for bitrot MRF heal trigger, got %v", derr)
	}
}

func TestSpannedShardDirectReadHedgeTimeoutFallback(t *testing.T) {
	defer RefreshECDirectShardReadFromEnv()
	SetECDirectShardReadEnabled(true)

	origDelay := ecDirectReadHedgeDelay
	ecDirectReadHedgeDelay = 20 * time.Millisecond
	defer func() { ecDirectReadHedgeDelay = origDelay }()

	set := setupEncodedStripeSet(t, 12, 4, 64*1024, 128*1024)
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			off := int64((iter % 4) * 512)
			tillOffset := set.erasure.ShardFileOffset(off, 4096, int64(len(set.payload)))
			readers := make([]io.ReaderAt, len(set.disks))
			var slow *slowBitrotReader
			for i, d := range set.disks {
				br := newBitrotReader(d, nil, set.volume, set.filePath, tillOffset, HighwayHash256S, nil, set.erasure.ShardSize())
				if i == 0 {
					slow = newSlowBitrotReader(br, 250*time.Millisecond)
					readers[i] = slow
				} else {
					readers[i] = br
				}
			}

			start := time.Now()
			var out bytes.Buffer
			written, derr := set.erasure.Decode(ctx, &out, readers, off, 4096, int64(len(set.payload)), nil)
			elapsed := time.Since(start)
			closeBitrotReaders(readers)

			if derr != nil || written != 4096 || !bytes.Equal(out.Bytes(), set.payload[off:off+4096]) {
				t.Errorf("iter %d hedge fallback failed: written=%d err=%v", iter, written, derr)
				return
			}
			if elapsed >= 200*time.Millisecond {
				t.Errorf("iter %d hedge timeout did not cut tail latency: elapsed=%v", iter, elapsed)
			}
			if !slow.canceled.Load() {
				t.Errorf("iter %d expected slow reader Cancel() to be invoked", iter)
			}
		}(g)
	}
	wg.Wait()
}

func TestLarge4MiBECBlockEncodeAndDirectRead(t *testing.T) {
	defer RefreshECDirectShardReadFromEnv()
	SetECDirectShardReadEnabled(true)

	const payloadLen = 9*1024*1024 + 321
	set := setupEncodedStripeSet(t, 12, 4, 4*1024*1024, payloadLen)
	ctx := context.Background()

	// Sub-stripe 4 KiB read inside a 4 MiB block (~341 KiB per shard) touches only 1 shard!
	readers, counters := set.makeCountingReaders(4096, 4096)
	var out bytes.Buffer
	written, derr := set.erasure.Decode(ctx, &out, readers, 4096, 4096, payloadLen, nil)
	closeBitrotReaders(readers)
	if derr != nil || written != 4096 || !bytes.Equal(out.Bytes(), set.payload[4096:8192]) {
		t.Fatalf("4MiB block direct read failed: written=%d err=%v", written, derr)
	}
	if touched, _ := countTouchedShards(counters); touched != 1 {
		t.Fatalf("expected 1 shard touched on 4MiB stripe 4KiB read, got %d", touched)
	}

	// Full read across 2x 4MiB stripes + partial tail stripe.
	readers, _ = set.makeCountingReaders(0, payloadLen)
	out.Reset()
	written, derr = set.erasure.Decode(ctx, &out, readers, 0, payloadLen, payloadLen, nil)
	closeBitrotReaders(readers)
	if derr != nil || written != payloadLen || !bytes.Equal(out.Bytes(), set.payload) {
		t.Fatalf("4MiB block full decode failed: written=%d err=%v", written, derr)
	}
}
