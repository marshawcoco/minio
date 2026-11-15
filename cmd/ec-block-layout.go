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
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// DefaultECBlockSize is the standard 1 MiB erasure-coding stripe size.
	DefaultECBlockSize int64 = blockSizeV2

	// MinObjectSizeFor4MiBECBlock is the minimum object size recommended for 4 MiB stripes.
	MinObjectSizeFor4MiBECBlock int64 = 16 * 1024 * 1024

	// S3 wire-protocol metadata and response header names (compatible with RustFS / BrewFS / warp).
	ECBlockSizeHintMetaKey     = "rustfs-ec-block-size-hint"
	ECBlockSizeHintHeader      = "x-amz-meta-rustfs-ec-block-size-hint"
	EffectiveECBlockSizeHeader = "x-rustfs-effective-ec-block-size"
	ECLayoutHintStatusHeader   = "x-rustfs-layout-hint-status"
	ECLayoutHintReasonHeader   = "x-rustfs-layout-hint-reason"

	// Rollout & feature gate environment variables (MINIO_* primary, RUSTFS_* fallback).
	EnvMinIOECBlockSizeHintEnable          = "MINIO_EC_BLOCK_SIZE_HINT_ENABLE"
	EnvRustFSECBlockSizeHintEnable         = "RUSTFS_EC_BLOCK_SIZE_HINT_ENABLE"
	EnvMinIOECBlockSizeHintFleetConfirmed  = "MINIO_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED"
	EnvRustFSECBlockSizeHintFleetConfirmed = "RUSTFS_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED"
	EnvMinIOECDirectShardReadEnable        = "MINIO_EC_DIRECT_SHARD_READ_ENABLE"
	EnvRustFSECDirectShardReadEnable       = "RUSTFS_EC_DIRECT_SHARD_READ_ENABLE"
)

// ECBlockSizeCandidates lists the supported per-object erasure-coding stripe sizes:
// 64 KiB, 256 KiB, 1 MiB, and 4 MiB.
var ECBlockSizeCandidates = [4]int64{
	64 * 1024,
	256 * 1024,
	1024 * 1024,
	4 * 1024 * 1024,
}

var (
	globalECDirectShardRead atomic.Bool

	largeECBlockBufPool = sync.Pool{
		New: func() any {
			const maxCandidate = 4 * 1024 * 1024
			b := make([]byte, maxCandidate, 2*maxCandidate)
			return &b
		},
	}
)

func init() {
	RefreshECDirectShardReadFromEnv()
}

// RefreshECDirectShardReadFromEnv reloads the spanned-shard direct read gate from environment variables.
func RefreshECDirectShardReadFromEnv() {
	globalECDirectShardRead.Store(
		parseBoolEnvWithFallback(EnvMinIOECDirectShardReadEnable, EnvRustFSECDirectShardReadEnable, true),
	)
}

// SetECDirectShardReadEnabled overrides the spanned-shard direct read gate at runtime (useful in tests).
func SetECDirectShardReadEnabled(enabled bool) {
	globalECDirectShardRead.Store(enabled)
}

func ecDirectShardReadEnabled() bool {
	return globalECDirectShardRead.Load()
}

func parseBoolEnvWithFallback(primary, fallback string, defaultVal bool) bool {
	for _, key := range [2]string{primary, fallback} {
		raw, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		switch strings.ToLower(trimmed) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		default:
			return defaultVal
		}
	}
	return defaultVal
}

func isECBlockSizeHintEnabled() bool {
	return parseBoolEnvWithFallback(EnvMinIOECBlockSizeHintEnable, EnvRustFSECBlockSizeHintEnable, false)
}

func isECBlockSizeHintFleetConfirmed() bool {
	return parseBoolEnvWithFallback(EnvMinIOECBlockSizeHintFleetConfirmed, EnvRustFSECBlockSizeHintFleetConfirmed, false)
}

// IsCandidateECBlockSize reports whether blockSize is in ECBlockSizeCandidates.
func IsCandidateECBlockSize(blockSize int64) bool {
	for _, c := range ECBlockSizeCandidates {
		if blockSize == c {
			return true
		}
	}
	return false
}

// RecommendECBlockSizeForObjectSize selects the recommended candidate EC block size for an object size.
func RecommendECBlockSizeForObjectSize(objectSize int64) int64 {
	if objectSize >= MinObjectSizeFor4MiBECBlock {
		return 4 * 1024 * 1024
	}
	target := max(objectSize, ECBlockSizeCandidates[0])
	for _, c := range ECBlockSizeCandidates[:3] {
		if c >= target {
			return c
		}
	}
	return DefaultECBlockSize
}

func parseECBlockSizeHint(raw string) (uint64, bool) {
	if len(raw) == 0 || len(raw) > 20 {
		return 0, false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || parsed == 0 {
		return 0, false
	}
	return parsed, true
}

func isECBlockSizeHintKey(key string) bool {
	return strings.EqualFold(key, ECBlockSizeHintHeader) || strings.EqualFold(key, ECBlockSizeHintMetaKey)
}

func extractECBlockSizeHintValue(metadata map[string]string) (string, bool) {
	if len(metadata) == 0 {
		return "", false
	}
	var matchedKeys []string
	for k := range metadata {
		if isECBlockSizeHintKey(k) {
			matchedKeys = append(matchedKeys, k)
		}
	}
	if len(matchedKeys) == 0 {
		return "", false
	}
	if len(matchedKeys) > 1 {
		sort.Strings(matchedKeys)
	}
	return metadata[matchedKeys[0]], true
}

func stripECBlockSizeHintMetadata(metadata map[string]string) {
	for k := range metadata {
		if isECBlockSizeHintKey(k) {
			delete(metadata, k)
		}
	}
}

type ecBlockLayoutResolution struct {
	EffectiveBlockSize int64
	Status             string
	Reason             string
}

func resolveECBlockLayoutFromMetadata(bucket, object string, metadata map[string]string, eligibleWrite bool) (ecBlockLayoutResolution, error) {
	rawHint, hasHint := extractECBlockSizeHintValue(metadata)
	if !hasHint {
		return ecBlockLayoutResolution{EffectiveBlockSize: DefaultECBlockSize}, nil
	}
	if !isECBlockSizeHintEnabled() {
		return ecBlockLayoutResolution{
			EffectiveBlockSize: DefaultECBlockSize,
			Status:             "ignored",
			Reason:             "disabled",
		}, nil
	}
	if !isECBlockSizeHintFleetConfirmed() {
		return ecBlockLayoutResolution{
			EffectiveBlockSize: DefaultECBlockSize,
			Status:             "ignored",
			Reason:             "fleet-unconfirmed",
		}, nil
	}

	parsed, ok := parseECBlockSizeHint(rawHint)
	if !ok {
		return ecBlockLayoutResolution{}, InvalidArgument{
			Bucket: bucket,
			Object: object,
			Err:    fmt.Errorf("invalid %s metadata value %q", ECBlockSizeHintHeader, rawHint),
		}
	}

	// Feature is enabled and fleet-confirmed, and syntax is valid: strip internal hint from stored user metadata.
	stripECBlockSizeHintMetadata(metadata)

	if parsed > math.MaxInt64 || !IsCandidateECBlockSize(int64(parsed)) {
		return ecBlockLayoutResolution{
			EffectiveBlockSize: DefaultECBlockSize,
			Status:             "ignored",
			Reason:             "unsupported-size",
		}, nil
	}
	if !eligibleWrite {
		return ecBlockLayoutResolution{
			EffectiveBlockSize: DefaultECBlockSize,
			Status:             "ignored",
			Reason:             "unsupported-write-mode",
		}, nil
	}

	return ecBlockLayoutResolution{
		EffectiveBlockSize: int64(parsed),
		Status:             "applied",
	}, nil
}

func effectiveECBlockSizeOrDefault(blockSize int64) int64 {
	if blockSize > 0 {
		return blockSize
	}
	return DefaultECBlockSize
}

func setEffectiveECBlockSizeHeader(h http.Header, blockSize int64) {
	if h == nil {
		return
	}
	h.Set(EffectiveECBlockSizeHeader, strconv.FormatInt(effectiveECBlockSizeOrDefault(blockSize), 10))
}

func setECLayoutHintHeaders(h http.Header, res ecBlockLayoutResolution) {
	if h == nil {
		return
	}
	setEffectiveECBlockSizeHeader(h, res.EffectiveBlockSize)
	if res.Status != "" {
		h.Set(ECLayoutHintStatusHeader, res.Status)
	}
	if res.Reason != "" {
		h.Set(ECLayoutHintReasonHeader, res.Reason)
	}
}

// acquireECBlockBuffer returns a buffer of length blockSize with sufficient capacity for
// zero-allocation Reed-Solomon split/encode, along with a release callback.
func acquireECBlockBuffer(blockSize int64) ([]byte, func()) {
	pool := globalBytePoolCap.Load()
	if pool != nil && int64(pool.Width()) >= blockSize && int64(pool.WidthCap()) >= 2*blockSize {
		buf := pool.Get()
		return buf[:blockSize], func() {
			pool.Put(buf)
		}
	}
	if blockSize <= 4*1024*1024 {
		bp := largeECBlockBufPool.Get().(*[]byte)
		buf := (*bp)[:blockSize]
		return buf, func() {
			largeECBlockBufPool.Put(bp)
		}
	}
	buf := make([]byte, blockSize, 2*blockSize)
	return buf, func() {}
}
