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
	"errors"
	"net/http"
	"testing"
)

func TestParseECBlockSizeHint(t *testing.T) {
	valid := map[string]uint64{
		"65536":   65536,
		"262144":  262144,
		"1048576": 1048576,
		"4194304": 4194304,
		"131072":  131072,
		"1":       1,
	}
	for raw, want := range valid {
		got, ok := parseECBlockSizeHint(raw)
		if !ok || got != want {
			t.Fatalf("parseECBlockSizeHint(%q) = (%d, %v), want (%d, true)", raw, got, ok, want)
		}
	}

	invalid := []string{
		"",
		"0",
		"0000",
		"+65536",
		"-65536",
		" 65536",
		"65536 ",
		"64k",
		"0x10000",
		"65_536",
		"18446744073709551616",  // uint64 overflow
		"123456789012345678901", // > 20 digits
	}
	for _, raw := range invalid {
		if got, ok := parseECBlockSizeHint(raw); ok {
			t.Fatalf("parseECBlockSizeHint(%q) unexpectedly succeeded with %d", raw, got)
		}
	}
}

func TestRecommendECBlockSizeForObjectSize(t *testing.T) {
	cases := []struct {
		size int64
		want int64
	}{
		{0, 64 * 1024},
		{4 * 1024, 64 * 1024},
		{64 * 1024, 64 * 1024},
		{64*1024 + 1, 256 * 1024},
		{256 * 1024, 256 * 1024},
		{256*1024 + 1, 1024 * 1024},
		{1024 * 1024, 1024 * 1024},
		{16*1024*1024 - 1, 1024 * 1024},
		{16 * 1024 * 1024, 4 * 1024 * 1024},
		{64 * 1024 * 1024, 4 * 1024 * 1024},
	}
	for _, tc := range cases {
		if got := RecommendECBlockSizeForObjectSize(tc.size); got != tc.want {
			t.Fatalf("RecommendECBlockSizeForObjectSize(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

func TestResolveECBlockLayoutRolloutMatrix(t *testing.T) {
	// 1. Absent hint -> default 1 MiB, no status/reason headers.
	t.Setenv(EnvMinIOECBlockSizeHintEnable, "true")
	t.Setenv(EnvMinIOECBlockSizeHintFleetConfirmed, "true")
	meta := map[string]string{"X-Amz-Meta-Custom": "keep"}
	res, err := resolveECBlockLayoutFromMetadata("bkt", "obj", meta, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EffectiveBlockSize != DefaultECBlockSize || res.Status != "" || res.Reason != "" {
		t.Fatalf("unexpected resolution for absent hint: %+v", res)
	}

	// 2. Disabled -> ignored/disabled, metadata preserved, malformed syntax not rejected.
	t.Setenv(EnvMinIOECBlockSizeHintEnable, "false")
	t.Setenv(EnvMinIOECBlockSizeHintFleetConfirmed, "true")
	meta = map[string]string{"X-Amz-Meta-Rustfs-Ec-Block-Size-Hint": "+bad"}
	res, err = resolveECBlockLayoutFromMetadata("bkt", "obj", meta, true)
	if err != nil {
		t.Fatalf("disabled gate must not reject malformed hint: %v", err)
	}
	if res.EffectiveBlockSize != DefaultECBlockSize || res.Status != "ignored" || res.Reason != "disabled" {
		t.Fatalf("unexpected disabled resolution: %+v", res)
	}
	if _, ok := meta["X-Amz-Meta-Rustfs-Ec-Block-Size-Hint"]; !ok {
		t.Fatal("disabled gate must not strip user metadata")
	}

	// 3. Enabled via RUSTFS_* fallback, fleet unconfirmed -> ignored/fleet-unconfirmed, metadata preserved.
	t.Setenv(EnvMinIOECBlockSizeHintEnable, "")
	t.Setenv(EnvRustFSECBlockSizeHintEnable, "1")
	t.Setenv(EnvMinIOECBlockSizeHintFleetConfirmed, "")
	t.Setenv(EnvRustFSECBlockSizeHintFleetConfirmed, "0")
	meta = map[string]string{"X-Amz-Meta-Rustfs-Ec-Block-Size-Hint": "65536"}
	res, err = resolveECBlockLayoutFromMetadata("bkt", "obj", meta, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EffectiveBlockSize != DefaultECBlockSize || res.Status != "ignored" || res.Reason != "fleet-unconfirmed" {
		t.Fatalf("unexpected fleet-unconfirmed resolution: %+v", res)
	}
	if _, ok := meta["X-Amz-Meta-Rustfs-Ec-Block-Size-Hint"]; !ok {
		t.Fatal("fleet-unconfirmed gate must not strip user metadata")
	}

	// 4. Enabled + fleet-confirmed: malformed hint -> InvalidArgument.
	t.Setenv(EnvRustFSECBlockSizeHintFleetConfirmed, "true")
	meta = map[string]string{"X-Amz-Meta-Rustfs-Ec-Block-Size-Hint": "+65536"}
	_, err = resolveECBlockLayoutFromMetadata("bkt", "obj", meta, true)
	var invArg InvalidArgument
	if !errors.As(err, &invArg) {
		t.Fatalf("expected InvalidArgument on malformed hint, got %T (%v)", err, err)
	}

	// 5. Enabled + fleet-confirmed: unsupported candidate -> ignored/unsupported-size, hint stripped.
	meta = map[string]string{
		"X-Amz-Meta-Rustfs-Ec-Block-Size-Hint": "131072",
		"X-Amz-Meta-Keep":                      "yes",
	}
	res, err = resolveECBlockLayoutFromMetadata("bkt", "obj", meta, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EffectiveBlockSize != DefaultECBlockSize || res.Status != "ignored" || res.Reason != "unsupported-size" {
		t.Fatalf("unexpected unsupported-size resolution: %+v", res)
	}
	if _, ok := meta["X-Amz-Meta-Rustfs-Ec-Block-Size-Hint"]; ok {
		t.Fatal("expected hint metadata to be stripped")
	}
	if meta["X-Amz-Meta-Keep"] != "yes" {
		t.Fatal("expected unrelated user metadata to be preserved")
	}

	// 6. Enabled + fleet-confirmed: ineligible write mode -> ignored/unsupported-write-mode, hint stripped.
	meta = map[string]string{"X-Amz-Meta-Rustfs-Ec-Block-Size-Hint": "65536"}
	res, err = resolveECBlockLayoutFromMetadata("bkt", "obj", meta, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EffectiveBlockSize != DefaultECBlockSize || res.Status != "ignored" || res.Reason != "unsupported-write-mode" {
		t.Fatalf("unexpected unsupported-write-mode resolution: %+v", res)
	}
	if len(meta) != 0 {
		t.Fatal("expected hint metadata to be stripped")
	}

	// 7. Enabled + fleet-confirmed: all 4 candidates apply cleanly and emit expected headers.
	for _, cand := range ECBlockSizeCandidates {
		meta = map[string]string{ECBlockSizeHintHeader: httpHeaderInt(cand)}
		res, err = resolveECBlockLayoutFromMetadata("bkt", "obj", meta, true)
		if err != nil {
			t.Fatalf("candidate %d failed: %v", cand, err)
		}
		if res.EffectiveBlockSize != cand || res.Status != "applied" || res.Reason != "" {
			t.Fatalf("candidate %d unexpected resolution: %+v", cand, res)
		}
		if len(meta) != 0 {
			t.Fatalf("candidate %d did not strip hint metadata", cand)
		}
		hdr := make(http.Header)
		setECLayoutHintHeaders(hdr, res)
		if got := hdr.Get(EffectiveECBlockSizeHeader); got != httpHeaderInt(cand) {
			t.Fatalf("header %s = %q, want %q", EffectiveECBlockSizeHeader, got, httpHeaderInt(cand))
		}
		if got := hdr.Get(ECLayoutHintStatusHeader); got != "applied" {
			t.Fatalf("header %s = %q, want applied", ECLayoutHintStatusHeader, got)
		}
		if got := hdr.Get(ECLayoutHintReasonHeader); got != "" {
			t.Fatalf("header %s = %q, want empty", ECLayoutHintReasonHeader, got)
		}
	}
}

func httpHeaderInt(v int64) string {
	h := make(http.Header)
	setEffectiveECBlockSizeHeader(h, v)
	return h.Get(EffectiveECBlockSizeHeader)
}
