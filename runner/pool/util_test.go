// Copyright 2025 Cloudbase Solutions SRL
//
//    Licensed under the Apache License, Version 2.0 (the "License"); you may
//    not use this file except in compliance with the License. You may obtain
//    a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//    License for the specific language governing permissions and limitations
//    under the License.

package pool

import (
	"sync"
	"testing"

	runnerErrors "github.com/cloudbase/garm-provider-common/errors"
	"github.com/cloudbase/garm/params"
)

func TestPoolRoundRobinCandidatesPreserveFallbackOrder(t *testing.T) {
	p := &poolRoundRobin{
		pools: []params.Pool{
			{ID: "1"},
			{ID: "2"},
			{ID: "3"},
		},
	}

	first, err := p.Candidates()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	assertPoolIDs(t, first, []string{"1", "2", "3"})

	second, err := p.Candidates()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	assertPoolIDs(t, second, []string{"2", "3", "1"})
}

func TestPoolRoundRobinEmptyPoolErrorsOut(t *testing.T) {
	p := &poolRoundRobin{}

	_, err := p.Candidates()
	if err != runnerErrors.ErrNoPoolsAvailable {
		t.Fatalf("expected ErrNoPoolsAvailable from Candidates, got %s", err)
	}
}

func TestPoolRoundRobinLen(t *testing.T) {
	p := &poolRoundRobin{
		pools: []params.Pool{
			{
				ID: "1",
			},
			{
				ID: "2",
			},
		},
	}

	if p.Len() != 2 {
		t.Fatalf("expected 2, got %d", p.Len())
	}
}

func TestPoolRoundRobinReset(t *testing.T) {
	p := &poolRoundRobin{
		pools: []params.Pool{
			{
				ID: "1",
			},
			{
				ID: "2",
			},
		},
	}

	if _, err := p.Candidates(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	p.Reset()
	if p.next != 0 {
		t.Fatalf("expected 0, got %d", p.next)
	}
}

func TestPoolsForTagsPackGet(t *testing.T) {
	p := &poolsForTags{
		poolCacheType: params.PoolBalancerTypePack,
	}

	pools := []params.Pool{
		{
			ID:       "1",
			Priority: 0,
		},
		{
			ID:       "2",
			Priority: 100,
		},
	}
	_ = p.Add([]string{"key"}, pools)
	cache, ok := p.Get([]string{"key"})
	if !ok {
		t.Fatalf("expected true, got false")
	}
	if cache.Len() != 2 {
		t.Fatalf("expected 2, got %d", cache.Len())
	}

	poolRR, ok := cache.(*poolRoundRobin)
	if !ok {
		t.Fatalf("expected poolRoundRobin, got %v", cache)
	}
	if poolRR.next != 0 {
		t.Fatalf("expected 0, got %d", poolRR.next)
	}
	candidates, err := poolRR.Candidates()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	assertPoolIDs(t, candidates, []string{"2", "1"})

	if poolRR.next != 1 {
		t.Fatalf("expected 1, got %d", poolRR.next)
	}
	// Getting the pool cache again should reset next
	cache, ok = p.Get([]string{"key"})
	if !ok {
		t.Fatalf("expected true, got false")
	}
	poolRR, ok = cache.(*poolRoundRobin)
	if !ok {
		t.Fatalf("expected poolRoundRobin, got %v", cache)
	}
	if poolRR.next != 0 {
		t.Fatalf("expected 0, got %d", poolRR.next)
	}

	candidates, err = poolRR.Candidates()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	assertPoolIDs(t, candidates, []string{"2", "1"})
}

func TestPoolsForTagsRoundRobinGet(t *testing.T) {
	p := &poolsForTags{
		poolCacheType: params.PoolBalancerTypeRoundRobin,
	}

	pools := []params.Pool{
		{
			ID:       "1",
			Priority: 0,
		},
		{
			ID:       "2",
			Priority: 100,
		},
	}
	_ = p.Add([]string{"key"}, pools)
	cache, ok := p.Get([]string{"key"})
	if !ok {
		t.Fatalf("expected true, got false")
	}
	if cache.Len() != 2 {
		t.Fatalf("expected 2, got %d", cache.Len())
	}

	candidates, err := cache.Candidates()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	assertPoolIDs(t, candidates, []string{"2", "1"})
	// Getting the pool cache again should not reset next, and
	// should return the next pool.
	cache, ok = p.Get([]string{"key"})
	if !ok {
		t.Fatalf("expected true, got false")
	}
	candidates, err = cache.Candidates()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	assertPoolIDs(t, candidates, []string{"1", "2"})
}

func TestPoolsForTagsNoPoolsForTag(t *testing.T) {
	p := &poolsForTags{
		pools: sync.Map{},
	}

	_, ok := p.Get([]string{"key"})
	if ok {
		t.Fatalf("expected false, got true")
	}
}

func TestPoolsForTagsBalancerTypePack(t *testing.T) {
	p := &poolsForTags{
		pools:         sync.Map{},
		poolCacheType: params.PoolBalancerTypePack,
	}

	poolCache := &poolRoundRobin{}
	p.pools.Store("key", poolCache)

	cache, ok := p.Get([]string{"key"})
	if !ok {
		t.Fatalf("expected true, got false")
	}
	if cache != poolCache {
		t.Fatalf("expected poolCache, got %v", cache)
	}
	if poolCache.next != 0 {
		t.Fatalf("expected 0, got %d", poolCache.next)
	}
}

func assertPoolIDs(t *testing.T, actual []params.Pool, expected []string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("expected %d pools, got %d", len(expected), len(actual))
	}
	for index, expectedID := range expected {
		if actual[index].ID != expectedID {
			t.Fatalf("pool %d: expected %s, got %s", index, expectedID, actual[index].ID)
		}
	}
}
