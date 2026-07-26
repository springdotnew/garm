// Copyright 2025 Cloudbase Solutions SRL
//
//	Licensed under the Apache License, Version 2.0 (the "License"); you may
//	not use this file except in compliance with the License. You may obtain
//	a copy of the License at
//
//	     http://www.apache.org/licenses/LICENSE-2.0
//
//	Unless required by applicable law or agreed to in writing, software
//	distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//	WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//	License for the specific language governing permissions and limitations
//	under the License.
package cache

import (
	"sync"

	"github.com/cloudbase/garm/runner/common"
)

var ghClientCache *GithubClientCache

type GithubClientCache struct {
	mux sync.Mutex

	cache      map[string]githubClientCacheEntry
	generation uint64
}

type githubClientCacheEntry struct {
	client     common.GithubClient
	generation uint64
}

func init() {
	clientCache := &GithubClientCache{
		cache: make(map[string]githubClientCacheEntry),
	}
	ghClientCache = clientCache
}

func (g *GithubClientCache) SetClient(entityID string, client common.GithubClient) {
	g.mux.Lock()
	defer g.mux.Unlock()

	g.generation++
	g.cache[entityID] = githubClientCacheEntry{
		client:     client,
		generation: g.generation,
	}
}

func (g *GithubClientCache) GetClient(entityID string) (common.GithubClient, bool) {
	client, _, ok := g.GetClientVersion(entityID)
	return client, ok
}

func (g *GithubClientCache) GetClientVersion(entityID string) (common.GithubClient, uint64, bool) {
	g.mux.Lock()
	defer g.mux.Unlock()

	if entry, ok := g.cache[entityID]; ok {
		return entry.client, entry.generation, true
	}
	return nil, 0, false
}

func (g *GithubClientCache) DeleteClient(entityID string) {
	g.mux.Lock()
	defer g.mux.Unlock()

	delete(g.cache, entityID)
}

func SetGithubClient(entityID string, client common.GithubClient) {
	ghClientCache.SetClient(entityID, client)
}

func GetGithubClient(entityID string) (common.GithubClient, bool) {
	return ghClientCache.GetClient(entityID)
}

func GetGithubClientVersion(entityID string) (common.GithubClient, uint64, bool) {
	return ghClientCache.GetClientVersion(entityID)
}

func DeleteGithubClient(entityID string) {
	ghClientCache.DeleteClient(entityID)
}
