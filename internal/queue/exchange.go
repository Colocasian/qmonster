/*
Copyright 2022 Rishvic Pushpakaran

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package queue

import "sync"

type Exchange struct {
	m  map[string]Queue
	mu sync.RWMutex
}

func NewExchange() *Exchange {
	return &Exchange{m: make(map[string]Queue)}
}

func (ex *Exchange) Load(key string) (q Queue, ok bool) {
	ex.mu.RLock()
	q, ok = ex.m[key]
	ex.mu.RUnlock()
	return q, ok
}

func (ex *Exchange) Store(key string, q Queue) {
	ex.mu.Lock()
	ex.m[key] = q
	ex.mu.Unlock()
}

func (ex *Exchange) LoadOrStore(key string, q Queue) (actual Queue, loaded bool) {
	actual, loaded = ex.Load(key)
	if loaded {
		return actual, true
	}

	ex.Store(key, q)
	return q, false
}
