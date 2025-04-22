/*
Copyright 2018 The Kubernetes Authors.

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

package cache

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

// NewDiskCache initializes and returns a new disk-backed Cache.
func NewDiskCache(cfg *rest.Config, opts Options) (Cache, error) {
	opts, err := defaultOpts(cfg, opts)
	if err != nil {
		return nil, err
	}

	newCacheFunc := newDiskCacheFunc(cfg, opts)

	var defaultCache Cache
	if len(opts.DefaultNamespaces) > 0 {
		return nil, fmt.Errorf("DefaultNamespaces not (yet) supported for disk cache")
		// defaultConfig := optionDefaultsToConfig(&opts)
		// defaultCache = newMultiNamespaceCache(newCacheFunc, opts.Scheme, opts.Mapper, opts.DefaultNamespaces, &defaultConfig)
	} else {
		defaultCache = newCacheFunc(optionDefaultsToConfig(&opts), corev1.NamespaceAll)
	}

	if len(opts.ByObject) == 0 {
		return defaultCache, nil
	}

	return nil, fmt.Errorf("ByObject not (yet) supported for disk cache")
	// delegating := &delegatingByGVKCache{
	// 	scheme:       opts.Scheme,
	// 	caches:       make(map[schema.GroupVersionKind]Cache, len(opts.ByObject)),
	// 	defaultCache: defaultCache,
	// }

	// for obj, config := range opts.ByObject {
	// 	gvk, err := apiutil.GVKForObject(obj, opts.Scheme)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("failed to get GVK for type %T: %w", obj, err)
	// 	}
	// 	var cache Cache
	// 	if len(config.Namespaces) > 0 {
	// 		cache = newMultiNamespaceCache(newCacheFunc, opts.Scheme, opts.Mapper, config.Namespaces, nil)
	// 	} else {
	// 		cache = newCacheFunc(byObjectToConfig(config), corev1.NamespaceAll)
	// 	}
	// 	delegating.caches[gvk] = cache
	// }

	// return delegating, nil
}

type buildDiskCacheFunc func(config Config, namespace string) Cache

func newDiskCacheFunc(restConfig *rest.Config, opts Options) buildDiskCacheFunc {
	return func(config Config, namespace string) Cache {
		return newDiskCache(restConfig, opts, config, namespace)
	}
}
