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
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/controller-runtime/pkg/cache/internal"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var (
	_ Informers     = &diskCache{}
	_ client.Reader = &diskCache{}
	_ Cache         = &diskCache{}
)

// diskCache is a Kubernetes Object cache populated from internal.Informers.
// diskCache wraps internal.Informers.
type diskCache struct {
	scheme *runtime.Scheme
	//*internal.Informers
	readerFailOnMissingInformer bool
}

func newDiskCache(restConfig *rest.Config, opts Options, config Config, namespace string) Cache {
	return &diskCache{
		scheme:                      opts.Scheme,
		readerFailOnMissingInformer: opts.ReaderFailOnMissingInformer,
	}
}

// Get implements Reader.
func (ic *diskCache) Get(ctx context.Context, key client.ObjectKey, out client.Object, opts ...client.GetOption) error {
	gvk, err := apiutil.GVKForObject(out, ic.scheme)
	if err != nil {
		return err
	}

	started, cache, err := ic.getInformerForKind(ctx, gvk, out)
	if err != nil {
		return err
	}

	if !started {
		return &ErrCacheNotStarted{}
	}
	return cache.Reader.Get(ctx, key, out, opts...)
}

// List implements Reader.
func (ic *diskCache) List(ctx context.Context, out client.ObjectList, opts ...client.ListOption) error {
	gvk, cacheTypeObj, err := ic.objectTypeForListObject(out)
	if err != nil {
		return err
	}

	started, cache, err := ic.getInformerForKind(ctx, *gvk, cacheTypeObj)
	if err != nil {
		return err
	}

	if !started {
		return &ErrCacheNotStarted{}
	}

	return cache.Reader.List(ctx, out, opts...)
}

// objectTypeForListObject tries to find the runtime.Object and associated GVK
// for a single object corresponding to the passed-in list type. We need them
// because they are used as cache map key.
func (ic *diskCache) objectTypeForListObject(list client.ObjectList) (*schema.GroupVersionKind, runtime.Object, error) {
	gvk, err := apiutil.GVKForObject(list, ic.scheme)
	if err != nil {
		return nil, nil, err
	}

	// We need the non-list GVK, so chop off the "List" from the end of the kind.
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")

	// Handle unstructured.UnstructuredList.
	if _, isUnstructured := list.(runtime.Unstructured); isUnstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		return &gvk, u, nil
	}
	// Handle metav1.PartialObjectMetadataList.
	if _, isPartialObjectMetadata := list.(*metav1.PartialObjectMetadataList); isPartialObjectMetadata {
		pom := &metav1.PartialObjectMetadata{}
		pom.SetGroupVersionKind(gvk)
		return &gvk, pom, nil
	}

	// Any other list type should have a corresponding non-list type registered
	// in the scheme. Use that to create a new instance of the non-list type.
	cacheTypeObj, err := ic.scheme.New(gvk)
	if err != nil {
		return nil, nil, err
	}
	return &gvk, cacheTypeObj, nil
}

// GetInformerForKind returns the informer for the GroupVersionKind. If no informer exists, one will be started.
func (ic *diskCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...InformerGetOption) (Informer, error) {
	// Map the gvk to an object
	obj, err := ic.scheme.New(gvk)
	if err != nil {
		return nil, err
	}

	_, i, err := ic.Informers.Get(ctx, gvk, obj, applyGetOptions(opts...))
	if err != nil {
		return nil, err
	}
	return i.Informer, nil
}

// GetInformer returns the informer for the obj. If no informer exists, one will be started.
func (ic *diskCache) GetInformer(ctx context.Context, obj client.Object, opts ...InformerGetOption) (Informer, error) {
	gvk, err := apiutil.GVKForObject(obj, ic.scheme)
	if err != nil {
		return nil, err
	}

	_, i, err := ic.Informers.Get(ctx, gvk, obj, applyGetOptions(opts...))
	if err != nil {
		return nil, err
	}
	return i.Informer, nil
}

func (ic *diskCache) getInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, obj runtime.Object) (bool, *internal.Cache, error) {
	if ic.readerFailOnMissingInformer {
		cache, started, ok := ic.Informers.Peek(gvk, obj)
		if !ok {
			return false, nil, &ErrResourceNotCached{GVK: gvk}
		}
		return started, cache, nil
	}

	return ic.Informers.Get(ctx, gvk, obj, &internal.GetOptions{})
}

// RemoveInformer deactivates and removes the informer from the cache.
func (ic *diskCache) RemoveInformer(_ context.Context, obj client.Object) error {
	gvk, err := apiutil.GVKForObject(obj, ic.scheme)
	if err != nil {
		return err
	}

	ic.Informers.Remove(gvk, obj)
	return nil
}

// NeedLeaderElection implements the LeaderElectionRunnable interface
// to indicate that this can be started without requiring the leader lock.
func (ic *diskCache) NeedLeaderElection() bool {
	return false
}

// IndexField adds an indexer to the underlying informer, using extractValue function to get
// value(s) from the given field. This index can then be used by passing a field selector
// to List. For one-to-one compatibility with "normal" field selectors, only return one value.
// The values may be anything. They will automatically be prefixed with the namespace of the
// given object, if present. The objects passed are guaranteed to be objects of the correct type.
func (ic *diskCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	informer, err := ic.GetInformer(ctx, obj)
	if err != nil {
		return err
	}
	return indexByField(informer, field, extractValue)
}

// // GetInformer fetches or constructs an informer for the given object that corresponds to a single
// // API kind and resource.
// func (c *diskCache) GetInformer(ctx context.Context, obj client.Object, opts ...InformerGetOption) (Informer, error) {
// 	return nil, fmt.Errorf("GetInformer not (yet) supported by diskCache")
// }

// // GetInformerForKind is similar to GetInformer, except that it takes a group-version-kind, instead
// // of the underlying object.
// func (c *diskCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...InformerGetOption) (Informer, error) {
// 	return nil, fmt.Errorf("GetInformerForKind not (yet) supported by diskCache")
// }

// // RemoveInformer removes an informer entry and stops it if it was running.
// func (c *diskCache) RemoveInformer(ctx context.Context, obj client.Object) error {
// 	return nil, fmt.Errorf("RemoveInformer not (yet) supported by diskCache")
// }

// Start runs all the informers known to this cache until the context is closed.
// It blocks.
func (c *diskCache) Start(ctx context.Context) error {
	return fmt.Errorf("Start not (yet) supported by diskCache")
}

// WaitForCacheSync waits for all the caches to sync. Returns false if it could not sync a cache.
func (c *diskCache) WaitForCacheSync(ctx context.Context) bool {
	panic("WaitForCacheSync not (yet) supported by diskCache")
	return false
}
