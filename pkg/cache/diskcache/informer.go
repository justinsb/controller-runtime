package diskcache

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// NewDiskInformer has the same function signature as cached.NewShardIndexInformer
// TODO: We could probably do this much more easiliy
func BuildNewDiskInformerBridge(scheme *runtime.Scheme, opt DiskInformerOptions) func(cache.ListerWatcher, runtime.Object, time.Duration, cache.Indexers) cache.SharedIndexInformer {
	return func(rw cache.ListerWatcher, exampleObject runtime.Object, defaultEventHandlerResyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
		gvk, err := apiutil.GVKForObject(exampleObject, scheme)
		if err != nil {
			klog.Fatalf("getting gvk: %v", err)
		}

		gvr := GroupVersionResource{
			Group:    gvk.Group,
			Version:  gvk.Version,
			Resource: "TODO",
		}
		return NewDiskInformer[client.Object](gvr, opt)
	}
}

type DiskInformerOptions struct {
	HTTPClient *http.Client
	BaseURL    *url.URL
}

func NewDiskInformer[T Object](gvr GroupVersionResource, opt DiskInformerOptions) *DiskInformer[T] {
	return &DiskInformer[T]{
		gvr:        gvr,
		httpClient: opt.HTTPClient,
		baseURL:    *opt.BaseURL,
	}
}

type DiskInformer[T Object] struct {
	gvr        GroupVersionResource
	httpClient *http.Client
	baseURL    url.URL

	watcher *Watcher[T]
}

func (i *DiskInformer[T]) Reader() client.Reader {
	return &watchReader[T]{watcher: i.watcher}
}

var _ cache.SharedIndexInformer = &DiskInformer[client.Object]{}

// AddEventHandler adds an event handler to the shared informer using
// the shared informer's resync period.  Events to a single handler are
// delivered sequentially, but there is no coordination between
// different handlers.
// It returns a registration handle for the handler that can be used to
// remove the handler again, or to tell if the handler is synced (has
// seen every item in the initial list).
//
// Contextual logging: AddEventHandlerWithOptions together with a logger in the options should be used instead of AddEventHandler in code which supports contextual logging.
func (i *DiskInformer[T]) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	panic("not implemented")
}

// AddEventHandlerWithResyncPeriod adds an event handler to the
// shared informer with the requested resync period; zero means
// this handler does not care about resyncs.  The resync operation
// consists of delivering to the handler an update notification
// for every object in the informer's local cache; it does not add
// any interactions with the authoritative storage.  Some
// informers do no resyncs at all, not even for handlers added
// with a non-zero resyncPeriod.  For an informer that does
// resyncs, and for each handler that requests resyncs, that
// informer develops a nominal resync period that is no shorter
// than the requested period but may be longer.  The actual time
// between any two resyncs may be longer than the nominal period
// because the implementation takes time to do work and there may
// be competing load and scheduling noise.
// It returns a registration handle for the handler that can be used to remove
// the handler again and an error if the handler cannot be added.
//
// Contextual logging: AddEventHandlerWithOptions together with a logger in the options should be used instead of AddEventHandlerWithResyncPeriod in code which supports contextual logging.
func (i *DiskInformer[T]) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, resyncPeriod time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	panic("not implemented")
}

// AddEventHandlerWithOptions is a variant of AddEventHandlerWithResyncPeriod where
// all optional parameters are passed in a struct.
func (i *DiskInformer[T]) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	panic("not implemented")
}

// RemoveEventHandler removes a formerly added event handler given by
// its registration handle.
// This function is guaranteed to be idempotent, and thread-safe.
func (i *DiskInformer[T]) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	panic("not implemented")
}

// GetStore returns the informer's local cache as a Store.
func (i *DiskInformer[T]) GetStore() cache.Store {
	panic("not implemented")
}

// GetController is deprecated, it does nothing useful
func (i *DiskInformer[T]) GetController() cache.Controller {
	panic("not implemented")
}

// Run starts and runs the shared informer, returning after it stops.
// The informer will be stopped when stopCh is closed.
//
// Contextual logging: RunWithContext should be used instead of Run in code which uses contextual logging.
func (i *DiskInformer[T]) Run(stopCh <-chan struct{}) {
	panic("not implemented")
}

// RunWithContext starts and runs the shared informer, returning after it stops.
// The informer will be stopped when the context is canceled.
func (i *DiskInformer[T]) RunWithContext(ctx context.Context) {
	log := klog.FromContext(ctx)

	client := NewStreamingClient[T](ClientOptions{
		HTTPClient:    i.httpClient,
		BaseURI:       &i.baseURL,
		Authorization: nil,
	})

	i.watcher = NewWatcher(client, i.gvr)

	err := i.watcher.RunWithContext(ctx)
	log.Error(err, "watcher exited")
}

// HasSynced returns true if the shared informer's store has been
// informed by at least one full LIST of the authoritative state
// of the informer's object collection.  This is unrelated to "resync".
//
// Note that this doesn't tell you if an individual handler is synced!!
// For that, please call HasSynced on the handle returned by
// AddEventHandler.
func (i *DiskInformer[T]) HasSynced() bool {
	if i.watcher == nil {
		return false
	}
	return i.watcher.HasSynced()
}

// LastSyncResourceVersion is the resource version observed when last synced with the underlying
// store. The value returned is not synchronized with access to the underlying store and is not
// thread-safe.
func (i *DiskInformer[T]) LastSyncResourceVersion() string {
	panic("not implemented")
}

// The WatchErrorHandler is called whenever ListAndWatch drops the
// connection with an error. After calling this handler, the informer
// will backoff and retry.
//
// The default implementation looks at the error type and tries to log
// the error message at an appropriate level.
//
// There's only one handler, so if you call this multiple times, last one
// wins; calling after the informer has been started returns an error.
//
// The handler is intended for visibility, not to e.g. pause the consumers.
// The handler should return quickly - any expensive processing should be
// offloaded.
//
// Contextual logging: SetWatchErrorHandlerWithContext should be used instead of SetWatchErrorHandler in code which supports contextual logging.
func (i *DiskInformer[T]) SetWatchErrorHandler(handler cache.WatchErrorHandler) error {
	panic("not implemented")
}

// SetWatchErrorHandlerWithContext is a variant of SetWatchErrorHandler where
// the handler is passed an additional context parameter.
func (i *DiskInformer[T]) SetWatchErrorHandlerWithContext(handler cache.WatchErrorHandlerWithContext) error {
	panic("not implemented")
}

// The TransformFunc is called for each object which is about to be stored.
//
// This function is intended for you to take the opportunity to
// remove, transform, or normalize fields. One use case is to strip unused
// metadata fields out of objects to save on RAM cost.
//
// Must be set before starting the informer.
//
// Please see the comment on TransformFunc for more details.
func (i *DiskInformer[T]) SetTransform(handler cache.TransformFunc) error {
	panic("not implemented")
}

// IsStopped reports whether the informer has already been stopped.
// Adding event handlers to already stopped informers is not possible.
// An informer already stopped will never be started again.
func (i *DiskInformer[T]) IsStopped() bool {
	panic("not implemented")
}

// AddIndexers add indexers to the informer before it starts.
func (i *DiskInformer[T]) AddIndexers(indexers cache.Indexers) error {
	panic("not implemented")
}

func (i *DiskInformer[T]) GetIndexer() cache.Indexer {
	panic("not implemented")
}
