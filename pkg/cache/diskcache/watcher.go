package diskcache

import (
	"context"
	"fmt"
	"sync/atomic"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Watcher[T Object] struct {
	client *StreamingClient[T]
	gvr    GroupVersionResource

	hasSynced atomic.Bool
	objects   map[namespacedName]T
}

type namespacedName struct {
	Namespace string
	Name      string
}

func NewWatcher[T Object](client *StreamingClient[T], gvr GroupVersionResource) *Watcher[T] {
	return &Watcher[T]{
		client:  client,
		gvr:     gvr,
		objects: make(map[namespacedName]T),
	}
}

func (w *Watcher[T]) RunWithContext(ctx context.Context) error {
	log := klog.FromContext(ctx)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := w.watchOnce(ctx); err != nil {
			log.Error(err, "error in informer")
			return err
		}
	}
}

func (w *Watcher[T]) HasSynced() bool {
	return w.hasSynced.Load()
}

func (w *Watcher[T]) watchOnce(ctx context.Context) error {
	listListener := &listListener[T]{
		watcher: w,
	}
	if err := w.client.List(ctx, w.gvr, listListener); err != nil {
		return fmt.Errorf("listing: %w", err)
	}

	w.hasSynced.Store(true)
	watchOptions := WatchOptions{
		ResourceVersion:     listListener.metadata.ResourceVersion,
		AllowWatchBookmarks: true,
	}
	// TODO: watcher could just implement watchListener and listListener
	watchListener := &watchListener[T]{
		watcher: w,
	}
	if err := w.client.Watch(ctx, w.gvr, watchOptions, watchListener); err != nil {
		return fmt.Errorf("listing: %w", err)
	}

	return nil
}

type watchListener[T Object] struct {
	watcher *Watcher[T]
}

func (l *watchListener[T]) OnWatchEvent(eventType string, obj T) error {
	nn := namespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
	switch eventType {
	case "ADDED":
		l.watcher.objects[nn] = obj
	case "MODIFIED":
		l.watcher.objects[nn] = obj
	default:
		return fmt.Errorf("unknown event type: %q", eventType)
	}

	return nil
}

type listListener[T Object] struct {
	metadata ListMetadata
	watcher  *Watcher[T]
}

func (l *listListener[T]) OnListBegin(metadata ListMetadata) {
	klog.Infof("ListMetadata: %+v", metadata)
	l.metadata = metadata
}

func (l *listListener[T]) OnListObject(obj T) error {
	nn := namespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
	l.watcher.objects[nn] = obj

	// klog.Infof("configMap: %v/%v", obj.GetNamespace(), obj.GetName())
	return nil
}

func (l *listListener[T]) OnListEnd() {

}

type watchReader[T Object] struct {
	watcher *Watcher[T]
}

// Get retrieves an obj for the given object key from the Kubernetes Cluster.
// obj must be a struct pointer so that obj can be updated with the response
// returned by the Server.
func (r *watchReader[T]) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if len(opts) != 0 {
		return fmt.Errorf("options not implemented by watchReader::Get")
	}

	return fmt.Errorf("watchReader::Get not implemented")
}

// List retrieves list of objects for a given namespace and list options. On a
// successful call, Items field in the list will be populated with the
// result returned from the server.
func (r *watchReader[T]) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if len(opts) != 0 {
		return fmt.Errorf("options not implemented by watchReader::Get")
	}

	objects := r.watcher.objects

	var allItems []runtime.Object
	for _, obj := range objects {
		runtimeObject, ok := any(obj).(runtime.Object)
		if !ok {
			panic("not a runtime object")
		}
		allItems = append(allItems, runtimeObject)
	}

	// listAccessor, err := apimeta.ListAccessor(list)
	// if err != nil {
	// 	return err
	// }

	// listAccessor.SetResourceVersion(resourceVersion)

	if err := apimeta.SetList(list, allItems); err != nil {
		return err
	}

	list.SetContinue("continue-not-supported")
	return nil

}
