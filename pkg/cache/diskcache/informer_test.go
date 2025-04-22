package diskcache

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func TestInformer(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	t.Cleanup(cancel)

	restConfig, err := config.GetConfig()
	if err != nil {
		t.Fatalf("geting rest config: %v", err)
	}
	httpClient, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		t.Fatalf("geting http client: %v", err)
	}

	baseURL, versionedAPIPath, err := rest.DefaultServerUrlFor(restConfig)
	if err != nil {
		t.Fatalf("getting base url: %v", err)
	}
	t.Logf("versionedAPIPath is %v", versionedAPIPath)

	gvr := GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	informerOptions := DiskInformerOptions{
		HTTPClient: httpClient,
		BaseURL:    baseURL,
	}
	informer := NewDiskInformer[*corev1.ConfigMap](gvr, informerOptions)

	if informer.HasSynced() {
		t.Errorf("HasSynced was true before it should be")
	}

	go func() {
		informer.RunWithContext(ctx)
	}()

	for i := 0; i < 10; i++ {
		if informer.HasSynced() {
			break
		}
		time.Sleep(time.Second)
	}
	if !informer.HasSynced() {
		t.Errorf("HasSynced did not become true")
	}

	reader := informer.Reader()
	var list corev1.ConfigMapList

	if err := reader.List(ctx, &list); err != nil {
		t.Errorf("error from list: %v", err)
	}

	for _, obj := range list.Items {
		t.Logf("item: %v/%v", obj.GetNamespace(), obj.GetName())
	}

}
