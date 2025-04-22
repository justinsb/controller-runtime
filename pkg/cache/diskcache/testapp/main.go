package main

import (
	"context"
	"fmt"
	"os"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/cache/diskcache"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

type kubeObject struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
}

type configMap struct {
	kubeObject

	Data map[string]string `json:"data"`
}

func (c *kubeObject) GetNamespace() string {
	return c.Metadata.Namespace
}
func (c *kubeObject) GetName() string {
	return c.Metadata.Name
}

func run(ctx context.Context) error {
	// baseURL, err := url.Parse("https://1.2.3.4/")
	// if err != nil {
	// 	return fmt.Errorf("parsing url: %w", err)
	// }

	// tokenSource, err := google.DefaultTokenSource(ctx)
	// if err != nil {
	// 	return fmt.Errorf("building google default token source: %w", err)
	// }
	// httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	// httpTransport.TLSClientConfig = &tls.Config{
	// 	InsecureSkipVerify: true,
	// }
	// httpClient := &http.Client{
	// 	Transport: httpTransport,
	// }

	restConfig, err := config.GetConfig()
	if err != nil {
		return fmt.Errorf("geting rest config: %w", err)
	}
	httpClient, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return fmt.Errorf("getting http client: %w", err)
	}

	baseURL, versionedAPIPath, err := rest.DefaultServerUrlFor(restConfig)
	if err != nil {
		return fmt.Errorf("getting base url: %w", err)
	}
	klog.Infof("versionedAPIPath is %v", versionedAPIPath)

	clientOptions := diskcache.ClientOptions{
		BaseURI:    baseURL,
		HTTPClient: httpClient,
		// Authorization: tokenSource,
	}
	gvr := diskcache.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

	client := diskcache.NewStreamingClient[*configMap](clientOptions)

	listListener := &configMapListener{}
	if err := client.List(ctx, gvr, listListener); err != nil {
		return fmt.Errorf("listing: %w", err)
	}

	watch := true
	if watch {
		watchOptions := diskcache.WatchOptions{
			ResourceVersion:     listListener.metadata.ResourceVersion,
			AllowWatchBookmarks: true,
		}
		if err := client.Watch(ctx, gvr, watchOptions, &configMapWatchListener{}); err != nil {
			return fmt.Errorf("listing: %w", err)
		}
	}

	return nil
}

type configMapListener struct {
	metadata diskcache.ListMetadata
}

func (l *configMapListener) OnListBegin(metadata diskcache.ListMetadata) {
	klog.Infof("ListMetadata: %+v", metadata)
	l.metadata = metadata
}

func (l *configMapListener) OnListObject(obj *configMap) error {
	klog.Infof("configMap: %v/%v", obj.Metadata.Namespace, obj.Metadata.Name)
	return nil
}
func (l *configMapListener) OnListEnd() {

}

type configMapWatchListener struct {
}

func (l *configMapWatchListener) OnWatchEvent(eventType string, obj *configMap) error {
	if obj == nil {
		klog.Infof("Watch event %v: object is nil", eventType)
	} else {
		klog.Infof("Watch event %v: %v/%v", eventType, obj.Metadata.Namespace, obj.Metadata.Name)
	}
	return nil
}
