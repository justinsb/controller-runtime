package diskcache

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/oauth2"
	"k8s.io/klog/v2"
)

type ClientOptions struct {
	HTTPClient    *http.Client
	BaseURI       *url.URL
	Authorization oauth2.TokenSource
}
type StreamingClient[T Object] struct {
	httpClient    *http.Client
	baseURI       url.URL
	authorization oauth2.TokenSource
}

type GroupVersionResource struct {
	Group    string
	Version  string
	Resource string
}

func NewStreamingClient[T Object](opt ClientOptions) *StreamingClient[T] {
	return &StreamingClient[T]{
		httpClient:    opt.HTTPClient,
		baseURI:       *opt.BaseURI,
		authorization: opt.Authorization,
	}
}

// var _ TypedReader[Object] = &StreamingClient[Object]

type ListMetadata struct {
	APIVersion      string
	Kind            string
	ResourceVersion string
}

type ListListener[T Object] interface {
	OnListBegin(metadata ListMetadata)
	OnListObject(obj T) error
	OnListEnd()
}

func (c *StreamingClient[T]) userAgent() string {
	return "StreamingClient" // TODO
}

func (c *StreamingClient[T]) setHeaders(request *http.Request) error {
	request.Header.Set("User-Agent", c.userAgent())
	if c.authorization != nil {
		token, err := c.authorization.Token()
		if err != nil {
			return fmt.Errorf("getting authorization token: %w", err)
		}
		if token != nil && token.AccessToken != "" {
			request.Header.Set("Authorization", "Bearer "+token.AccessToken)
		}
	}
	return nil
}

// func (c *StreamingClient[T]) List(ctx context.Context, gvr GroupVersionResource, listener ListListener[T]) error {
// 	u := c.constructURL(gvr)

// 	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
// 	if err != nil {
// 		return fmt.Errorf("building http request: %w", err)
// 	}
// 	if err := c.setHeaders(request); err != nil {
// 		return err
// 	}
// 	request.Header.Set("Accept", "application/json")

// 	response, err := c.httpClient.Do(request)
// 	if err != nil {
// 		return fmt.Errorf("sending http request: %w", err)
// 	}
// 	defer response.Body.Close()

// 	if response.StatusCode != http.StatusOK {
// 		return fmt.Errorf("unexpected status from %v: %v", u, response.Status)
// 	}

// 	// TODO: Implement true streaming parsing (likely with https://github.com/golang/go/issues/71497)
// 	b, err := io.ReadAll(response.Body)
// 	if err != nil {
// 		return fmt.Errorf("reading response body from %v: %w", u, err)
// 	}

// 	type listT struct {
// 		APIVersion string `json:"apiVersion"`
// 		Kind       string `json:"kind"`
// 		Metadata   struct {
// 			ResourceVersion string `json:"resourceVersion"`
// 		} `json:"metadata"`
// 		Items []json.RawMessage `json:"items"`
// 	}
// 	var list listT
// 	if err := json.Unmarshal(b, &list); err != nil {
// 		return fmt.Errorf("decoding response body from %v: %w", u, err)
// 	}

// 	listener.BeginObjects(ListMetadata{
// 		APIVersion:      list.APIVersion,
// 		Kind:            list.Kind,
// 		ResourceVersion: list.Metadata.ResourceVersion,
// 	})

// 	for _, item := range list.Items {
// 		var t T
// 		if err := json.Unmarshal(item, &t); err != nil {
// 			return fmt.Errorf("decoding %T from %v: %w", t, u, err)
// 		}
// 		if err := listener.Object(&t); err != nil {
// 			return err
// 		}
// 	}

// 	listener.EndObjects()

// 	return nil
// }

func (c *StreamingClient[T]) List(ctx context.Context, gvr GroupVersionResource, listener ListListener[T]) error {
	u := c.constructURL(gvr)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("building http request: %w", err)
	}
	if err := c.setHeaders(request); err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("sending http request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status from %v: %v", u, response.Status)
	}

	r := bufio.NewReader(response.Body)
	var header bytes.Buffer

	for {
		b, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("reading header from %v: %w", u, err)
		}
		header.WriteByte(b)
		if strings.HasSuffix(header.String(), "\"items\"") { // TODO: Optimize
			for {
				b, err := r.ReadByte()
				if err != nil {
					return fmt.Errorf("reading header from %v: %w", u, err)
				}
				header.WriteByte(b)
				if b == '[' {
					break
				}
			}
			break
		}
	}
	header.WriteString("]}") // close items

	type listT struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Items []json.RawMessage `json:"items"`
	}
	var list listT
	if err := json.Unmarshal(header.Bytes(), &list); err != nil {
		klog.Infof("header is %v", header.String())
		return fmt.Errorf("decoding response body from %v: %w", u, err)
	}

	listener.OnListBegin(ListMetadata{
		APIVersion:      list.APIVersion,
		Kind:            list.Kind,
		ResourceVersion: list.Metadata.ResourceVersion,
	})

	objectSplitter := &objectSplitReader{inner: r}

	// for _, item := range list.Items {
	for {
		jsonDecoder := json.NewDecoder(objectSplitter)
		var t T
		if err := jsonDecoder.Decode(&t); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decoding %T from %v: %w", t, u, err)
		}
		if err := listener.OnListObject(t); err != nil {
			return err
		}

		objectSplitter.objectEOF = false
	}

	listener.OnListEnd()

	return nil
}

type WatchOptions struct {
	ResourceVersion     string
	AllowWatchBookmarks bool
}

type WatchListener[T Object] interface {
	OnWatchEvent(eventType string, object T) error
}

func (c *StreamingClient[T]) Watch(ctx context.Context, gvr GroupVersionResource, watchOptions WatchOptions, listener WatchListener[T]) error {
	u := c.constructURL(gvr)

	q := u.Query()
	q.Set("watch", "true")
	if watchOptions.ResourceVersion != "" {
		q.Set("resourceVersion", watchOptions.ResourceVersion)
	}
	if watchOptions.AllowWatchBookmarks {
		q.Set("allowWatchBookmarks", "true")
	}
	u.RawQuery = q.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("building http request: %w", err)
	}
	if err := c.setHeaders(request); err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("sending http request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status from %v: %v", u, response.Status)
	}

	type eventT struct {
		Type   string          `json:"type"`
		Object json.RawMessage `json:"object"`
	}
	lineReader := &lineSplitReader{inner: response.Body}
	for {
		var event eventT

		jsonDecoder := json.NewDecoder(lineReader)
		if err := jsonDecoder.Decode(&event); err != nil {
			return fmt.Errorf("decoding event from %v: %w", u, err)
		}

		var object T
		if event.Object != nil {
			var t T
			if err := json.Unmarshal(event.Object, &t); err != nil {
				return fmt.Errorf("decoding %T from %v: %w", t, u, err)
			}
			object = t
		}
		if err := listener.OnWatchEvent(event.Type, object); err != nil {
			return err
		}

		// reset lineEOF to read next line
		lineReader.lineEOF = false
	}
}

type lineSplitReader struct {
	inner    io.Reader
	buffer   []byte
	innerEOF bool

	lineEOF bool
}

var _ io.Reader = &lineSplitReader{}

const lineReaderBufferSize = 8192

func (r *lineSplitReader) Read(p []byte) (int, error) {
	// Send io.EOF until we are reset
	if r.lineEOF {
		return 0, io.EOF
	}
	if len(r.buffer) == 0 {
		if r.buffer == nil {
			r.buffer = make([]byte, lineReaderBufferSize)
		}
		n, err := r.inner.Read(r.buffer[0:cap(r.buffer)])
		if err != nil {
			if err == io.EOF {
				r.innerEOF = true
			} else {
				return 0, err
			}
		}

		klog.Infof("Read %v bytes from inner reader", n)

		if n == 0 {
			if r.innerEOF {
				return 0, io.EOF
			}
		}

		r.buffer = r.buffer[:n]
		klog.Infof("buffer is %v", string(r.buffer))
	}

	// Only return up to the newline
	ret := r.buffer
	nl := bytes.IndexByte(r.buffer, '\n')
	if nl != -1 {
		ret = r.buffer[:nl+1]
	}

	// Return what fits into the target buffer
	n := copy(p, ret)
	r.buffer = r.buffer[n:]
	if len(r.buffer) == 0 {
		// release the buffer (maybe a sync pool would be faster)
		r.buffer = nil
	}
	klog.Infof("returning %v", string(p[:n]))

	// If we sent a newline, pretend this is EOF
	if nl != -1 && n == nl+1 {
		r.lineEOF = true
		return n, io.EOF
	}
	return n, nil
}

func (c *StreamingClient[T]) constructURL(gvr GroupVersionResource) *url.URL {
	u := &c.baseURI
	if gvr.Group != "" {
		u = u.JoinPath("apis", gvr.Group, gvr.Version, gvr.Resource)
	} else {
		u = u.JoinPath("api", gvr.Version, gvr.Resource)
	}
	return u
}

type objectSplitReader struct {
	inner    io.Reader
	buffer   []byte
	innerEOF bool

	objectEOF bool

	objectDepth    int
	inString       bool
	inStringEscape bool
}

var _ io.Reader = &lineSplitReader{}

const objectSplitReaderBufferSize = 8192

func (r *objectSplitReader) Read(p []byte) (int, error) {
	// Send io.EOF until we are reset
	if r.objectEOF {
		return 0, io.EOF
	}
	if len(r.buffer) == 0 {
		if r.buffer == nil {
			r.buffer = make([]byte, objectSplitReaderBufferSize)
		}
		n, err := r.inner.Read(r.buffer[0:cap(r.buffer)])
		if err != nil {
			if err == io.EOF {
				r.innerEOF = true
			} else {
				return 0, err
			}
		}

		// klog.Infof("Read %v bytes from inner reader", n)

		if n == 0 {
			if r.innerEOF {
				return 0, io.EOF
			}
		}

		r.buffer = r.buffer[:n]
		// klog.Infof("buffer is %v", string(r.buffer))
	}

	objectDepth := r.objectDepth
	inString := r.inString
	inStringEscape := r.inStringEscape

	n := 0
	max := min(len(r.buffer), len(p))
	for i := 0; i < max; i++ {
		c := r.buffer[0]
		r.buffer = r.buffer[1:]

		if inStringEscape {
			inStringEscape = false
			p[n] = c
			n++
			continue
		}

		if inString {
			if c == '\\' {
				inStringEscape = true
			} else if c == '"' {
				inString = false
			}
			p[n] = c
			n++
			continue
		}

		if c == ',' && n == 0 && objectDepth == 0 {
			// Skip over commas between objects
			continue
		}

		if c == ']' && n == 0 && objectDepth == 0 {
			// This looks like the end of the list, stop returning objects
			// (currently we return the trailer)
			trailer := r.buffer
			b, err := io.ReadAll(r.inner)
			if err != nil {
				if err != io.EOF {
					return 0, err
				}
			}
			trailer = append(trailer, b...)
			klog.Warningf("trailer is %v", string(trailer))
			r.buffer = nil
			return 0, io.EOF
		}

		if c == '}' {
			objectDepth--
			if objectDepth == 0 {
				r.objectEOF = true
				p[n] = c
				n++
				break
			}
		}

		if c == '{' {
			objectDepth++
		}
		p[n] = c
		n++
	}

	r.objectDepth = objectDepth
	r.inString = inString
	r.inStringEscape = inStringEscape

	if len(r.buffer) == 0 {
		// release the buffer (maybe a sync pool would be faster)
		r.buffer = nil
	}
	// klog.Infof("returning %v", string(p[:n]))

	return n, nil
}
