package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/gateway"
	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carbs "github.com/ipld/go-car/v2/blockstore"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func startGateway(ctx context.Context, gatewayAddress string, epoch int, epochCid cid.Cid) error {
	carUrl := fmt.Sprintf("https://files.old-faithful.net/%d/epoch-%d.car", epoch, epoch)
	indexUrl := fmt.Sprintf("https://files.old-faithful.net/%d/epoch-%d-%s-mainnet-cid-to-offset-and-size.index", epoch, epoch, epochCid)
	bsrv, err := newBlockServiceFromCAR(carUrl, indexUrl)
	if err != nil {
		return err
	}

	backend, err := gateway.NewBlocksBackend(bsrv)
	if err != nil {
		return err
	}

	h := NewHandler(backend)

	log.Printf("Listening on %s", gatewayAddress)
	log.Printf("Metrics available at http://%s/debug/metrics/prometheus", gatewayAddress)

	ln, err := net.Listen("tcp", gatewayAddress)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	if err := http.Serve(ln, h); err != nil {
		return err
	}

	return nil
}

func newBlockServiceFromCAR(carUrl, indexUrl string) (blockservice.BlockService, error) {
	carReader := NewHTTPReaderAt(carUrl)
	idx, err := NewYellowstoneIndex(indexUrl)
	if err != nil {
		return nil, err
	}

	bs, err := carbs.NewReadOnly(carReader, idx)
	if err != nil {
		return nil, err
	}

	roots, err := bs.Roots()
	if err != nil {
		return nil, err
	}

	for _, root := range roots {
		fmt.Println(root)
	}

	blockService := blockservice.New(bs, offline.Exchange(bs))
	return blockService, nil
}

type ReadOnlyBlockstoreValidator struct {
	blockstore.Blockstore
}

func (b *ReadOnlyBlockstoreValidator) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	blk, err := b.Blockstore.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	calculatedCid, err := c.Prefix().Sum(blk.RawData())
	if err != nil {
		return nil, err
	}

	if !calculatedCid.Equals(c) {
		return nil, blockstore.ErrHashMismatch
	}

	return blocks.NewBlockWithCid(blk.RawData(), c)
}

func (b *ReadOnlyBlockstoreValidator) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	return b.Blockstore.GetSize(ctx, c)
}

func (b *ReadOnlyBlockstoreValidator) HashOnRead(enabled bool) {}

var _ blockstore.Blockstore = (*ReadOnlyBlockstoreValidator)(nil)

// HTTPReaderAt implements io.ReaderAt using HTTP Range requests
type HTTPReaderAt struct {
	url string
}

// NewHTTPReaderAt creates a new HTTPReaderAt for the given URL
func NewHTTPReaderAt(url string) *HTTPReaderAt {
	return &HTTPReaderAt{url: url}
}

// ReadAt sends a Range request and reads data into p starting at offset off
func (h *HTTPReaderAt) ReadAt(p []byte, off int64) (int, error) {
	client := &http.Client{}

	// Build HTTP request with Range header
	req, err := http.NewRequest("GET", h.url, nil)
	if err != nil {
		return 0, err
	}

	// Set Range header to specify the byte range
	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)
	req.Header.Set("Range", rangeHeader)

	// Perform the HTTP request
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("server does not support range requests: status %s", resp.Status)
	}

	// Read the response body into the buffer
	return io.ReadFull(resp.Body, p)
}

func (h *HTTPReaderAt) Close() error {
	return nil
}

func NewHandler(gwAPI gateway.IPFSBackend) http.Handler {
	conf := gateway.Config{
		// If you set DNSLink to point at the CID from CAR, you can load it!
		NoDNSLink: false,

		// For these examples we have the trusted mode enabled by default. That is,
		// all types of requests will be accepted. By default, only Trustless Gateway
		// requests work: https://specs.ipfs.tech/http-gateways/trustless-gateway/
		DeserializedResponses: true,

		// Initialize the public gateways that we will want to have available
		// through Host header rewriting. This step is optional, but required
		// if you're running multiple public gateways on different hostnames
		// and want different settings such as support for Deserialized
		// Responses on localhost, or DNSLink and Subdomain Gateways.
		PublicGateways: map[string]*gateway.PublicGateway{
			// Support local requests
			"localhost": {
				Paths:         []string{"/ipfs", "/ipns"},
				NoDNSLink:     false,
				UseSubdomains: true,
				// Localhost is considered trusted, ok to allow deserialized responses
				// as long it is not exposed to the internet.
				DeserializedResponses: true,
			},
		},

		// Add an example menu item called 'Boxo', linking to our library.
		Menu: []assets.MenuItem{
			{
				URL:   "https://github.com/ipfs/boxo",
				Title: "Boxo",
			},
		},
	}

	// Creates a mux to serve the gateway paths. This is not strictly necessary
	// and gwHandler could be used directly. However, on the next step we also want
	// to add prometheus metrics, hence needing the mux.
	gwHandler := gateway.NewHandler(conf, gwAPI)
	mux := http.NewServeMux()
	mux.Handle("/ipfs/", gwHandler)
	mux.Handle("/ipns/", gwHandler)

	// Serves prometheus metrics alongside the gateway. This step is optional and
	// only required if you need or want to access the metrics. You may also decide
	// to expose the metrics on a different path, or port.
	mux.Handle("/debug/metrics/prometheus", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))

	// Then wrap the mux with the hostname handler. Please note that the metrics
	// will not be available under the previously defined publicGateways.
	// You will be able to access the metrics via 127.0.0.1 but not localhost
	// or example.net. If you want to expose the metrics on such gateways,
	// you will have to add the path "/debug" to the variable Paths.
	var handler http.Handler
	handler = gateway.NewHostnameHandler(conf, gwAPI, mux)

	// Then, wrap with the withConnect middleware. This is required since we use
	// http.ServeMux which does not support CONNECT by default.
	handler = withConnect(handler)

	// Add headers middleware that applies any headers we define to all requests
	// as well as a default CORS configuration.
	handler = gateway.NewHeaders(nil).ApplyCors().Wrap(handler)

	// Finally, wrap with the otelhttp handler. This will allow the tracing system
	// to work and for correct propagation of tracing headers. This step is optional
	// and only required if you want to use tracing. Note that OTel must be correctly
	// setup in order for this to have an effect.
	handler = otelhttp.NewHandler(handler, "Gateway.Request")

	return handler
}

// withConnect provides a middleware that adds support to the HTTP CONNECT method.
// This is required if the implementer is using http.ServeMux, or some other structure
// that does not support the CONNECT method. It should be applied to the top-level handler.
// https://golang.org/src/net/http/request.go#L111
func withConnect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
