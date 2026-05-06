package grpcproxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"

	"golang.org/x/net/http2"
)

const maxForwardResponseDrainBytes = 4 << 20

type Forwarder struct {
	clients []forwarderTarget
}

type forwarderTarget struct {
	url    string
	client *http.Client
}

func NewForwarder(masterURLs []string) *Forwarder {
	targets := make([]forwarderTarget, len(masterURLs))
	for i, u := range masterURLs {
		transport := &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		}
		targets[i] = forwarderTarget{
			url:    u,
			client: &http.Client{Transport: transport},
		}
	}
	return &Forwarder{clients: targets}
}

func (f *Forwarder) ForwardGRPC(ctx context.Context, masterIdx int, path string, body []byte) (err error) {
	if masterIdx < 0 || masterIdx >= len(f.clients) {
		return fmt.Errorf("master index %d out of range", masterIdx)
	}
	t := f.clients[masterIdx]

	reqURL := t.url + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("forward gRPC to %s: %w", t.url, err)
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = fmt.Errorf("close response body from %s: %w", t.url, cerr)
		}
	}()

	n, copyErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxForwardResponseDrainBytes+1))
	if copyErr != nil {
		return fmt.Errorf("drain response from %s: %w", t.url, copyErr)
	}
	if n > maxForwardResponseDrainBytes {
		return fmt.Errorf("response body from %s exceeds %d bytes", t.url, maxForwardResponseDrainBytes)
	}

	if grpcStatus := resp.Trailer.Get("grpc-status"); grpcStatus != "" && grpcStatus != "0" {
		grpcMsg := resp.Trailer.Get("grpc-message")
		return fmt.Errorf("gRPC error from %s: status=%s msg=%s", t.url, grpcStatus, grpcMsg)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, t.url)
	}
	return nil
}
