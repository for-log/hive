package hrana

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultMaxResponseBytes = 32 * 1024 * 1024 // 32 MiB

// Client sends Hrana HTTP requests to a single libSQL master.
// Construct with NewClient; do not copy after first use.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	maxBytes   int64
}

type ClientConfig struct {
	BaseURL      string
	Token        string
	Timeout      time.Duration
	MaxBodyBytes int64
}

func NewClient(cfg ClientConfig) *Client {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	maxBytes := cfg.MaxBodyBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxResponseBytes
	}
	return &Client{
		baseURL: cfg.BaseURL,
		token:   cfg.Token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		maxBytes: maxBytes,
	}
}

// Pipeline executes a pipeline request against /v2/pipeline.
func (c *Client) Pipeline(ctx context.Context, req *PipelineRequest) (*PipelineResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("hrana client: marshal pipeline request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v2/pipeline", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("hrana client: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	c.setAuth(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("hrana client: pipeline: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if err := checkStatus(resp); err != nil {
		return nil, err
	}

	var out PipelineResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, c.maxBytes)).Decode(&out); err != nil {
		return nil, fmt.Errorf("hrana client: decode pipeline response: %w", err)
	}
	return &out, nil
}

// Cursor opens a /v3/cursor streaming request against the master.
// The caller receives the raw HTTP response body (chunked, newline-delimited JSON)
// and is responsible for closing it. The first line is the CursorResponseHeader.
// Returns an error only if the HTTP request itself fails or the server returns non-200.
func (c *Client) Cursor(ctx context.Context, req *CursorRequest) (*http.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("hrana client: marshal cursor request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v3/cursor", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("hrana client: build cursor request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	c.setAuth(httpReq)

	// Use a client without timeout so the stream can stay open as long as needed.
	noTimeoutClient := &http.Client{Transport: c.httpClient.Transport}
	resp, err := noTimeoutClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("hrana client: cursor: %w", err)
	}
	if err := checkStatus(resp); err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// Describe sends a single "describe" request and returns the DescribeResult.
// Uses a fresh (baton-less) pipeline so it is safe to call at any time.
func (c *Client) Describe(ctx context.Context, sql string) (*DescribeResult, error) {
	req := &PipelineRequest{
		Requests: []StreamRequest{
			DescribeRequest(sql),
			CloseRequest(),
		},
	}
	resp, err := c.Pipeline(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(resp.Results) == 0 {
		return nil, fmt.Errorf("hrana client: describe: empty results")
	}
	res := resp.Results[0]
	if res.Type == "error" && res.Error != nil {
		return nil, fmt.Errorf("hrana client: describe: %s", res.Error.Message)
	}
	if res.Response == nil {
		return nil, fmt.Errorf("hrana client: describe: nil response")
	}
	return res.Response.DescribeResultValue()
}

// Dump fetches the full SQL dump from /dump (SQLite-compatible format).
func (c *Client) Dump(ctx context.Context) (string, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/dump", nil)
	if err != nil {
		return "", fmt.Errorf("hrana client: build dump request: %w", err)
	}
	c.setAuth(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("hrana client: dump: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if err := checkStatus(resp); err != nil {
		return "", err
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBytes))
	if err != nil {
		return "", fmt.Errorf("hrana client: read dump body: %w", err)
	}
	return string(data), nil
}

// Health checks /health and returns nil when the master is healthy.
func (c *Client) Health(ctx context.Context) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return fmt.Errorf("hrana client: build health request: %w", err)
	}
	c.setAuth(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("hrana client: health: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	return checkStatus(resp)
}

// Version returns the server version string from /version.
func (c *Client) Version(ctx context.Context) (string, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/version", nil)
	if err != nil {
		return "", fmt.Errorf("hrana client: build version request: %w", err)
	}
	c.setAuth(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("hrana client: version: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if err := checkStatus(resp); err != nil {
		return "", err
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("hrana client: read version body: %w", err)
	}
	return string(data), nil
}

func (c *Client) setAuth(r *http.Request) {
	if c.token != "" {
		r.Header.Set("Authorization", "Bearer "+c.token)
	}
}

// checkStatus returns an error for non-2xx responses, attempting to parse
// the body as a Hrana Error structure first.
func checkStatus(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	// Best-effort parse of Hrana error body.
	var hranaErr Error
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&hranaErr); err == nil && hranaErr.Message != "" {
		return fmt.Errorf("hrana client: HTTP %d: %s", resp.StatusCode, hranaErr.Message)
	}
	return fmt.Errorf("hrana client: HTTP %d", resp.StatusCode)
}
