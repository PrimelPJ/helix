// Package client provides a Go client for a Helix cluster.
//
// The client discovers the leader automatically by following
// X-Raft-Leader redirect headers, so callers do not need to track
// which node is currently the leader.
//
// Usage:
//
//	c := client.New("http://localhost:8001", "http://localhost:8002", "http://localhost:8003")
//	err := c.Set(ctx, "greeting", "salam alaykum")
//	val, err := c.Get(ctx, "greeting")
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrNotFound is returned by Get when the key does not exist in the cluster.
var ErrNotFound = errors.New("client: key not found")

// ErrNoLeader is returned when no node in the seed list reports a leader.
var ErrNoLeader = errors.New("client: no leader available")

// Client is a Helix cluster client. It is safe for concurrent use.
type Client struct {
	seeds   []string
	http    *http.Client
	maxHops int
}

// New returns a Client seeded with one or more node addresses.
func New(seeds ...string) *Client {
	return &Client{
		seeds: seeds,
		http: &http.Client{
			Timeout: 10 * time.Second,
			// Do not follow redirects automatically; we handle them manually
			// so we can update our leader hint.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxHops: 5,
	}
}

// Get retrieves the value for key from the cluster.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	for _, addr := range c.seeds {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("%s/kv/%s", addr, key), nil)
		resp, err := c.http.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return "", ErrNotFound
		}
		if resp.StatusCode == http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return string(b), nil
		}
	}
	return "", ErrNoLeader
}

// Set writes key=value to the cluster, following leader redirects.
func (c *Client) Set(ctx context.Context, key, value string) error {
	return c.write(ctx, http.MethodPut, key, value)
}

// Delete removes key from the cluster.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.write(ctx, http.MethodDelete, key, "")
}

func (c *Client) write(ctx context.Context, method, key, value string) error {
	addrs := make([]string, len(c.seeds))
	copy(addrs, c.seeds)

	for hop := 0; hop < c.maxHops; hop++ {
		for _, addr := range addrs {
			var body io.Reader
			if value != "" {
				body = strings.NewReader(value)
			}
			req, _ := http.NewRequestWithContext(ctx, method,
				fmt.Sprintf("%s/kv/%s", addr, key), body)
			resp, err := c.http.Do(req)
			if err != nil {
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				return nil
			}
			if resp.StatusCode == http.StatusTemporaryRedirect {
				if leader := resp.Header.Get("X-Raft-Leader"); leader != "" {
					// Move leader address to front for next hop
					addrs = append([]string{"http://" + leader}, addrs...)
				}
				break
			}
		}
	}
	return ErrNoLeader
}
