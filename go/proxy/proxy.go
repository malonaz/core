package proxy

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Opts struct {
	URL                     string `long:"url" required:"true" description:"Proxy URL"`
	Username                string `long:"username" required:"true" description:"Proxy username"`
	Password                string `long:"password" required:"true" description:"Proxy password"`
	RotateIPIntervalSeconds int64  `long:"rotate-ip-interval-seconds" default:"0" description:"How often should we switch the IP"`
}

// CreateHTTPClient initializes a new http.Client with proxy settings.
func NewHTTPClient(opts *Opts) (*http.Client, error) {
	proxyURL, err := url.Parse(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("Invalid proxy URL: %w", err)
	}
	// Add your proxy authentication details.
	auth := url.UserPassword(opts.Username, opts.Password)
	proxyURL.User = auth

	// Define proxy function.
	proxy := func(*http.Request) (*url.URL, error) {
		if opts.RotateIPIntervalSeconds == 0 {
			return proxyURL, nil
		}
		suffix := time.Now().Unix() / opts.RotateIPIntervalSeconds
		username := opts.Username + "-session-" + strconv.FormatInt(suffix, 10)
		proxyURL, err := url.Parse(opts.URL)
		if err != nil {
			return nil, fmt.Errorf("Invalid proxy URL: %w", err)
		}
		auth := url.UserPassword(username, opts.Password)
		proxyURL.User = auth
		return proxyURL, nil
	}

	// Set up the HTTP transport with the proxy.
	transport := &http.Transport{
		Proxy: proxy,
	}

	// Return the new http.Client.
	return &http.Client{
		Transport: transport,
	}, nil
}
