package panel

import (
	"crypto/tls"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/go-resty/resty/v2"
	"github.com/wyx2685/v2node/conf"
)

// Panel is the interface for different panel's api.

type Client struct {
	client   *resty.Client
	APIHost  string
	Token    string
	NodeId   int
	nodeEtag string
	userEtag string
	UserList *UserListBody
	AliveMap *AliveMap
}

func New(c *conf.NodeConfig) (*Client, error) {
	client := resty.New()
	// CRITICAL: control-plane connection hardening. Without this, the panel
	// goes "offline" after a few hours of uptime even though Xray is fine.
	//
	// 1. Fully disable HTTP/2. Behind Cloudflare, Go's TLS ALPN negotiates h2
	//    and multiplexes every request onto a single TCP connection. When that
	//    connection rots, ALL requests (heartbeat included) silently hang until
	//    timeout, so the panel stops seeing heartbeats and marks the node
	//    offline. ForceAttemptHTTP2=false alone is NOT enough — an empty,
	//    non-nil TLSNextProto map is the Go-official way to truly disable h2.
	// 2. Disable keep-alive: use a fresh TCP+TLS connection per call. v2node
	//    only hits the panel ~every 60s, so the ~100ms handshake cost is
	//    negligible, but reusing a connection that Cloudflare/Nginx silently
	//    RST'd (after 1-3h) makes every later request time out. Fresh
	//    connections = zero connection-rot risk.
	client.SetTransport(&http.Transport{
		ForceAttemptHTTP2:     false,
		TLSNextProto:          make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		DisableKeepAlives:     true,
	})
	client.SetRetryCount(3)
	if c.Timeout > 0 {
		client.SetTimeout(time.Duration(c.Timeout) * time.Second)
	} else {
		client.SetTimeout(5 * time.Second)
	}
	client.OnError(func(req *resty.Request, err error) {
		var v *resty.ResponseError
		if errors.As(err, &v) {
			logrus.Error(v.Err)
		}
	})
	client.SetBaseURL(c.APIHost)
	// set params
	client.SetQueryParams(map[string]string{
		"node_type": "v2node",
		"node_id":   strconv.Itoa(c.NodeID),
		"token":     c.Key,
	})
	return &Client{
		client:   client,
		Token:    c.Key,
		APIHost:  c.APIHost,
		NodeId:   c.NodeID,
		UserList: &UserListBody{},
		AliveMap: &AliveMap{},
	}, nil
}
