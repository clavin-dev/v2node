package panel

import (
	"errors"
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
	// Use Go's default HTTP transport (HTTP/2 + keep-alive) — the same behavior
	// as upstream v2node and curl, both of which talk to the (CDN-fronted)
	// panels reliably (~1s). The previous forced-HTTP/1.1 + DisableKeepAlives
	// transport made requests to some CDN-fronted panels hang for minutes and
	// leak one connection per hang (observed: 1200+ leaked conns, FD climbing).
	// Each panel call is now bounded by a per-request timeout (see the api
	// methods) and backstopped by the task watchdog, so stalls fail fast and
	// retry instead of hanging — without disabling keep-alive.
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
