package panel

import (
	"context"
	"fmt"
	"strings"

	"encoding/json/jsontext"
	"encoding/json/v2"

	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const UserNotModified = "UserNotModified"

type OnlineUser struct {
	UID int
	IP  string
}

type UserInfo struct {
	Id          int    `json:"id" msgpack:"id"`
	Uuid        string `json:"uuid" msgpack:"uuid"`
	SpeedLimit  int    `json:"speed_limit" msgpack:"speed_limit"`
	DeviceLimit int    `json:"device_limit" msgpack:"device_limit"`
}

type UserListBody struct {
	Users []UserInfo `json:"users" msgpack:"users"`
}

type AliveMap struct {
	Alive map[int]int `json:"alive"`
}

// GetUserList will pull user from v2board.
// Returns (users, newEtag, error) and always fetches the full list.
//
// We deliberately do NOT send If-None-Match here. When a panel sits
// behind a CDN (e.g. Cloudflare), the CDN caches the response together
// with its ETag and serves stale 304s from the edge without consulting
// the origin — even after the user data changed (new purchases, etc.).
// That makes the node believe "no users changed" and skip sync for
// hours, while different edge cache policies cause some nodes to sync
// and others to stay stuck. The user list is only a few KB, so pulling
// it fresh every cycle is negligible and guarantees correctness.
func (c *Client) GetUserList(ctx context.Context) ([]UserInfo, string, error) {
	const path = "/api/v1/server/UniProxy/user"
	r, err := c.client.R().
		SetContext(ctx).
		SetHeader("X-Response-Format", "msgpack").
		SetDoNotParseResponse(true).
		Get(path)
	if err != nil {
		return nil, "", err
	}
	if r == nil || r.RawResponse == nil {
		return nil, "", fmt.Errorf("received nil response or raw response")
	}
	defer r.RawResponse.Body.Close()

	newEtag := r.Header().Get("ETag")
	userlist := &UserListBody{}
	if strings.Contains(r.Header().Get("Content-Type"), "application/x-msgpack") {
		decoder := msgpack.NewDecoder(r.RawResponse.Body)
		if err := decoder.Decode(userlist); err != nil {
			return nil, "", fmt.Errorf("decode user list error: %w", err)
		}
	} else {
		dec := jsontext.NewDecoder(r.RawResponse.Body)
		for {
			tok, err := dec.ReadToken()
			if err != nil {
				return nil, "", fmt.Errorf("decode user list error: %w", err)
			}
			if tok.Kind() == '"' && tok.String() == "users" {
				break
			}
		}
		tok, err := dec.ReadToken()
		if err != nil {
			return nil, "", fmt.Errorf("decode user list error: %w", err)
		}
		if tok.Kind() != '[' {
			return nil, "", fmt.Errorf(`decode user list error: expected "users" array`)
		}
		for dec.PeekKind() != ']' {
			val, err := dec.ReadValue()
			if err != nil {
				return nil, "", fmt.Errorf("decode user list error: read user object: %w", err)
			}
			var u UserInfo
			if err := json.Unmarshal(val, &u); err != nil {
				return nil, "", fmt.Errorf("decode user list error: unmarshal user error: %w", err)
			}
			userlist.Users = append(userlist.Users, u)
		}
	}
	return userlist.Users, newEtag, nil
}

// CommitUserEtag saves the ETag. Call this ONLY after the user list
// has been successfully applied to xray core and c.userList updated.
func (c *Client) CommitUserEtag(etag string) {
	if etag != "" {
		c.userEtag = etag
	}
}

// GetUserAlive will fetch the alive_ip count for users
func (c *Client) GetUserAlive(ctx context.Context) (map[int]int, error) {
	c.AliveMap = &AliveMap{}
	const path = "/api/v1/server/UniProxy/alivelist"
	r, err := c.client.R().
		SetContext(ctx).
		ForceContentType("application/json").
		Get(path)
	if err != nil {
		c.AliveMap.Alive = make(map[int]int)
		return c.AliveMap.Alive, nil
	}
	if r == nil || r.RawResponse == nil || r.StatusCode() >= 399 {
		c.AliveMap.Alive = make(map[int]int)
		return c.AliveMap.Alive, nil
	}
	defer r.RawResponse.Body.Close()
	if err := json.Unmarshal(r.Body(), c.AliveMap); err != nil {
		log.Errorf("unmarshal user alive list error: %s", err)
		c.AliveMap.Alive = make(map[int]int)
	}

	return c.AliveMap.Alive, nil
}

type UserTraffic struct {
	UID      int
	Upload   int64
	Download int64
}

// ReportUserTraffic reports the user traffic
func (c *Client) ReportUserTraffic(ctx context.Context, userTraffic []UserTraffic) error {
	data := make(map[int][]int64, len(userTraffic))
	for i := range userTraffic {
		data[userTraffic[i].UID] = []int64{userTraffic[i].Upload, userTraffic[i].Download}
	}
	const path = "/api/v1/server/UniProxy/push"
	resp, err := c.client.R().
		SetContext(ctx).
		SetBody(data).
		ForceContentType("application/json").
		Post(path)
	if err != nil {
		return err
	}
	if resp != nil && resp.IsError() {
		return fmt.Errorf("server returned error: %d %s", resp.StatusCode(), resp.String())
	}
	return nil
}

func (c *Client) ReportNodeOnlineUsers(ctx context.Context, data *map[int][]string) error {
	const path = "/api/v1/server/UniProxy/alive"
	resp, err := c.client.R().
		SetContext(ctx).
		SetBody(data).
		ForceContentType("application/json").
		Post(path)

	if err != nil {
		return err
	}
	if resp != nil && resp.IsError() {
		return fmt.Errorf("server returned error: %d %s", resp.StatusCode(), resp.String())
	}

	return nil
}
