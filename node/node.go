package node

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/conf"
	"github.com/wyx2685/v2node/core"
)

type Node struct {
	controllers []*Controller
	NodeInfos   []*panel.NodeInfo
}

// New fetches node info from each panel concurrently.
// If one panel is unreachable, that node is skipped.
// Only returns an error if ALL nodes fail.
func New(nodes []conf.NodeConfig) (*Node, error) {
	type result struct {
		controller *Controller
		info       *panel.NodeInfo
	}

	results := make(chan result, len(nodes))
	var wg sync.WaitGroup

	for i := range nodes {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p, err := panel.New(&nodes[idx])
			if err != nil {
				log.WithFields(log.Fields{
					"host": nodes[idx].APIHost,
					"id":   nodes[idx].NodeID,
					"err":  err,
				}).Error("Create panel client failed, skipping this node")
				return
			}
			info, initEtag, err := p.GetNodeInfo()
			if err != nil {
				log.WithFields(log.Fields{
					"host": nodes[idx].APIHost,
					"id":   nodes[idx].NodeID,
					"err":  err,
				}).Error("Get node info failed, skipping this node")
				return
			}
			p.CommitNodeEtag(initEtag)
			results <- result{
				controller: NewController(p, &nodes[idx], info),
				info:       info,
			}
		}(i)
	}

	wg.Wait()
	close(results)

	n := &Node{}
	for r := range results {
		n.controllers = append(n.controllers, r.controller)
		n.NodeInfos = append(n.NodeInfos, r.info)
	}

	if len(n.controllers) == 0 {
		return nil, fmt.Errorf("all %d nodes failed to initialize", len(nodes))
	}
	if len(n.controllers) < len(nodes) {
		log.Warnf("%d/%d nodes initialized successfully, %d skipped",
			len(n.controllers), len(nodes), len(nodes)-len(n.controllers))
	}
	return n, nil
}

// Start starts each node controller independently. If one fails,
// the rest still start normally.
func (n *Node) Start(core *core.V2Core) error {
	var started int
	for i := range n.controllers {
		err := n.controllers[i].Start(core)
		if err != nil {
			log.WithFields(log.Fields{
				"tag": n.controllers[i].tag,
				"err": err,
			}).Error("Start node controller failed, skipping")
			continue
		}
		started++
	}
	if started == 0 {
		return fmt.Errorf("all %d node controllers failed to start", len(n.controllers))
	}
	return nil
}

func (n *Node) Close() error {
	for _, c := range n.controllers {
		if err := c.Close(); err != nil {
			log.WithField("err", err).Error("Close controller failed")
		}
	}
	n.controllers = nil
	return nil
}
