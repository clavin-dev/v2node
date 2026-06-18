package node

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/conf"
	"github.com/wyx2685/v2node/core"
)

type Node struct {
	controllers []*Controller
	NodeInfos   []*panel.NodeInfo
}

// New fetches node info from each panel independently. If one panel
// is unreachable, that node is skipped and the rest continue normally.
// Only returns an error if ALL nodes fail (nothing to start).
func New(nodes []conf.NodeConfig) (*Node, error) {
	n := &Node{}
	for i := range nodes {
		p, err := panel.New(&nodes[i])
		if err != nil {
			log.WithFields(log.Fields{
				"host": nodes[i].APIHost,
				"id":   nodes[i].NodeID,
				"err":  err,
			}).Error("Create panel client failed, skipping this node")
			continue
		}
		info, err := p.GetNodeInfo()
		if err != nil {
			log.WithFields(log.Fields{
				"host": nodes[i].APIHost,
				"id":   nodes[i].NodeID,
				"err":  err,
			}).Error("Get node info failed, skipping this node")
			continue
		}
		n.controllers = append(n.controllers, NewController(p, &nodes[i], info))
		n.NodeInfos = append(n.NodeInfos, info)
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
			// Don't return — continue closing the rest
		}
	}
	n.controllers = nil
	return nil
}
