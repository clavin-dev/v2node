package node

import (
	"fmt"
	"net"
	"reflect"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/task"
	vCore "github.com/wyx2685/v2node/core"
)

func (c *Controller) startTasks(node *panel.NodeInfo) {
	// fetch node info task
	c.nodeInfoMonitorPeriodic = &task.Task{
		Name:     "nodeInfoMonitor[" + c.tag + "]",
		Interval: node.PullInterval,
		Execute:  c.nodeInfoMonitor,
	}
	// fetch user list task
	c.userReportPeriodic = &task.Task{
		Name:     "reportUserTrafficTask[" + c.tag + "]",
		Interval: node.PushInterval,
		Execute:  c.reportUserTrafficTask,
	}
	log.WithField("tag", c.tag).Info("Start monitor node status")
	_ = c.nodeInfoMonitorPeriodic.Start(true)
	log.WithField("tag", c.tag).Info("Start report node status")
	_ = c.userReportPeriodic.Start(true)
	if node.Security == panel.Tls {
		switch c.info.Common.CertInfo.CertMode {
		case "none", "", "file", "self":
		default:
			c.renewCertPeriodic = &task.Task{
				Name:     "renewCertTask",
				Interval: time.Hour * 24,
				Execute:  c.renewCertTask,
			}
			log.WithField("tag", c.tag).Info("Start renew cert")
			// delay to start renewCert
			_ = c.renewCertPeriodic.Start(true)
		}
	}
}

// nodeInfoMonitor follows XrayR's pattern exactly:
// 1. Fetch node info (304 = not modified)
// 2. If modified, compare parsed struct with reflect.DeepEqual
// 3. Only do DelNode+AddNode if struct actually changed
func (c *Controller) nodeInfoMonitor() (err error) {
	// Fetch node info
	var nodeInfoChanged = true
	newN, err := c.apiClient.GetNodeInfo()
	if err != nil {
		if err.Error() == panel.NodeNotModified {
			nodeInfoChanged = false
			newN = c.info
		} else {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Get node info failed")
			return nil
		}
	}

	// If node info changed, check if it REALLY changed via DeepEqual
	if nodeInfoChanged {
		if !reflect.DeepEqual(c.info, newN) {
			log.WithField("tag", c.tag).Info("Node info changed, updating in-place")
			// Remove old inbound
			if err = c.server.DelNode(c.tag); err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Error("Failed to remove old inbound")
				return nil
			}
			// Wait for port to be released
			time.Sleep(time.Second)
			// Add new inbound (do NOT update c.info yet)
			if err = c.server.AddNode(c.tag, newN); err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Error("Failed to add new inbound, will retry next cycle")
				// c.info stays old → next DeepEqual detects diff → auto retry
				return nil
			}
			// Re-add all current users to the new inbound
			if len(c.userList) > 0 {
				_, err = c.server.AddUsers(&vCore.AddUsersParams{
					Tag:      c.tag,
					NodeInfo: newN,
					Users:    c.userList,
				})
				if err != nil {
					log.WithFields(log.Fields{
						"tag": c.tag,
						"err": err,
					}).Error("Failed to re-add users after inbound update")
					return nil
				}
			}
			// Only update c.info AFTER everything succeeds
			c.info = newN
			log.WithField("tag", c.tag).Info("Node inbound updated")
		} else {
			nodeInfoChanged = false
		}
	}

	// Update users
	var usersChanged = true
	newU, newEtag, err := c.apiClient.GetUserList()
	if err != nil {
		if err.Error() == panel.UserNotModified {
			usersChanged = false
			newU = c.userList
		} else {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Get user list failed")
			return nil
		}
	}

	// get user alive — if it fails, we still proceed with user sync.
	newA, err := c.apiClient.GetUserAlive()
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Warn("Get alive list failed, proceeding with user sync")
	}

	// update alive list
	if newA != nil {
		c.limiter.UpdateAliveList(newA)
	}

	if nodeInfoChanged {
		// Node changed — users were already re-added above, just sync userList
		c.userList = newU
		c.apiClient.CommitUserEtag(newEtag)
	} else if usersChanged {
		deleted, added, modified := compareUserList(c.userList, newU)
		if len(deleted) > 0 {
			err = c.server.DelUsers(deleted, c.tag, c.info)
			if err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Error("Delete users failed")
				return nil
			}
		}
		if len(added) > 0 {
			_, err = c.server.AddUsers(&vCore.AddUsersParams{
				Tag:      c.tag,
				NodeInfo: c.info,
				Users:    added,
			})
			if err != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": err,
				}).Error("Add users failed")
				return nil
			}
		}
		if len(added) > 0 || len(deleted) > 0 || len(modified) > 0 {
			c.limiter.UpdateUser(c.tag, added, deleted, modified)
		}
		c.userList = newU
		c.apiClient.CommitUserEtag(newEtag)
		log.WithField("tag", c.tag).Infof("%d user deleted, %d user added, %d user modified", len(deleted), len(added), len(modified))
	}

	// Port health check: verify our listening port is still alive.
	// If the port dropped (OS reclaim, xray-core crash, etc.), rebuild it.
	if c.info != nil && c.info.Common != nil && c.info.Common.ServerPort > 0 {
		addr := fmt.Sprintf("127.0.0.1:%d", c.info.Common.ServerPort)
		conn, dialErr := net.DialTimeout("tcp", addr, 2*time.Second)
		if dialErr != nil {
			log.WithFields(log.Fields{
				"tag":  c.tag,
				"port": c.info.Common.ServerPort,
			}).Warn("Port health check failed, rebuilding inbound")
			_ = c.server.DelNode(c.tag)
			time.Sleep(time.Second)
			if rebuildErr := c.server.AddNode(c.tag, c.info); rebuildErr != nil {
				log.WithFields(log.Fields{
					"tag": c.tag,
					"err": rebuildErr,
				}).Error("Port rebuild failed, will retry next cycle")
			} else {
				// Re-add users after rebuild
				if len(c.userList) > 0 {
					_, _ = c.server.AddUsers(&vCore.AddUsersParams{
						Tag:      c.tag,
						NodeInfo: c.info,
						Users:    c.userList,
					})
				}
				log.WithFields(log.Fields{
					"tag":  c.tag,
					"port": c.info.Common.ServerPort,
				}).Info("Port rebuilt successfully")
			}
		} else {
			conn.Close()
		}
	}

	return nil
}
