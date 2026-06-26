package node

import (
	"context"
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/common/task"
	vCore "github.com/wyx2685/v2node/core"
)

// userReconcileEvery is how many nodeInfoMonitor cycles pass between full
// drift reconciles against actual core state. At the default 60s pull
// interval this is ~5 minutes. Tune higher to reduce overhead, lower to
// repair drift faster.
const userReconcileEvery = 5

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

// nodeNeedsRebuild checks only the fields that actually require a port
// rebuild (DelNode + AddNode). Ignores json.RawMessage byte differences
// and BaseConfig interval changes that don't affect the inbound listener.
func nodeNeedsRebuild(old, new *panel.NodeInfo) bool {
	if old == nil || new == nil || old.Common == nil || new.Common == nil {
		return true
	}
	o := old.Common
	n := new.Common
	// Core listener fields
	if o.ServerPort != n.ServerPort ||
		o.Protocol != n.Protocol ||
		o.ListenIP != n.ListenIP ||
		o.Network != n.Network ||
		o.Tls != n.Tls ||
		o.Flow != n.Flow ||
		o.Cipher != n.Cipher ||
		o.ServerKey != n.ServerKey ||
		o.ServerName != n.ServerName ||
		o.CongestionControl != n.CongestionControl ||
		o.Encryption != n.Encryption {
		return true
	}
	// TLS settings
	if o.TlsSettings.ServerName != n.TlsSettings.ServerName ||
		o.TlsSettings.PrivateKey != n.TlsSettings.PrivateKey ||
		o.TlsSettings.Dest != n.TlsSettings.Dest ||
		o.TlsSettings.CertMode != n.TlsSettings.CertMode {
		return true
	}
	// Security type change
	if old.Security != new.Security {
		return true
	}
	return false
}

// nodeInfoMonitor:
// 1. Fetch node info (always 200, no ETag/304)
// 2. Compare critical fields with nodeNeedsRebuild
// 3. Only do DelNode+AddNode if listener config actually changed
func (c *Controller) nodeInfoMonitor(ctx context.Context) (err error) {
	// Fetch node info — always returns fresh data (no ETag)
	newN, err := c.apiClient.GetNodeInfo(ctx)
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get node info failed")
		return nil
	}

	// Check if the config REALLY needs a port rebuild
	if nodeNeedsRebuild(c.info, newN) {
		log.WithField("tag", c.tag).Info("Node config changed, rebuilding inbound")
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
			// c.info stays old → next cycle retries automatically
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
		// No rebuild needed — just update non-critical fields (intervals, etc.)
		c.info = newN
	}

	// Update users
	var usersChanged = true
	newU, newEtag, err := c.apiClient.GetUserList(ctx)
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
	newA, err := c.apiClient.GetUserAlive(ctx)
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

	// Always reconcile users against the panel as an incremental diff —
	// whether or not the inbound was rebuilt this cycle. After a rebuild the
	// new inbound was seeded with the OLD c.userList, so the diff
	// (old c.userList -> newU) is exactly the delta needed to converge. This
	// also captures users that were added in the SAME cycle as a config
	// change, which the old nodeInfoChanged special-case dropped — causing
	// those users to sit in c.userList but never get added to xray (permanent
	// new-user desync). Only the changed users are touched; existing users'
	// connections are never disturbed (no full reload, no downtime).
	if usersChanged {
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

	// Periodic drift safety net: every userReconcileEvery cycles, compare the
	// panel's desired users against what core ACTUALLY has and repair only the
	// difference. The fast-path diff above trusts c.userList (the in-memory
	// mirror); this catches any case where that mirror drifted from real core
	// state (e.g. a user that ended up in c.userList but was never registered
	// in xray). Only the differing users are touched — existing connections are
	// never disturbed, so there is no downtime.
	c.reconcileCounter++
	if c.reconcileCounter >= userReconcileEvery {
		c.reconcileCounter = 0
		c.reconcileUsers(c.userList)
	}

	// Port health check: verify our listening port is still alive.
	// Only for TCP-based protocols (skip hysteria2/tuic which use UDP).
	if c.info != nil && c.info.Common != nil && c.info.Common.ServerPort > 0 {
		switch c.info.Type {
		case "hysteria2", "tuic":
			// UDP protocols — cannot TCP-dial, skip health check
		default:
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
	}

	return nil
}

// reconcileUsers is the periodic drift safety net (XrayR-style targeted
// repair): it diffs the panel's desired user set against the set actually
// registered in core and applies ONLY the difference — adding users the
// panel wants but core is missing, and removing users core still has but the
// panel dropped. Existing users are left untouched, so there is no downtime.
func (c *Controller) reconcileUsers(desired []panel.UserInfo) {
	coreSet := c.server.GetUserUUIDs(c.tag)

	desiredSet := make(map[string]struct{}, len(desired))
	var missing []panel.UserInfo
	for i := range desired {
		desiredSet[desired[i].Uuid] = struct{}{}
		if _, ok := coreSet[desired[i].Uuid]; !ok {
			missing = append(missing, desired[i])
		}
	}

	var extra []panel.UserInfo
	for uuid := range coreSet {
		if _, ok := desiredSet[uuid]; !ok {
			extra = append(extra, panel.UserInfo{Uuid: uuid})
		}
	}

	if len(missing) == 0 && len(extra) == 0 {
		return
	}

	if len(extra) > 0 {
		if err := c.server.DelUsers(extra, c.tag, c.info); err != nil {
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Error("Reconcile: delete extra users failed")
		}
	}
	if len(missing) > 0 {
		if _, err := c.server.AddUsers(&vCore.AddUsersParams{
			Tag:      c.tag,
			NodeInfo: c.info,
			Users:    missing,
		}); err != nil {
			log.WithFields(log.Fields{"tag": c.tag, "err": err}).Error("Reconcile: add missing users failed")
		}
	}
	c.limiter.UpdateUser(c.tag, missing, extra, nil)
	log.WithField("tag", c.tag).Warnf("User drift reconciled: +%d missing added, -%d extra removed", len(missing), len(extra))
}
