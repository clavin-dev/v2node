package node

import (
	"context"
	"errors"
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
		ReloadCh: c.server.ReloadCh,
	}
	// fetch user list task
	c.userReportPeriodic = &task.Task{
		Name:     "reportUserTrafficTask[" + c.tag + "]",
		Interval: node.PushInterval,
		Execute:  c.reportUserTrafficTask,
		ReloadCh: c.server.ReloadCh,
	}
	log.WithField("tag", c.tag).Info("Start monitor node status")
	// delay to start nodeInfoMonitor
	_ = c.nodeInfoMonitorPeriodic.Start(false)
	log.WithField("tag", c.tag).Info("Start report node status")
	_ = c.userReportPeriodic.Start(false)
	if node.Security == panel.Tls {
		switch c.info.Common.CertInfo.CertMode {
		case "none", "", "file", "self":
		default:
			c.renewCertPeriodic = &task.Task{
				Name:     "renewCertTask",
				Interval: time.Hour * 24,
				Execute:  c.renewCertTask,
				ReloadCh: c.server.ReloadCh,
			}
			log.WithField("tag", c.tag).Info("Start renew cert")
			// delay to start renewCert
			_ = c.renewCertPeriodic.Start(true)
		}
	}
}

func (c *Controller) nodeInfoMonitor(ctx context.Context) (err error) {
	// get node info
	newN, err := c.apiClient.GetNodeInfo(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get node info failed")
		return nil
	}
	if newN != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
		}).Info("Node info changed, updating in-place")
		// In-place update: only this node's inbound is replaced.
		// All other nodes and their connections remain untouched.
		if err = c.server.DelNode(c.tag); err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Failed to remove old inbound")
			return nil
		}
		if err = c.server.AddNode(c.tag, newN); err != nil {
			log.WithFields(log.Fields{
				"tag": c.tag,
				"err": err,
			}).Error("Failed to add new inbound")
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
		c.info = newN
		log.WithField("tag", c.tag).Info("Node inbound updated without restart")
	} else {
		log.WithField("tag", c.tag).Debug("Node info no change")
	}

	// get user info — ETag is NOT committed here; we hold newEtag
	// and only commit it after c.userList is successfully updated.
	// FIX: prevents ETag/userList desync when downstream processing fails.
	newU, newEtag, err := c.apiClient.GetUserList(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Error("Get user list failed")
		return nil
	}

	// get user alive — FIX: do NOT let this block user sync.
	// If it fails, we still proceed with user changes.
	newA, err := c.apiClient.GetUserAlive(ctx)
	if err != nil {
		log.WithFields(log.Fields{
			"tag": c.tag,
			"err": err,
		}).Warn("Get alive list failed, proceeding with user sync")
		// Don't return — continue with user sync
	}

	// update alive list
	if newA != nil {
		c.limiter.UpdateAliveList(newA)
	}
	// node no changed, check users
	if len(newU) == 0 {
		log.WithField("tag", c.tag).Debug("User list no change")
		return nil
	}
	// FIX: if context is already cancelled, we are a leaked goroutine
	// from a timed-out cycle. Abort to avoid overwriting newer state.
	if ctx.Err() != nil {
		log.WithField("tag", c.tag).Warn("Leaked goroutine detected after GetUserList, aborting commit")
		return nil
	}
	deleted, added, modified := compareUserList(c.userList, newU)
	if len(deleted) > 0 {
		// have deleted users
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
		// have added users
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
		// update Limiter
		c.limiter.UpdateUser(c.tag, added, deleted, modified)
	}
	// FIX: final guard — if context was cancelled during user operations,
	// do NOT commit. This prevents leaked goroutines from overwriting
	// newer ETag/userList with stale data.
	if ctx.Err() != nil {
		log.WithField("tag", c.tag).Warn("Leaked goroutine detected before commit, discarding stale state")
		return nil
	}
	c.userList = newU
	c.apiClient.CommitUserEtag(newEtag)
	log.WithField("tag", c.tag).Infof("%d user deleted, %d user added, %d user modified", len(deleted), len(added), len(modified))
	return nil
}
