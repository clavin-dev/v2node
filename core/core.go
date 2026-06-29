package core

import (
	"sync"

	log "github.com/sirupsen/logrus"
	panel "github.com/wyx2685/v2node/api/v2board"
	"github.com/wyx2685/v2node/conf"
	"github.com/wyx2685/v2node/core/app/dispatcher"
	_ "github.com/wyx2685/v2node/core/distro/all"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/inbound"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	coreConf "github.com/xtls/xray-core/infra/conf"
	"google.golang.org/protobuf/proto"
)

type AddUsersParams struct {
	Tag   string
	Users []panel.UserInfo
	*panel.NodeInfo
}

type V2Core struct {
	Config     *conf.Conf
	ReloadCh   chan struct{}
	access     sync.Mutex
	Server     *core.Instance
	users      *UserMap
	ihm        inbound.Manager
	ohm        outbound.Manager
	dispatcher *dispatcher.DefaultDispatcher
}

type UserMap struct {
	uidMap sync.Map // email -> UID (int)
}

func New(config *conf.Conf) *V2Core {
	core := &V2Core{
		Config: config,
		users: &UserMap{},
	}
	return core
}

func (v *V2Core) Start(infos []*panel.NodeInfo) error {
	v.access.Lock()
	defer v.access.Unlock()
	v.Server = getCore(v.Config, infos)
	if err := v.Server.Start(); err != nil {
		return err
	}
	v.ihm = v.Server.GetFeature(inbound.ManagerType()).(inbound.Manager)
	v.ohm = v.Server.GetFeature(outbound.ManagerType()).(outbound.Manager)
	v.dispatcher = v.Server.GetFeature(routing.DispatcherType()).(*dispatcher.DefaultDispatcher)
	return nil
}

func (v *V2Core) Close() error {
	v.access.Lock()
	defer v.access.Unlock()
	v.Config = nil
	v.ihm = nil
	v.ohm = nil
	v.dispatcher = nil
	err := v.Server.Close()
	if err != nil {
		return err
	}
	return nil
}

func getCore(c *conf.Conf, infos []*panel.NodeInfo) *core.Instance {
	// Log Config
	coreLogConfig := &coreConf.LogConfig{
		LogLevel:  c.LogConfig.Level,
		AccessLog: c.LogConfig.Access,
		ErrorLog:  c.LogConfig.Output,
	}
	// Custom config
	dnsConfig, outBoundConfig, routeConfig, err := GetCustomConfig(infos)
	if err != nil {
		log.WithField("err", err).Panic("failed to build custom config")
	}
	// Inbound config
	var inBoundConfig []*core.InboundHandlerConfig

	// Policy config — tuned to match XrayR/soga's lean profile.
	//
	// BufferSize is the per-connection buffer in KB (xray multiplies it by
	// 1024). It was 128 KB/conn, vs XrayR's 4 KB — a 32x difference that, at a
	// few thousand concurrent connections, cost hundreds of MB of RAM for no
	// benefit (splice/zero-copy already bypasses this buffer for unthrottled
	// connections). 4 KB matches XrayR and is the single biggest memory win.
	//
	// ConnectionIdle = seconds a connection may sit idle before being closed.
	// 60 matches XrayR — long enough to avoid churn, short enough to reclaim
	// idle connections promptly (120s previously doubled the live conn count).
	//
	// StatsUserUplink/Downlink are FALSE on purpose: traffic is reported from
	// our own per-user counters in the dispatcher (vc.dispatcher.Counter, read
	// by GetUserTrafficSlice). xray's built-in per-user stats were never read
	// (d.stats is assigned but unused), so enabling them only doubled the
	// per-packet counting work and kept a redundant counter per user in RAM.
	levelPolicyConfig := &coreConf.Policy{
		StatsUserUplink:   false,
		StatsUserDownlink: false,
		Handshake:         proto.Uint32(10),
		ConnectionIdle:    proto.Uint32(60),
		UplinkOnly:        proto.Uint32(2),
		DownlinkOnly:      proto.Uint32(4),
		BufferSize:        proto.Int32(4),
	}
	corePolicyConfig := &coreConf.PolicyConfig{}
	corePolicyConfig.Levels = map[uint32]*coreConf.Policy{0: levelPolicyConfig}
	policyConfig, _ := corePolicyConfig.Build()
	// Build Xray conf
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(coreLogConfig.Build()),
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(policyConfig),
			serial.ToTypedMessage(dnsConfig),
			serial.ToTypedMessage(routeConfig),
		},
		Inbound:  inBoundConfig,
		Outbound: outBoundConfig,
	}
	server, err := core.New(config)
	if err != nil {
		log.WithField("err", err).Panic("failed to create instance")
	}
	log.Info("Xray Core Version: ", core.Version())
	return server
}
