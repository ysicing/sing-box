package group

import (
	"context"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"golang.org/x/net/publicsuffix"
)

func RegisterLoadBalance(registry *outbound.Registry) {
	outbound.Register[option.LoadBalanceOutboundOptions](registry, C.TypeLoadBalance, NewLoadBalance)
}

var (
	_ adapter.ConnectionOutboundGroup = (*LoadBalance)(nil)
	_ adapter.URLTestGroup            = (*LoadBalance)(nil)
	_ adapter.InterfaceUpdateListener = (*LoadBalance)(nil)
)

const (
	LoadBalanceStrategyRoundRobin = "round-robin"
	LoadBalanceStrategyDomain     = "domain"
)

const (
	// 域名分配在最近一次访问后保留的时长
	loadBalanceDomainTTL = 10 * time.Minute
	// 过期条目的清理间隔。清理需要在锁内全表扫描，不能每次未命中都做
	loadBalanceSweepInterval = time.Minute
)

type domainEntry struct {
	index   int
	expires time.Time
}

// loadBalanceAvailability 是一次健康检查后的成员可用性快照，下标与 members 对应。
// 选择路径只读快照，不在每条连接上查询测试历史。
type loadBalanceAvailability struct {
	tcp []bool
	udp []bool
}

type LoadBalance struct {
	outbound.Adapter
	ctx                          context.Context
	manager                      adapter.OutboundManager
	logger                       log.ContextLogger
	tags                         []string
	strategy                     string
	link                         string
	interval                     time.Duration
	tolerance                    uint16
	idleTimeout                  time.Duration
	interruptExternalConnections bool
	members                      []adapter.Outbound
	// 与 members 一一对应：成员失效时只中断该成员上的连接
	interrupts   []*interrupt.Group
	group        *URLTestGroup
	checkAccess  sync.Mutex
	availability atomic.Pointer[loadBalanceAvailability]
	// 上一次检查中各成员是否通过测试（TCP、UDP），由 group.updateAccess 保护
	lastAlive    [2][]bool
	next         atomic.Uint64
	lastSelected atomic.Int64
	// 以下字段由 domainAccess 保护
	domainAccess sync.Mutex
	domainCache  map[string]domainEntry
	nextSweep    time.Time
}

func NewLoadBalance(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.LoadBalanceOutboundOptions) (adapter.Outbound, error) {
	if len(options.Outbounds) == 0 {
		return nil, E.New("missing tags")
	}
	strategy := options.Strategy
	if strategy == "" {
		strategy = LoadBalanceStrategyRoundRobin
	}
	switch strategy {
	case LoadBalanceStrategyRoundRobin, LoadBalanceStrategyDomain:
	default:
		return nil, E.New("unknown load balance strategy: ", strategy)
	}
	for i, member := range options.Outbounds {
		if slices.Contains(options.Outbounds[:i], member) {
			return nil, E.New("duplicate tag: ", member)
		}
	}
	return &LoadBalance{
		Adapter:                      outbound.NewAdapter(C.TypeLoadBalance, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		ctx:                          ctx,
		manager:                      service.FromContext[adapter.OutboundManager](ctx),
		logger:                       logger,
		tags:                         options.Outbounds,
		strategy:                     strategy,
		link:                         options.URL,
		interval:                     time.Duration(options.Interval),
		tolerance:                    options.Tolerance,
		idleTimeout:                  time.Duration(options.IdleTimeout),
		interruptExternalConnections: options.InterruptExistConnections,
		domainCache:                  make(map[string]domainEntry),
	}, nil
}

func (l *LoadBalance) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	switch stage {
	case adapter.StartStateStart:
		members := make([]adapter.Outbound, 0, len(l.tags))
		for i, tag := range l.tags {
			member, loaded := l.manager.Outbound(tag)
			if !loaded {
				return E.New("outbound ", i, " not found: ", tag)
			}
			members = append(members, member)
		}
		return l.initialize(members)
	case adapter.StartStateStarted:
		l.group.PostStart()
		scope.Add(l.group.Close)
	}
	return nil
}

func (l *LoadBalance) initialize(members []adapter.Outbound) error {
	// 只复用 URLTestGroup 的测试循环（定时、空闲停止、暂停），测试结果由 updateAvailability 处理，
	// 因此它自身的 tolerance 和中断参数不起作用。
	group, err := NewURLTestGroup(l.ctx, l.manager, l.logger, members, l.link, l.interval, 0, l.idleTimeout, false)
	if err != nil {
		return err
	}
	group.updateHandler = l.updateAvailability
	l.members = members
	l.group = group
	l.interrupts = make([]*interrupt.Group, len(members))
	for i := range l.interrupts {
		l.interrupts[i] = interrupt.NewGroup()
	}
	return nil
}

func (l *LoadBalance) All() []string { return l.tags }

// Selected 返回最近一次选中的成员，仅用于面板展示和延迟历史查询；
// 实际分流由 SelectConnection（路由）和 DialContext/ListenPacket（detour）完成。
func (l *LoadBalance) Selected(network string) adapter.Outbound {
	index := l.lastSelected.Load()
	if index < 0 || index >= int64(len(l.members)) {
		return nil
	}
	return l.members[index]
}

func (l *LoadBalance) URLTest(ctx context.Context) (map[string]uint16, error) {
	return l.group.URLTest(ctx)
}

func (l *LoadBalance) PerformUpdateCheck() {
	l.group.performUpdateCheck()
}

func (l *LoadBalance) InterfaceUpdated(ctx context.Context) {
	group := l.group
	if group == nil {
		return
	}
	if group.pause.IsDevicePaused() || group.pause.IsNetworkPaused() {
		return
	}
	go func() {
		l.checkAccess.Lock()
		defer l.checkAccess.Unlock()
		if ctx.Err() != nil {
			return
		}
		group.CheckOutbounds(ctx, true)
	}()
}

// updateAvailability 根据最新测试历史重建可用性快照，并中断刚失效成员上的连接。
// 由 URLTestGroup 在每轮测试后调用，调用方持有 group.updateAccess。
func (l *LoadBalance) updateAvailability() {
	tcpAlive, tcpAvailable := l.checkMembers(N.NetworkTCP)
	udpAlive, udpAvailable := l.checkMembers(N.NetworkUDP)
	l.availability.Store(&loadBalanceAvailability{tcp: tcpAvailable, udp: udpAvailable})
	failed := make([]bool, len(l.members))
	for n, alive := range [2][]bool{tcpAlive, udpAlive} {
		lastAlive := l.lastAlive[n]
		l.lastAlive[n] = alive
		// 没有任何成员通过测试时，更可能是测试地址或本机网络的问题而不是节点全部失效，
		// 此时中断全部连接只会放大故障。
		if lastAlive == nil || !slices.Contains(alive, true) {
			continue
		}
		for i := range alive {
			if lastAlive[i] && !alive[i] {
				failed[i] = true
			}
		}
	}
	for i, memberFailed := range failed {
		if memberFailed {
			l.logger.Info("outbound ", l.members[i].Tag(), " unavailable, interrupting connections")
			l.interrupts[i].Interrupt(l.interruptExternalConnections)
		}
	}
}

// checkMembers 返回各成员是否通过测试（alive），以及是否参与分流（available）。
// 设置了 tolerance 时，延迟超过“最快成员 + tolerance”的成员不参与分流，但不算失效，已有连接不会被中断。
func (l *LoadBalance) checkMembers(network string) (alive []bool, available []bool) {
	alive = make([]bool, len(l.members))
	available = make([]bool, len(l.members))
	delays := make([]uint16, len(l.members))
	measured := make([]bool, len(l.members))
	var (
		minDelay uint16
		hasDelay bool
	)
	for i, member := range l.members {
		// 嵌套的按连接选择出站组没有固定的“当前成员”，延迟无从比较；它自己负责健康检查
		if _, isConnectionGroup := member.(adapter.ConnectionOutboundGroup); isConnectionGroup {
			alive[i] = true
			continue
		}
		history := l.group.history.LoadURLTestHistory(RealTag(member, network))
		if history == nil {
			continue
		}
		alive[i] = true
		measured[i] = true
		delays[i] = history.Delay
		if !hasDelay || history.Delay < minDelay {
			minDelay = history.Delay
			hasDelay = true
		}
	}
	for i := range alive {
		available[i] = alive[i]
		if l.tolerance > 0 && measured[i] && uint32(delays[i]) > uint32(minDelay)+uint32(l.tolerance) {
			available[i] = false
		}
	}
	return
}

// loadBalanceCandidates 描述一条连接可以选择的成员：通过健康检查且支持该网络。
// 成员支持的网络可能随时变化（例如 selector 切换），所以每条连接现算而不缓存。
type loadBalanceCandidates struct {
	members []adapter.Outbound
	network string
	// 为 nil 表示不按健康检查结果过滤
	available []bool
}

func (c loadBalanceCandidates) contains(index int) bool {
	if c.available != nil && !c.available[index] {
		return false
	}
	return common.Contains(c.members[index].Network(), c.network)
}

func (c loadBalanceCandidates) count() int {
	var count int
	for index := range c.members {
		if c.contains(index) {
			count++
		}
	}
	return count
}

// at 返回第 position 个候选成员的下标，不存在时返回 -1。
func (c loadBalanceCandidates) at(position int) int {
	for index := range c.members {
		if !c.contains(index) {
			continue
		}
		if position == 0 {
			return index
		}
		position--
	}
	return -1
}

func (l *LoadBalance) SelectConnection(metadata *adapter.InboundContext, network string) adapter.Outbound {
	_, member := l.selectMember(metadata, M.Socksaddr{}, network)
	return member
}

func (l *LoadBalance) selectMember(metadata *adapter.InboundContext, destination M.Socksaddr, network string) (int, adapter.Outbound) {
	candidates := loadBalanceCandidates{members: l.members, network: N.NetworkName(network)}
	if availability := l.availability.Load(); availability != nil {
		if candidates.network == N.NetworkUDP {
			candidates.available = availability.udp
		} else {
			candidates.available = availability.tcp
		}
	}
	count := candidates.count()
	if count == 0 && candidates.available != nil {
		// 所有成员都被健康检查排除时退回全体成员：测试地址不可达不代表节点不可用，
		// 继续尝试好过直接拒绝全部连接。
		candidates.available = nil
		count = candidates.count()
	}
	if count == 0 {
		return -1, nil
	}
	var index int
	if l.strategy == LoadBalanceStrategyDomain {
		index = l.domainIndex(loadBalanceKey(metadata, destination), candidates, count)
	} else {
		index = candidates.at(l.nextPosition(count))
	}
	if index < 0 {
		return -1, nil
	}
	l.lastSelected.Store(int64(index))
	return index, l.members[index]
}

// loadBalanceKey 计算 domain 策略的粘连键：可注册域名，或规范化后的 IP。
func loadBalanceKey(metadata *adapter.InboundContext, destination M.Socksaddr) string {
	var host string
	if metadata != nil {
		// TUN/透明代理下 Destination 是 IP，域名只存在于嗅探结果里，必须优先使用
		host = metadata.Domain
		if host == "" && metadata.Destination.IsValid() {
			destination = metadata.Destination
		}
	}
	if host == "" {
		host = destination.Fqdn
	}
	if host == "" {
		return destination.Addr.WithZone("").Unmap().String()
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.WithZone("").Unmap().String()
	}
	if site, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
		return site
	}
	return host
}

func (l *LoadBalance) domainIndex(key string, candidates loadBalanceCandidates, count int) int {
	l.domainAccess.Lock()
	defer l.domainAccess.Unlock()
	now := time.Now()
	// 已分配的成员不再是候选（失效或不支持该网络）时重新分配并覆盖缓存，
	// 成员恢复后域名不会切回去，避免来回跳。
	if entry, exists := l.domainCache[key]; exists && now.Before(entry.expires) && candidates.contains(entry.index) {
		entry.expires = now.Add(loadBalanceDomainTTL)
		l.domainCache[key] = entry
		return entry.index
	}
	index := candidates.at(l.nextPosition(count))
	if index < 0 {
		return -1
	}
	l.domainCache[key] = domainEntry{index, now.Add(loadBalanceDomainTTL)}
	if now.After(l.nextSweep) {
		l.nextSweep = now.Add(loadBalanceSweepInterval)
		for domain, entry := range l.domainCache {
			if now.After(entry.expires) {
				delete(l.domainCache, domain)
			}
		}
	}
	return index
}

// nextPosition 在 count 个候选成员中轮询，候选集合变化时仍然均分。
func (l *LoadBalance) nextPosition(count int) int {
	return int((l.next.Add(1) - 1) % uint64(count))
}

// AttachConnection 只在不知道连接所用成员时被调用，无法按成员中断，仅保持测试循环活跃。
func (l *LoadBalance) AttachConnection(closer io.Closer) func() {
	l.group.Touch()
	return func() {}
}

func (l *LoadBalance) AttachMemberConnection(member adapter.Outbound, closer io.Closer) func() {
	l.group.Touch()
	index := slices.Index(l.members, member)
	if index < 0 {
		return func() {}
	}
	return l.interrupts[index].Add(closer, true)
}

func (l *LoadBalance) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	l.group.Touch()
	index, member := l.selectMember(adapter.ContextFrom(ctx), destination, network)
	if member == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := member.DialContext(ctx, network, destination)
	if err != nil {
		return nil, err
	}
	return l.interrupts[index].NewConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}

func (l *LoadBalance) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	l.group.Touch()
	index, member := l.selectMember(adapter.ContextFrom(ctx), destination, N.NetworkUDP)
	if member == nil {
		return nil, E.New("missing supported outbound")
	}
	conn, err := member.ListenPacket(ctx, destination)
	if err != nil {
		return nil, err
	}
	return l.interrupts[index].NewPacketConn(conn, interrupt.IsExternalConnectionFromContext(ctx)), nil
}
