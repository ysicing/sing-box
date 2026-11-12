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

var _ adapter.ConnectionOutboundGroup = (*LoadBalance)(nil)

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

type LoadBalance struct {
	outbound.Adapter
	manager      adapter.OutboundManager
	tags         []string
	members      []adapter.Outbound
	next         atomic.Uint64
	strategy     string
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
		Adapter:     outbound.NewAdapter(C.TypeLoadBalance, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		manager:     service.FromContext[adapter.OutboundManager](ctx),
		tags:        options.Outbounds,
		strategy:    strategy,
		domainCache: make(map[string]domainEntry),
	}, nil
}

func (l *LoadBalance) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	l.members = make([]adapter.Outbound, 0, len(l.tags))
	for i, tag := range l.tags {
		member, loaded := l.manager.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		l.members = append(l.members, member)
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

func (l *LoadBalance) SelectConnection(metadata *adapter.InboundContext, network string) adapter.Outbound {
	return l.selectMember(metadata, M.Socksaddr{}, network)
}

func (l *LoadBalance) selectMember(metadata *adapter.InboundContext, destination M.Socksaddr, network string) adapter.Outbound {
	if len(l.members) == 0 {
		return nil
	}
	var start int
	if l.strategy == LoadBalanceStrategyDomain {
		start = l.domainIndex(loadBalanceKey(metadata, destination))
	} else {
		start = l.nextIndex()
	}
	// 从选中位置向后找第一个支持该网络的成员：不支持 UDP 的成员不会让连接失败，
	// 同一域名的回退结果也保持稳定。代价是被跳过成员的份额会落到它的后继上。
	network = N.NetworkName(network)
	for offset := range l.members {
		index := (start + offset) % len(l.members)
		member := l.members[index]
		if common.Contains(member.Network(), network) {
			l.lastSelected.Store(int64(index))
			return member
		}
	}
	return nil
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

func (l *LoadBalance) domainIndex(key string) int {
	l.domainAccess.Lock()
	defer l.domainAccess.Unlock()
	now := time.Now()
	if entry, exists := l.domainCache[key]; exists && now.Before(entry.expires) {
		entry.expires = now.Add(loadBalanceDomainTTL)
		l.domainCache[key] = entry
		return entry.index
	}
	index := l.nextIndex()
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

func (l *LoadBalance) nextIndex() int {
	return int((l.next.Add(1) - 1) % uint64(len(l.members)))
}

func (l *LoadBalance) AttachConnection(closer io.Closer) func() { return func() {} }

func (l *LoadBalance) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	member := l.selectMember(adapter.ContextFrom(ctx), destination, network)
	if member == nil {
		return nil, E.New("missing supported outbound")
	}
	return member.DialContext(ctx, network, destination)
}

func (l *LoadBalance) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	member := l.selectMember(adapter.ContextFrom(ctx), destination, N.NetworkUDP)
	if member == nil {
		return nil, E.New("missing supported outbound")
	}
	return member.ListenPacket(ctx, destination)
}
