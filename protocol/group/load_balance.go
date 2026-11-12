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
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
	"golang.org/x/net/publicsuffix"
)

func RegisterLoadBalance(registry *outbound.Registry) {
	outbound.Register[option.LoadBalanceOutboundOptions](registry, C.TypeLoadBalance, NewLoadBalance)
}

var _ adapter.OutboundGroup = (*LoadBalance)(nil)

const (
	LoadBalanceStrategyRoundRobin = "round-robin"
	LoadBalanceStrategyDomain     = "domain"
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
	selectionMu  sync.Mutex
	lastSelected atomic.Int64
	domainCache  map[string]domainEntry
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

func (l *LoadBalance) Selected(network string) adapter.Outbound {
	if len(l.members) == 0 {
		return nil
	}
	index := l.lastSelected.Load()
	if index < 0 || index >= int64(len(l.members)) {
		return nil
	}
	return l.members[index]
}

func (l *LoadBalance) selectedIndex(ctx context.Context, destination M.Socksaddr, network string) int {
	if l.strategy == LoadBalanceStrategyRoundRobin {
		return l.nextIndex()
	}
	if ctx != nil {
		if metadata := adapter.ContextFrom(ctx); metadata != nil {
			if metadata.Destination.IsValid() {
				destination = metadata.Destination
			}
		}
	}
	host := strings.TrimSuffix(destination.Fqdn, ".")
	if host == "" {
		host = destination.AddrString()
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		host = addr.WithZone("").Unmap().String()
	} else if site, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
		host = site
	}
	l.selectionMu.Lock()
	defer l.selectionMu.Unlock()
	now := time.Now()
	if entry, exists := l.domainCache[host]; exists && now.Before(entry.expires) {
		entry.expires = now.Add(10 * time.Minute)
		l.domainCache[host] = entry
		return entry.index
	}
	index := l.nextIndex()
	l.domainCache[host] = domainEntry{index, now.Add(10 * time.Minute)}
	for domain, entry := range l.domainCache {
		if now.After(entry.expires) {
			delete(l.domainCache, domain)
		}
	}
	return index
}

func (l *LoadBalance) nextIndex() int {
	index := int((l.next.Add(1) - 1) % uint64(len(l.members)))
	l.lastSelected.Store(int64(index))
	return index
}

func (l *LoadBalance) AttachConnection(closer io.Closer) func() { return func() {} }

func (l *LoadBalance) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if len(l.members) == 0 {
		return nil, E.New("missing supported outbound")
	}
	member := l.members[l.selectedIndex(ctx, destination, network)]
	if member == nil {
		return nil, E.New("missing supported outbound")
	}
	return member.DialContext(ctx, network, destination)
}

func (l *LoadBalance) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if len(l.members) == 0 {
		return nil, E.New("missing supported outbound")
	}
	member := l.members[l.selectedIndex(ctx, destination, N.NetworkUDP)]
	if member == nil {
		return nil, E.New("missing supported outbound")
	}
	return member.ListenPacket(ctx, destination)
}
