package group

import (
	"context"
	"hash/fnv"
	"io"
	"net"
	"slices"
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
)

func RegisterLoadBalance(registry *outbound.Registry) {
	outbound.Register[option.LoadBalanceOutboundOptions](registry, C.TypeLoadBalance, NewLoadBalance)
}

var _ adapter.OutboundGroup = (*LoadBalance)(nil)

const (
	LoadBalanceStrategyRoundRobin        = "round-robin"
	LoadBalanceStrategyConsistentHashing = "consistent-hashing"
	LoadBalanceStrategyStickySessions    = "sticky-sessions"
)

type stickyEntry struct {
	index   int
	expires time.Time
}

type LoadBalance struct {
	outbound.Adapter
	manager        adapter.OutboundManager
	tags           []string
	members        []adapter.Outbound
	next           atomic.Uint64
	strategy       string
	sticky         sync.Mutex
	stickySessions map[string]stickyEntry
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
	case LoadBalanceStrategyRoundRobin, LoadBalanceStrategyConsistentHashing, LoadBalanceStrategyStickySessions:
	default:
		return nil, E.New("unknown load balance strategy: ", strategy)
	}
	for i, member := range options.Outbounds {
		if slices.Contains(options.Outbounds[:i], member) {
			return nil, E.New("duplicate tag: ", member)
		}
	}
	return &LoadBalance{
		Adapter:        outbound.NewAdapter(C.TypeLoadBalance, tag, []string{N.NetworkTCP, N.NetworkUDP}, options.Outbounds),
		manager:        service.FromContext[adapter.OutboundManager](ctx),
		tags:           options.Outbounds,
		strategy:       strategy,
		stickySessions: make(map[string]stickyEntry),
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
	return l.members[l.selectedIndex(nil, M.Socksaddr{}, network)]
}

func (l *LoadBalance) selectedIndex(ctx context.Context, destination M.Socksaddr, network string) int {
	if l.strategy == LoadBalanceStrategyRoundRobin {
		return int((l.next.Add(1) - 1) % uint64(len(l.members)))
	}
	var source string
	if ctx != nil {
		if metadata := adapter.ContextFrom(ctx); metadata != nil {
			source = metadata.Source.String()
			if metadata.Destination.IsValid() {
				destination = metadata.Destination
			}
		}
	}
	key := destination.String()
	if l.strategy == LoadBalanceStrategyStickySessions {
		key = source + "|" + key
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	if l.strategy == LoadBalanceStrategyConsistentHashing {
		return int(hash.Sum64() % uint64(len(l.members)))
	}
	l.sticky.Lock()
	defer l.sticky.Unlock()
	now := time.Now()
	if entry, exists := l.stickySessions[key]; exists && now.Before(entry.expires) {
		return entry.index
	}
	index := int(hash.Sum64() % uint64(len(l.members)))
	l.stickySessions[key] = stickyEntry{index, now.Add(10 * time.Minute)}
	for session, entry := range l.stickySessions {
		if now.After(entry.expires) {
			delete(l.stickySessions, session)
		}
	}
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
