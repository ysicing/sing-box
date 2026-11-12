package group

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type testMember struct {
	outbound.Adapter
}

func (m *testMember) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}

func (m *testMember) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func newTestLoadBalance(t *testing.T, strategy string, networks ...[]string) *LoadBalance {
	t.Helper()
	return newTestLoadBalanceWithOptions(t, option.LoadBalanceOutboundOptions{Strategy: strategy}, networks...)
}

func newTestLoadBalanceWithOptions(t *testing.T, options option.LoadBalanceOutboundOptions, networks ...[]string) *LoadBalance {
	t.Helper()
	options.Outbounds = []string{"a", "b", "c"}[:len(networks)]
	ctx := service.ContextWithPtr(context.Background(), urltest.NewHistoryStorage())
	created, err := NewLoadBalance(ctx, nil, log.NewNOPFactory().Logger(), "lb", options)
	require.NoError(t, err)
	loadBalance := created.(*LoadBalance)
	members := make([]adapter.Outbound, 0, len(networks))
	for i, tag := range options.Outbounds {
		members = append(members, &testMember{outbound.NewAdapter("test", tag, networks[i], nil)})
	}
	require.NoError(t, loadBalance.initialize(members))
	return loadBalance
}

// setTestDelays 写入各成员的测试结果（0 表示测试失败）并触发一次可用性更新。
func setTestDelays(loadBalance *LoadBalance, delays ...uint16) {
	for i, delay := range delays {
		tag := loadBalance.members[i].Tag()
		if delay == 0 {
			loadBalance.group.history.DeleteURLTestHistory(tag)
		} else {
			loadBalance.group.history.StoreURLTestHistory(tag, &adapter.URLTestHistory{Time: time.Now(), Delay: delay})
		}
	}
	loadBalance.PerformUpdateCheck()
}

type testCloser struct {
	closed bool
}

func (c *testCloser) Close() error {
	c.closed = true
	return nil
}

var testBoth = []string{N.NetworkTCP, N.NetworkUDP}

func selectTags(loadBalance *LoadBalance, network string, metadata ...adapter.InboundContext) []string {
	var tags []string
	for i := range metadata {
		tags = append(tags, loadBalance.SelectConnection(&metadata[i], network).Tag())
	}
	return tags
}

func TestLoadBalanceRoundRobin(t *testing.T) {
	t.Parallel()
	loadBalance := newTestLoadBalance(t, "", testBoth, testBoth, testBoth)
	tags := selectTags(loadBalance, N.NetworkTCP, make([]adapter.InboundContext, 6)...)
	require.Equal(t, []string{"a", "b", "c", "a", "b", "c"}, tags)
	// 面板展示的 Selected 必须与最近一次实际选择一致
	require.Equal(t, "c", loadBalance.Selected(N.NetworkTCP).Tag())
}

func TestLoadBalanceSkipUnsupportedNetwork(t *testing.T) {
	t.Parallel()
	loadBalance := newTestLoadBalance(t, "", testBoth, []string{N.NetworkTCP}, testBoth)
	tags := selectTags(loadBalance, N.NetworkUDP, make([]adapter.InboundContext, 6)...)
	// 在支持该网络的成员之间均分，而不是把被跳过成员的份额压给后继
	require.Equal(t, []string{"a", "c", "a", "c", "a", "c"}, tags)

	tcpOnly := newTestLoadBalance(t, "", []string{N.NetworkTCP})
	require.Nil(t, tcpOnly.SelectConnection(&adapter.InboundContext{}, N.NetworkUDP))
}

func TestLoadBalanceDomain(t *testing.T) {
	t.Parallel()
	loadBalance := newTestLoadBalance(t, LoadBalanceStrategyDomain, testBoth, testBoth, testBoth)
	ipDestination := func(address string) M.Socksaddr {
		return M.SocksaddrFrom(netip.MustParseAddr(address), 443)
	}
	tags := selectTags(loadBalance, N.NetworkTCP,
		// 嗅探到的域名优先于 IP 目标：同一站点的不同 CDN IP 落到同一成员
		adapter.InboundContext{Domain: "www.example.com", Destination: ipDestination("1.1.1.1")},
		adapter.InboundContext{Domain: "API.Example.com.", Destination: ipDestination("2.2.2.2")},
		adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "example.org", Port: 443}},
		adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "cdn.example.com", Port: 443}},
		// 没有域名时按 IP 粘连，IPv4-mapped 地址视为同一目标
		adapter.InboundContext{Destination: ipDestination("3.3.3.3")},
		adapter.InboundContext{Destination: ipDestination("::ffff:3.3.3.3")},
	)
	require.Equal(t, []string{"a", "a", "b", "a", "c", "c"}, tags)
	// 缓存命中同样要更新 Selected
	require.Equal(t, "c", loadBalance.Selected(N.NetworkTCP).Tag())
}

// detour 路径没有路由参与，由 DialContext 自己完成选择。
func TestLoadBalanceDialContext(t *testing.T) {
	t.Parallel()
	loadBalance := newTestLoadBalance(t, "", testBoth, testBoth)
	for _, expected := range []string{"a", "b", "a"} {
		_, err := loadBalance.DialContext(context.Background(), N.NetworkTCP, M.Socksaddr{Fqdn: "example.com", Port: 443})
		require.ErrorIs(t, err, net.ErrClosed)
		require.Equal(t, expected, loadBalance.Selected(N.NetworkTCP).Tag())
	}
}

func TestLoadBalanceHealthCheck(t *testing.T) {
	t.Parallel()
	loadBalance := newTestLoadBalance(t, "", testBoth, testBoth, testBoth)
	six := func() []string {
		return selectTags(loadBalance, N.NetworkTCP, make([]adapter.InboundContext, 6)...)
	}
	// 首轮测试完成前不过滤
	require.Equal(t, []string{"a", "b", "c", "a", "b", "c"}, six())

	setTestDelays(loadBalance, 100, 0, 300)
	require.Equal(t, []string{"a", "c", "a", "c", "a", "c"}, six())

	// 成员恢复后重新参与分流
	setTestDelays(loadBalance, 100, 200, 300)
	require.ElementsMatch(t, []string{"a", "a", "b", "b", "c", "c"}, six())

	// 全部测试失败时退回全体成员，而不是拒绝连接
	setTestDelays(loadBalance, 0, 0, 0)
	require.ElementsMatch(t, []string{"a", "a", "b", "b", "c", "c"}, six())
}

func TestLoadBalanceTolerance(t *testing.T) {
	t.Parallel()
	loadBalance := newTestLoadBalanceWithOptions(t, option.LoadBalanceOutboundOptions{Tolerance: 50}, testBoth, testBoth, testBoth)
	closer := new(testCloser)
	loadBalance.AttachMemberConnection(loadBalance.members[2], closer)
	setTestDelays(loadBalance, 100, 150, 100)
	setTestDelays(loadBalance, 100, 150, 151)
	tags := selectTags(loadBalance, N.NetworkTCP, make([]adapter.InboundContext, 4)...)
	require.NotContains(t, tags, "c")
	require.Contains(t, tags, "a")
	require.Contains(t, tags, "b")
	// 只是偏慢的成员不算失效，已有连接保留
	require.False(t, closer.closed)
}

func TestLoadBalanceInterrupt(t *testing.T) {
	t.Parallel()
	// 路由连接属于入站连接，未开启 interrupt_exist_connections 时不中断
	keep := newTestLoadBalance(t, "", testBoth, testBoth)
	kept := new(testCloser)
	keep.AttachMemberConnection(keep.members[1], kept)
	setTestDelays(keep, 100, 100)
	setTestDelays(keep, 100, 0)
	require.False(t, kept.closed)

	loadBalance := newTestLoadBalanceWithOptions(t, option.LoadBalanceOutboundOptions{InterruptExistConnections: true}, testBoth, testBoth)
	closers := []*testCloser{new(testCloser), new(testCloser)}
	for i, closer := range closers {
		loadBalance.AttachMemberConnection(loadBalance.members[i], closer)
	}
	setTestDelays(loadBalance, 100, 100)
	setTestDelays(loadBalance, 100, 0)
	// 只中断失效成员上的连接
	require.False(t, closers[0].closed)
	require.True(t, closers[1].closed)

	// 全部测试失败多半是测试地址或本机网络的问题，不中断
	setTestDelays(loadBalance, 0, 0)
	require.False(t, closers[0].closed)
}

func TestLoadBalanceDomainReassign(t *testing.T) {
	t.Parallel()
	loadBalance := newTestLoadBalance(t, LoadBalanceStrategyDomain, testBoth, testBoth)
	metadata := adapter.InboundContext{Domain: "example.com"}
	require.Equal(t, "a", loadBalance.SelectConnection(&metadata, N.NetworkTCP).Tag())
	setTestDelays(loadBalance, 0, 100)
	require.Equal(t, "b", loadBalance.SelectConnection(&metadata, N.NetworkTCP).Tag())
	// 原成员恢复后不切回，避免来回跳
	setTestDelays(loadBalance, 100, 100)
	require.Equal(t, "b", loadBalance.SelectConnection(&metadata, N.NetworkTCP).Tag())
}
