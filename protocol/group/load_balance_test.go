package group

import (
	"context"
	"net"
	"net/netip"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

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
	tags := []string{"a", "b", "c"}[:len(networks)]
	created, err := NewLoadBalance(context.Background(), nil, nil, "lb", option.LoadBalanceOutboundOptions{
		Outbounds: tags,
		Strategy:  strategy,
	})
	require.NoError(t, err)
	loadBalance := created.(*LoadBalance)
	for i, tag := range tags {
		loadBalance.members = append(loadBalance.members, &testMember{outbound.NewAdapter("test", tag, networks[i], nil)})
	}
	return loadBalance
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
	require.NotContains(t, tags, "b")
	require.Contains(t, tags, "a")
	require.Contains(t, tags, "c")

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
