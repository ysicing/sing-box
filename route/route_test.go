package route

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type testOutbound struct {
	outbound.Adapter
}

func newTestOutbound(tag string) *testOutbound {
	return &testOutbound{outbound.NewAdapter("test", tag, []string{N.NetworkTCP, N.NetworkUDP}, nil)}
}

func (o *testOutbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return nil, net.ErrClosed
}

func (o *testOutbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

// testConnectionGroup 的 Selected 固定返回第一个成员，只有 SelectConnection 会轮换。
type testConnectionGroup struct {
	*testOutbound
	members []adapter.Outbound
	next    int
}

func (g *testConnectionGroup) All() []string { return nil }

func (g *testConnectionGroup) Selected(network string) adapter.Outbound { return g.members[0] }

func (g *testConnectionGroup) AttachConnection(closer io.Closer) func() { return func() {} }

func (g *testConnectionGroup) SelectConnection(metadata *adapter.InboundContext, network string) adapter.Outbound {
	member := g.members[g.next%len(g.members)]
	g.next++
	return member
}

func TestResolveOutboundConnectionGroup(t *testing.T) {
	t.Parallel()
	group := &testConnectionGroup{
		testOutbound: newTestOutbound("group"),
		members:      []adapter.Outbound{newTestOutbound("a"), newTestOutbound("b")},
	}
	for _, expected := range []string{"a", "b", "a"} {
		chain, err := resolveOutbound(group, N.NetworkTCP, &adapter.InboundContext{})
		require.NoError(t, err)
		require.Len(t, chain, 2)
		require.Equal(t, expected, chain[1].Tag())
	}
	// 预匹配阶段不能消耗选择状态，否则随后的正常路由会让同一条连接推进两次
	_, err := resolveOutbound(group, N.NetworkTCP, nil)
	require.Error(t, err)
	require.Equal(t, 3, group.next)
}
