package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/varbin"
)

type URLTestHistory struct {
	Time  time.Time `json:"time"`
	Delay uint16    `json:"delay"`
}

type V2RayServer interface {
	LifecycleService
	StatsService() ConnectionTracker
}

type CacheFile interface {
	LifecycleService

	CacheID() string

	StoreFakeIP() bool
	FakeIPStorage

	StoreRDRC() bool
	RDRCStore

	StoreDNS() bool
	DNSCacheStore

	SetDisableExpire(disableExpire bool)
	SetOptimisticTimeout(timeout time.Duration)

	LoadMode() string
	StoreMode(mode string) error
	LoadSelected(group string) string
	StoreSelected(group string, selected string) error
	LoadGroupExpand(group string) (isExpand bool, loaded bool)
	StoreGroupExpand(group string, expand bool) error
	LoadRuleSet(tag string) *SavedBinary
	SaveRuleSet(tag string, set *SavedBinary) error
}

type SavedBinary struct {
	Content     []byte
	LastUpdated time.Time
	LastEtag    string
	URLHash     []byte
}

func (s *SavedBinary) MarshalBinary() ([]byte, error) {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, uint8(2))
	if err != nil {
		return nil, err
	}
	_, err = varbin.WriteUvarint(&buffer, uint64(len(s.Content)))
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(s.Content)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buffer, binary.BigEndian, s.LastUpdated.Unix())
	if err != nil {
		return nil, err
	}
	_, err = varbin.WriteUvarint(&buffer, uint64(len(s.LastEtag)))
	if err != nil {
		return nil, err
	}
	_, err = buffer.WriteString(s.LastEtag)
	if err != nil {
		return nil, err
	}
	_, err = varbin.WriteUvarint(&buffer, uint64(len(s.URLHash)))
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(s.URLHash)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *SavedBinary) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	var version uint8
	err := binary.Read(reader, binary.BigEndian, &version)
	if err != nil {
		return err
	}
	contentLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if contentLength > uint64(reader.Len()) {
		return E.New("invalid content length: ", contentLength)
	}
	s.Content = make([]byte, contentLength)
	_, err = io.ReadFull(reader, s.Content)
	if err != nil {
		return err
	}
	var lastUpdated int64
	err = binary.Read(reader, binary.BigEndian, &lastUpdated)
	if err != nil {
		return err
	}
	s.LastUpdated = time.Unix(lastUpdated, 0)
	etagLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if etagLength > uint64(reader.Len()) {
		return E.New("invalid etag length: ", etagLength)
	}
	etagBytes := make([]byte, etagLength)
	_, err = io.ReadFull(reader, etagBytes)
	if err != nil {
		return err
	}
	s.LastEtag = string(etagBytes)
	if version < 2 {
		return nil
	}
	urlHashLength, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	if urlHashLength > uint64(reader.Len()) {
		return E.New("invalid url hash length: ", urlHashLength)
	}
	s.URLHash = make([]byte, urlHashLength)
	_, err = io.ReadFull(reader, s.URLHash)
	if err != nil {
		return err
	}
	return nil
}

type OutboundGroup interface {
	Outbound
	All() []string
	Selected(network string) Outbound
	AttachConnection(closer io.Closer) (detach func())
}

// ConnectionOutboundGroup 是按连接选择成员的出站组。
// Selected 拿不到连接信息，只能返回最近一次的选择结果（供面板展示），
// 因此路由展开出站链时必须改用 SelectConnection，且每条连接只调用一次。
type ConnectionOutboundGroup interface {
	OutboundGroup
	SelectConnection(metadata *InboundContext, network string) Outbound
}

type URLTestGroup interface {
	OutboundGroup
	URLTest(ctx context.Context) (map[string]uint16, error)
	PerformUpdateCheck()
}
