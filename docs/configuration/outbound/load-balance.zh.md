### 结构

```json
{
  "type": "load-balance",
  "tag": "balanced",
  "outbounds": ["proxy-a", "proxy-b"],
  "strategy": "round-robin",
  "url": "",
  "interval": "",
  "tolerance": 0,
  "idle_timeout": "",
  "interrupt_exist_connections": false
}
```

### 字段

#### outbounds

==必填==

参与负载均衡的出站标签列表。不支持当前连接网络类型或未通过健康检查的成员不参与分配；所有成员都未通过健康检查时退回全部成员。

#### strategy

负载均衡策略。`round-robin` 将连接轮流分配给各成员；`domain` 按轮询为每个可注册域名分配成员（优先使用嗅探到的域名，没有域名时使用目标 IP），同一站点的子域名复用该成员。分配在 10 分钟未访问后过期，每次访问都会续期。成员失效后域名会重新分配，成员恢复后不切回。默认为 `round-robin`。

#### url

用于健康检查的链接。默认使用 `https://www.gstatic.com/generate_204`。

#### interval

健康检查间隔。默认使用 `3m`。

#### tolerance

以毫秒为单位的延迟容差。延迟超过最快成员加上该值的成员不参与分配，其已有连接不受影响。默认使用 `0`，即不按延迟筛选。

#### idle_timeout

空闲超时，超时后停止健康检查。默认使用 `30m`。

#### interrupt_exist_connections

当成员未通过健康检查时，中断该成员上的现有连接。

仅入站连接受此设置影响，内部连接将始终被中断。
