### 结构

```json
{
  "type": "load-balance",
  "tag": "balanced",
  "outbounds": ["proxy-a", "proxy-b"],
  "strategy": "round-robin"
}
```

### 字段

#### outbounds

==必填==

成员 outbound 标签。每个新连接按轮询顺序选择一个成员。

#### strategy

负载均衡策略。`round-robin` 将连接轮流分配给各成员；`domain` 按轮询为每个可注册域名分配成员，同一站点的子域名复用该成员。缓存默认 10 分钟未访问后过期，每次访问都会续期。默认为 `round-robin`。
