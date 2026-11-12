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

负载均衡策略。`round-robin` 将请求轮流分配给各成员；`consistent-hashing` 将相同目标地址的请求分配给同一成员；`sticky-sessions` 将相同来源地址和目标地址的请求分配给同一成员，缓存 10 分钟后过期。默认为 `round-robin`。
