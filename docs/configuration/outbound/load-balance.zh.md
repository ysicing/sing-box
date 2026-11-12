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

参与负载均衡的出站标签列表。不支持当前连接网络类型的成员会被跳过。

#### strategy

负载均衡策略。`round-robin` 将连接轮流分配给各成员；`domain` 按轮询为每个可注册域名分配成员（优先使用嗅探到的域名，没有域名时使用目标 IP），同一站点的子域名复用该成员。分配在 10 分钟未访问后过期，每次访问都会续期。默认为 `round-robin`。
