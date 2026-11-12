### Structure

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

### Fields

#### outbounds

==Required==

List of outbound tags to balance connections across. Outbounds that do not support the network of a connection or failed the health check are excluded; if every outbound failed the health check, all of them are used.

#### strategy

Load balancing strategy. `round-robin` distributes connections in rotation. `domain` assigns each registrable domain (sniffed domain preferred, destination IP otherwise) to an outbound using round-robin and reuses that assignment for 10 minutes after the most recent request. A domain is reassigned when its outbound fails and is not moved back after recovery. Defaults to `round-robin`.

#### url

The URL used for health checks. `https://www.gstatic.com/generate_204` will be used if empty.

#### interval

The health check interval. `3m` will be used if empty.

#### tolerance

The delay tolerance in milliseconds. Outbounds slower than the fastest one by more than this value are excluded from new connections; their existing connections are kept. `0` (no delay filtering) will be used if empty.

#### idle_timeout

The idle timeout, after which health checks stop. `30m` will be used if empty.

#### interrupt_exist_connections

Interrupt existing connections on an outbound when it fails the health check.

Only inbound connections are affected by this setting, internal connections will always be interrupted.
