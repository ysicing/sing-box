### Structure

```json
{
  "type": "load-balance",
  "tag": "balanced",
  "outbounds": ["proxy-a", "proxy-b"],
  "strategy": "round-robin"
}
```

### Fields

#### outbounds

==Required==

Outbound tags used in round-robin order for each new connection.

#### strategy

Load balancing strategy. `round-robin` distributes connections in rotation. `domain` assigns each registrable domain to an outbound using round-robin and reuses that assignment for 10 minutes after the most recent request. Defaults to `round-robin`.
