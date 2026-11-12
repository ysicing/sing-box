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

List of outbound tags to balance connections across. Outbounds that do not support the network of a connection are skipped.

#### strategy

Load balancing strategy. `round-robin` distributes connections in rotation. `domain` assigns each registrable domain (sniffed domain preferred, destination IP otherwise) to an outbound using round-robin and reuses that assignment for 10 minutes after the most recent request. Defaults to `round-robin`.
