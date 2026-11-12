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

Load balancing strategy. `round-robin` distributes requests in rotation, `consistent-hashing` sends requests for the same destination to the same outbound, and `sticky-sessions` sends requests with the same source and destination to the same outbound for 10 minutes. Defaults to `round-robin`.
