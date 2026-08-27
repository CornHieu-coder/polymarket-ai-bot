# Market Feed Contract Probe

This directory contains the bounded, public, read-only research probe authorized
by `IP-001-market-feed-contract-probe.md`. It is not the production collector or
replay engine.

## Safety boundary

- The CLI accepts explicit public CLOB token IDs; it performs no market discovery.
- The only HTTP operation is `GET https://clob.polymarket.com/book?token_id=...`.
- The only WebSocket is `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- WebSocket redirects, HTTP redirects, environment proxies, credential-bearing
  headers, authenticated channels, and arbitrary endpoint overrides are refused.
- The only outbound WebSocket messages are the documented market subscription
  and `PING` heartbeat. The corrective subscription sends exactly `assets_ids`
  and `type`; optional fields and their documented defaults are recorded as
  omitted rather than asserted as explicit values.

## Probe-only dependencies

From the repository root, install the versions recorded in
`requirements-probe.txt` into an isolated environment if they are not already
available:

```text
python -m pip install -r tools/probes/market_feed_contract/requirements-probe.txt
```

These dependencies do not select the production application stack.

## Offline tests

```text
python -m unittest discover -s tests/probes/market_feed_contract -p test_*.py -v
```

The tests do not require or permit network access.

## Bounded live capture

Supply 1–10 explicit current public outcome token IDs. The default run lasts
three minutes, polls REST every 15 seconds, and makes one clean controlled
reconnect midway through the run.

```text
python -m tools.probes.market_feed_contract --token-id TOKEN_ID_1 --token-id TOKEN_ID_2 --duration-seconds 180 --rest-interval-seconds 15 --offline-test-result "offline suite passed" --baseline-summary outputs/market-feed-contract/ORIGINAL_RUN_ID/summary.json
```

Raw evidence and the machine-readable summary are written beneath the ignored
`outputs/market-feed-contract/` path. When `--baseline-summary` is supplied, its
path must remain under ignored `outputs/`; its bytes are hashed into corrective
provenance, an individual corrective report is kept in the new ignored run
directory, and the deterministic historical/corrective Markdown comparison is
written to `docs/experiments/market-feed-contract-probe.md`.
