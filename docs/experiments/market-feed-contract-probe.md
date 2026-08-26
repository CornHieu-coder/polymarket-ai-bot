# Market Feed Contract Probe

Status: **completed bounded read-only research probe**

This report is empirical evidence for IP-001, not production collector/replay documentation and not a trading-performance result.

## Run identity and configuration

| Property | Value |
| --- | --- |
| Run ID | `20260826T015136.607229Z-45c482c8` |
| UTC start | `2026-08-26T01:51:36.703084Z` |
| UTC end | `2026-08-26T01:54:39.070920Z` |
| Requested duration | 180 seconds |
| Actual wall/monotonic duration | 182.375 seconds |
| REST interval | 15 seconds |
| Controlled reconnects | 1 |
| Initial dump | `true` (explicit) |
| WebSocket level | 2 |
| Git commit captured | `09c30078c0f645d9b8d796e51f7dd57487661376` |
| Git worktree dirty at capture | `false` |
| Python | `3.12.4` |
| Probe transports | `httpx 0.28.1`, `websockets 16.0` |
| Offline test command | `python -m unittest discover -s tests/probes/market_feed_contract -p 'test_*.py' -v` |
| Offline test result | 38 tests passed in 0.454s on Python 3.12.4 |
| Raw evidence (ignored by Git) | `outputs\market-feed-contract\20260826T015136.607229Z-45c482c8` |
| Raw capture boundary | WebSocket application text/bytes after protocol decompression |

Explicit sampled token IDs:

- `35198549486569600595408965368290795524428922595732253580266052028158040373233`
- `29633826701713552120011280893309822625942861174174976276686142480636225243753`
- `49869160041323840584812036178393008034046781140245590791017996532351395946894`
- `39652532623602170846456682951463932352134896182641898905432425437612057868317`

Sample provenance: Selected 2026-08-26 from the public Gamma /markets response: active=true, closed=false, enableOrderBook=true, acceptingOrders=true; two highest-volume binary markets at discovery time.

## Methodology and safety boundary

- Connected only to the fixed public Polymarket Market WebSocket and sent the documented market subscription plus `PING` heartbeats.
- Performed only public `GET /book` requests. HTTP redirects, environment proxies, credential-bearing headers, and WebSocket redirects were rejected.
- Used two independent WebSocket sessions with a clean client-controlled disconnect/reconnect; reconstruction state was not carried across the gap.
- Persisted exact application frames before derived state analysis, with separate source timestamps and local UTC receive timestamps, a global monotonic ingest sequence, session IDs, and SHA-256 payload digests.
- Parsed canonical price and size values with Python `Decimal`; binary float equality was not used for contract conclusions.
- Applied the project hypothesis in observed entry order: BUY→bid, SELL→ask, zero→delete, non-zero→replace. Best-price agreement alone was not treated as proof of aggregate-size replacement.
- Attempted only the cited first-party order-book-summary SHA-1 algorithm and only when all required fields existed in the same payload/state. No missing hash input was filled from REST or another message.
- Compared REST depth only across a stable observed request window: the asset had valid state before the request, and its session/version did not change through receipt. Other responses remained diagnostics.

No authenticated endpoint, user WebSocket, API credential, wallet, private key, order construction, order simulation, order placement, or cancellation was used.

## Evidence volume

| Record type | Count |
| --- | ---: |
| `control_events` | 231 |
| `rest_responses` | 45 |
| `websocket_frames` | 234 |

## Q1–Q8 conclusions

Labels apply only to this bounded sample. `CONFIRMED` means directly supported within this run with no eligible unexplained counterexample; `CONTRADICTED` means eligible evidence falsified the tested claim; `UNRESOLVED` means evidence was absent, ambiguous, ineligible, or non-discriminating.

| Question | Status | Bounded conclusion |
| --- | --- | --- |
| Q1 — Current event schema | **UNRESOLVED** | Enumerated 2 event types across 224 logical events; 0 sequence/order-field presences observed. Absence is sample-bounded. |
| Q2 — Initial dump/reconnect | **CONFIRMED** | Tested 2 sessions including one controlled reconnect; 0 tokens lacked book-first evidence. |
| Q3 — Price-level update semantics | **UNRESOLVED** | Applied 228 non-zero and 4 zero-size updates; BBO 228/232 exact, with 192 independently validated discriminating replacements. |
| Q4 — Multiple updates per frame | **UNRESOLVED** | Observed 216 price-change frames, 216 multi-entry frames, and 0 repeated-key frames. |
| Q5 — Hash semantics | **UNRESOLVED** | Official algorithm: WS book 0/0 matches; post-change 0/0 matches; 240 payloads ineligible because inputs were absent. |
| Q6 — REST `/book` comparison | **CONFIRMED** | REST /book: 45/45 successful; 15/15 stable-window comparisons matched exactly. Unaligned mismatches remain diagnostics only. |
| Q7 — Optional WS book metadata | **UNRESOLVED** | Measured optional metadata over 8 WS book events: last_trade_price=8, min_order_size=0, neg_risk=0, tick_size=8. |
| Q8 — Timestamp behavior | **CONFIRMED** | Measured 224 timestamped events; regressions=0, future-source=0, local-clock-dependent p50 delay=752.537 ms. |

### Q1 — Current event schema — **UNRESOLVED**

#### `book` (8 logical events)

| Field path | Events containing field | Total occurrences | Observed types |
| --- | ---: | ---: | --- |
| `asks` | 8 | 8 | `{"array":8}` |
| `asks[]` | 6 | 344 | `{"object":344}` |
| `asks[].price` | 6 | 344 | `{"string":344}` |
| `asks[].size` | 6 | 344 | `{"string":344}` |
| `asset_id` | 8 | 8 | `{"string":8}` |
| `bids` | 8 | 8 | `{"array":8}` |
| `bids[]` | 6 | 344 | `{"object":344}` |
| `bids[].price` | 6 | 344 | `{"string":344}` |
| `bids[].size` | 6 | 344 | `{"string":344}` |
| `event_type` | 8 | 8 | `{"string":8}` |
| `hash` | 8 | 8 | `{"string":8}` |
| `last_trade_price` | 8 | 8 | `{"string":8}` |
| `market` | 8 | 8 | `{"string":8}` |
| `tick_size` | 8 | 8 | `{"string":8}` |
| `timestamp` | 8 | 8 | `{"string":8}` |

#### `price_change` (216 logical events)

| Field path | Events containing field | Total occurrences | Observed types |
| --- | ---: | ---: | --- |
| `event_type` | 216 | 216 | `{"string":216}` |
| `market` | 216 | 216 | `{"string":216}` |
| `price_changes` | 216 | 216 | `{"array":216}` |
| `price_changes[]` | 216 | 432 | `{"object":432}` |
| `price_changes[].asset_id` | 216 | 432 | `{"string":432}` |
| `price_changes[].best_ask` | 216 | 432 | `{"string":432}` |
| `price_changes[].best_bid` | 216 | 432 | `{"string":432}` |
| `price_changes[].hash` | 216 | 432 | `{"string":432}` |
| `price_changes[].price` | 216 | 432 | `{"string":432}` |
| `price_changes[].side` | 216 | 432 | `{"string":432}` |
| `price_changes[].size` | 216 | 432 | `{"string":432}` |
| `timestamp` | 216 | 216 | `{"string":216}` |

Candidate ordering fields observed: none.

Field absence is reported only for this bounded sample and is not a claim that a field can never appear.

### Q2 — Initial dump/reconnect — **CONFIRMED**

| Session | Token | First state event (seq) | First book seq | First delta seq | Book events | Delta entries | Controlled close |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `session-001` | `29633826701713552120011280893309822625942861174174976276686142480636225243753` | `book` (`7`) | 7 | 9 | 1 | 56 | `true` |
| `session-001` | `35198549486569600595408965368290795524428922595732253580266052028158040373233` | `book` (`7`) | 7 | 9 | 1 | 56 | `true` |
| `session-001` | `39652532623602170846456682951463932352134896182641898905432425437612057868317` | `book` (`7`) | 7 | 11 | 1 | 43 | `true` |
| `session-001` | `49869160041323840584812036178393008034046781140245590791017996532351395946894` | `book` (`7`) | 7 | 11 | 1 | 43 | `true` |
| `session-002` | `29633826701713552120011280893309822625942861174174976276686142480636225243753` | `book` (`235`) | 235 | 236 | 1 | 58 | `true` |
| `session-002` | `35198549486569600595408965368290795524428922595732253580266052028158040373233` | `book` (`235`) | 235 | 236 | 1 | 58 | `true` |
| `session-002` | `39652532623602170846456682951463932352134896182641898905432425437612057868317` | `book` (`235`) | 235 | 239 | 1 | 59 | `true` |
| `session-002` | `49869160041323840584812036178393008034046781140245590791017996532351395946894` | `book` (`235`) | 235 | 239 | 1 | 59 | `true` |

A token/session without a later delta can confirm receipt of a fresh snapshot but cannot fully exercise snapshot-before-delta precedence.

### Q3 — Price-level update semantics — **UNRESOLVED**

| Measurement | Count |
| --- | ---: |
| Observed price-change entries | 432 |
| Applied price-change entries | 232 |
| Excluded price-change entries | 200 |
| Best bid/ask comparisons | 232 |
| Exact best bid/ask matches | 228 |
| Best bid/ask mismatches | 4 |
| Direct single-entry mismatches | 0 |
| Non-zero updates | 228 |
| Zero-size updates | 4 |
| Discriminating non-zero updates | 226 |
| Independently validated non-zero replacements | 192 |
| Discriminating zero deletions | 4 |
| Independently validated zero deletions | 0 |
| Idempotent replacements/deletions | 2 |

Best-price agreement alone cannot distinguish aggregate replacement from arithmetic increment. Independent validation requires exact full-depth agreement in a stable observed REST request window.

Side counts: `{"BUY":116,"SELL":116}`. Component comparisons: `{"best_ask_matches":230,"best_ask_mismatches":2,"best_ask_total":232,"best_bid_matches":230,"best_bid_mismatches":2,"best_bid_total":232}`.

### Q4 — Multiple updates per frame — **UNRESOLVED**

Entries-per-frame histogram: `{"2":216}`.

| Measurement | Count |
| --- | ---: |
| Price-change frames | 216 |
| Multi-entry frames | 216 |
| Multiple entries for the same asset | 0 |
| Repeated asset/side/price key | 0 |
| Observed order-sensitive repeated-key frames | 0 |

A zero count establishes only non-observation in this run; it does not prove that the payload shape cannot occur.

### Q5 — Hash semantics — **UNRESOLVED**

Algorithm attempted: py-clob-client-v2 generate_orderbook_summary_hash: SHA-1 of compact ordered JSON with hash set to an empty string.

| Payload shape | Attempts | Matches | Mismatches | Skipped: missing fields |
| --- | ---: | ---: | ---: | ---: |
| WebSocket `book` | 0 | 0 | 0 | 8 |
| Post-update `price_change` | 0 | 0 | 0 | 232 |
| REST diagnostic | 45 | 45 | 0 | n/a |

Non-reproduction or missing required inputs leaves semantics unresolved; no alternative hash interpretation was reverse engineered.

### Q6 — REST `/book` comparison — **CONFIRMED**

HTTP status counts: `{"200":45}`.

| Expected REST field | Present | Missing |
| --- | ---: | ---: |
| `hash` | 45 | 0 |
| `last_trade_price` | 45 | 0 |
| `min_order_size` | 45 | 0 |
| `neg_risk` | 45 | 0 |
| `tick_size` | 45 | 0 |

Alignment rule: Compare levels only when a valid reconstructed state exists before the request and its session/version remain unchanged through receipt.

Aligned comparisons: **15**; exact matches: **15**; mismatches: **0**; unaligned diagnostics: **30**.

A mismatch outside the declared alignment window is not classified as a WebSocket reconstruction failure.

### Q7 — Optional metadata in WS book frames — **UNRESOLVED**

Denominator: **8** WebSocket `book` events.

| Optional field | Presence count |
| --- | ---: |
| `last_trade_price` | 8 |
| `min_order_size` | 0 |
| `neg_risk` | 0 |
| `tick_size` | 8 |

Observed absence in this sample does not establish universal absence.

### Q8 — Timestamp behavior — **CONFIRMED**

Per-token timestamped event counts: `{"29633826701713552120011280893309822625942861174174976276686142480636225243753":116,"35198549486569600595408965368290795524428922595732253580266052028158040373233":116,"39652532623602170846456682951463932352134896182641898905432425437612057868317":104,"49869160041323840584812036178393008034046781140245590791017996532351395946894":104}`.

| Measurement | Value |
| --- | ---: |
| Timestamped logical events | 224 |
| Source timestamp regressions | 0 |
| Adjacent same-timestamp events | 6 |
| Same-timestamp groups | 6 |
| Source timestamps after local receive | 0 |
| Delay samples | 224 |
| Minimum delay (ms) | 733.207 |
| P50 delay (ms) | 752.537 |
| P95 delay (ms) | 1294.112 |
| Maximum delay (ms) | 9204.928 |

Quantile method: nearest-rank without interpolation.

Delay is local-clock dependent. Negative values are retained, not corrected, and may indicate clock skew. These measurements do not prove gap-free delivery.

## Counterexamples and errors

- Repeated raw-payload hashes retained: **15**. This all-frame count is not, by itself, evidence of duplicate data events.
- Q3: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.29","side":"BUY","size":"0"}`
- Q3: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","change_index":1,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_bid":{"derived":null,"observed":"0"}},"multi_entry_semantics_ambiguous":true,"price":"0.71","side":"SELL","size":"0"}`
- Q3: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":239,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.31","side":"BUY","size":"0"}`
- Q3: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","change_index":1,"frame_entry_count":2,"ingest_sequence":239,"mismatches":{"best_bid":{"derived":null,"observed":"0"}},"multi_entry_semantics_ambiguous":true,"price":"0.69","side":"SELL","size":"0"}`
- Parser/operational error `best_bid_ask_mismatch`: **4** occurrence(s).
- Parser/operational error `price_change_without_valid_book`: **200** occurrence(s).

Representative traceable examples (up to three per category, linked by raw `ingest_sequence`; complete counts and instances remain in the ignored evidence summary):

- `{"code":"best_bid_ask_mismatch","details":{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.29","side":"BUY","size":"0"},"event_type":"price_change","ingest_sequence":11,"message":"reconstructed best bid/ask did not match event values"}`
- `{"code":"best_bid_ask_mismatch","details":{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","change_index":1,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_bid":{"derived":null,"observed":"0"}},"multi_entry_semantics_ambiguous":true,"price":"0.71","side":"SELL","size":"0"},"event_type":"price_change","ingest_sequence":11,"message":"reconstructed best bid/ask did not match event values"}`
- `{"code":"best_bid_ask_mismatch","details":{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":239,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.31","side":"BUY","size":"0"},"event_type":"price_change","ingest_sequence":239,"message":"reconstructed best bid/ask did not match event values"}`
- `best_bid_ask_mismatch`: 1 additional traceable instance(s) retained outside Git.
- `{"code":"price_change_without_valid_book","details":{},"event_type":"price_change","ingest_sequence":14,"message":"cannot apply change for uninitialized/invalid asset 49869160041323840584812036178393008034046781140245590791017996532351395946894"}`
- `{"code":"price_change_without_valid_book","details":{},"event_type":"price_change","ingest_sequence":14,"message":"cannot apply change for uninitialized/invalid asset 39652532623602170846456682951463932352134896182641898905432425437612057868317"}`
- `{"code":"price_change_without_valid_book","details":{},"event_type":"price_change","ingest_sequence":24,"message":"cannot apply change for uninitialized/invalid asset 49869160041323840584812036178393008034046781140245590791017996532351395946894"}`
- `price_change_without_valid_book`: 197 additional traceable instance(s) retained outside Git.

## Raw evidence integrity

Raw evidence is outside Git under the ignored run directory. The committed report contains only derived counts and evidence references.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `control-events.jsonl` | 99154 | `b1def99d4f6f9f1bdc5076cf29b229e79ded7f272d7220af25a3b466a5481a9e` |
| `raw-rest-book.jsonl` | 229741 | `1c5203639a753c5c69b7d3b0ca81a564bad71c4724b8f7d38004b6056e6b78b3` |
| `raw-websocket.jsonl` | 439616 | `70b54a3a693595300b20fde97d0125b6c235803cc43fcff255e2b1cf69097453` |
| `resolved-config.json` | 1955 | `79137617b3cff5655fcf91faee94404c97e00b09937b2e3624f0cc831e2c8861` |
| `run-config.json` | 1955 | `79137617b3cff5655fcf91faee94404c97e00b09937b2e3624f0cc831e2c8861` |

## Limitations

- This bounded sample cannot establish that an unobserved field or payload shape never occurs.
- A short successful run cannot establish gap-free WebSocket delivery.
- Local-clock delay is not authoritative exchange/network latency.
- REST and WebSocket observations are asynchronous; only predeclared stable observed windows were eligible for exact depth comparison.
- Feed-derived trade direction was not evaluated and is not treated as authoritative.
- No result in this report is a trading-performance result.

## Deviations from IP-001

None. The probe used the packet-authorized explicit-token input mode and did not implement discovery, a production collector, or a replay engine.

## First-party references applied

- https://docs.polymarket.com/api-reference/wss/market
- https://github.com/Polymarket/agent-skills/blob/main/websocket.md
- https://docs.polymarket.com/api-reference/market-data/get-order-book
- https://docs.polymarket.com/v2-migration
- https://docs.polymarket.com/market-data/overview
- https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py
