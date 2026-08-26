# Market Feed Contract Probe: Historical and Corrective Evidence

Status: **comparative bounded read-only research report**

The historical run remains immutable evidence. The corrective run is additional evidence collected under a revised probe; it does not overwrite the historical run, its exclusions, counterexamples, status labels, or manifest.

## Run identities and provenance

| Property | Historical/original run | Corrective run |
| --- | --- | --- |
| Run ID | `20260826T015136.607229Z-45c482c8` | `20260826T165055.566599Z-84d473ac` |
| UTC start | `2026-08-26T01:51:36.703084Z` | `2026-08-26T16:50:55.844742Z` |
| UTC end | `2026-08-26T01:54:39.070920Z` | `2026-08-26T16:53:58.743151Z` |
| Requested duration (seconds) | 180 | 180 |
| Actual duration (seconds) | 182.375 | 182.891 |
| REST interval (seconds) | 15 | 5 |
| Controlled reconnects | 1 | 1 |
| Sample/token IDs | `["35198549486569600595408965368290795524428922595732253580266052028158040373233","29633826701713552120011280893309822625942861174174976276686142480636225243753","49869160041323840584812036178393008034046781140245590791017996532351395946894","39652532623602170846456682951463932352134896182641898905432425437612057868317"]` | `["35198549486569600595408965368290795524428922595732253580266052028158040373233","29633826701713552120011280893309822625942861174174976276686142480636225243753","49869160041323840584812036178393008034046781140245590791017996532351395946894","39652532623602170846456682951463932352134896182641898905432425437612057868317"]` |
| Sample provenance | Selected 2026-08-26 from the public Gamma /markets response: active=true, closed=false, enableOrderBook=true, acceptingOrders=true; two highest-volume binary markets at discovery time. | Corrective comparison sample reused the four explicit token IDs from preserved run 20260826T015136.607229Z-45c482c8; the original selection provenance was public Gamma active=true, closed=false, enableOrderBook=true, acceptingOrders=true on 2026-08-26. |
| Initial dump subscription field | `true` (explicit) | omitted; documented default relied upon |
| WebSocket level subscription field | `2` (explicit) | omitted; documented default relied upon |
| Subscription control | not recorded | Minimal current first-party example payload; optional initial_dump, level, and custom_feature_enabled fields omitted so documented server defaults, rather than explicit probe controls, govern the run. |
| Subscription fields | `[]` | `["assets_ids","type"]` |
| Subscription payload | `{}` | `{"assets_ids":["35198549486569600595408965368290795524428922595732253580266052028158040373233","29633826701713552120011280893309822625942861174174976276686142480636225243753","49869160041323840584812036178393008034046781140245590791017996532351395946894","39652532623602170846456682951463932352134896182641898905432425437612057868317"],"type":"market"}` |
| Raw evidence path | `outputs\market-feed-contract\20260826T015136.607229Z-45c482c8` | `outputs\market-feed-contract\20260826T165055.566599Z-84d473ac` |
| Raw capture boundary | WebSocket application text/bytes after protocol decompression | WebSocket application text/bytes after protocol decompression |
| Software provenance | `{"git_commit":"09c30078c0f645d9b8d796e51f7dd57487661376","git_dirty":false,"httpx":"0.28.1","probe_version":"0.1.0","python":"3.12.4","websockets":"16.0"}` | `{"git_commit":"ad1ed60e70efd1f132c6ebb390804af6baa1f4af","git_dirty":false,"httpx":"0.28.1","probe_version":"0.2.0","python":"3.12.4","websockets":"16.0"}` |
| Offline test command | `python -m unittest discover -s tests/probes/market_feed_contract -p 'test_*.py' -v` | `python -m unittest discover -s tests/probes/market_feed_contract -p 'test_*.py' -v` |
| Offline test result | 38 tests passed in 0.454s on Python 3.12.4 | 59 tests passed in 1.142s on Python 3.12.4 |
| Baseline run ID | `not recorded` | `20260826T015136.607229Z-45c482c8` |
| Baseline summary path | `not recorded` | `outputs\market-feed-contract\20260826T015136.607229Z-45c482c8\summary.json` |
| Baseline summary SHA-256 | `not recorded` | `6a61bc26796451f154b2bc041f989654ca5ffb51b6c79868316f31ecc44f7ded` |

Both summaries are reported with their own provenance. A baseline pointer in the corrective run is a lineage reference, not permission to merge or replace the historical evidence.

## Raw evidence manifests

### Historical/original run

Run directory: `outputs\market-feed-contract\20260826T015136.607229Z-45c482c8`.

Record counts: `{"control_events":231,"rest_responses":45,"websocket_frames":234}`.

Manifest algorithm: `sha256`.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `control-events.jsonl` | 99154 | `b1def99d4f6f9f1bdc5076cf29b229e79ded7f272d7220af25a3b466a5481a9e` |
| `raw-rest-book.jsonl` | 229741 | `1c5203639a753c5c69b7d3b0ca81a564bad71c4724b8f7d38004b6056e6b78b3` |
| `raw-websocket.jsonl` | 439616 | `70b54a3a693595300b20fde97d0125b6c235803cc43fcff255e2b1cf69097453` |
| `resolved-config.json` | 1955 | `79137617b3cff5655fcf91faee94404c97e00b09937b2e3624f0cc831e2c8861` |
| `run-config.json` | 1955 | `79137617b3cff5655fcf91faee94404c97e00b09937b2e3624f0cc831e2c8861` |

### Corrective run

Run directory: `outputs\market-feed-contract\20260826T165055.566599Z-84d473ac`.

Record counts: `{"control_events":86,"rest_responses":116,"websocket_frames":57}`.

Manifest algorithm: `sha256`.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `control-events.jsonl` | 36235 | `48ad24d03278d8b84dfed280a64b301c1a98a1ab141364e4b8b6f65122e11259` |
| `raw-rest-book.jsonl` | 396510 | `343acf92299cee6a64f34922fe8f5a8cd7c27c61eac5177f0d6442affecb486b` |
| `raw-websocket.jsonl` | 105604 | `2ff5af66cf1b2d7ec9137c965c1d018cc26fcc99cb054c9eb7a810dddf03458a` |
| `resolved-config.json` | 3207 | `2fbdd6787ca7fc4ea47aa1bbf45f30d329b08ce5f546e9b1474fd9d7f2b937e7` |
| `run-config.json` | 3207 | `2fbdd6787ca7fc4ea47aa1bbf45f30d329b08ce5f546e9b1474fd9d7f2b937e7` |

## Q1-Q8 status delta

Statuses are bounded to each run's eligible evidence. A change in status is not a retroactive rewrite of the historical result.

| Question | Historical/original status | Corrective status | Delta |
| --- | --- | --- | --- |
| Q1 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q2 | **CONFIRMED** | **UNRESOLVED** | `CONFIRMED -> UNRESOLVED` |
| Q3 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q4 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q5 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q6 | **CONFIRMED** | **UNRESOLVED** | `CONFIRMED -> UNRESOLVED` |
| Q7 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q8 | **CONFIRMED** | **UNRESOLVED** | `CONFIRMED -> UNRESOLVED` |

## Per-question evidence, differences, and applicability

Counterexamples and exclusions are reproduced in the later significant-evidence section with raw ingest-sequence lineage where available.

### Q1 — Current event schema

- Historical/original: **UNRESOLVED** — 224 logical events across 2 event types; 0 candidate ordering-field presences.
- Corrective: **UNRESOLVED** — 43 logical events across 2 event types; 0 candidate ordering-field presences.
- Difference/method: The schema inventory method is unchanged; the corrective run supplies an independent current-production sample.
- Applicability limit: Field absence is bounded to the observed sample and does not prove that a field cannot appear.

### Q2 — Initial dump/reconnect

- Historical/original: **CONFIRMED** — 2 sessions / 8 token-session observations; book first in 8; later delta exercised in 8; clean controlled reconnect=not recorded.
- Corrective: **UNRESOLVED** — 2 sessions / 8 token-session observations; book first in 4; later delta exercised in 4; clean controlled reconnect=true.
- Difference/method: The historical run explicitly sent optional controls. The corrective run sends only `assets_ids` and `type`, relying on currently documented defaults.
- Applicability limit: The result applies only to the named tokens, sessions, subscription payload, and clean reconnect; it is not a universal reconnect guarantee.

### Q3 — Price-level update semantics

- Historical/original: **UNRESOLVED** — observed/applied/excluded changes=432/232/200; BBO exact/genuine mismatch=228/4; validated non-zero/zero=192/0; empty-side candidates confirmed/observed=0/0; superseded before validation=0.
- Corrective: **UNRESOLVED** — observed/applied/excluded changes=78/78/0; BBO exact/genuine mismatch=78/0; validated non-zero/zero=48/0; empty-side candidates confirmed/observed=0/0; superseded before validation=18.
- Difference/method: The corrective analyzer separates locally empty-side numeric 0/1 candidates from genuine mismatches, requires targeted aligned REST confirmation, and prevents superseded updates from receiving duplicate validation credit.
- Applicability limit: Only applied, non-superseded, eligible updates and explicitly aligned REST evidence support the label; candidate numeric boundaries are not universal protocol rules.

### Q4 — Multiple updates per frame

- Historical/original: **UNRESOLVED** — frames=216; multi-entry=216; same-asset=0; repeated key=0; order-sensitive=0.
- Corrective: **UNRESOLVED** — frames=39; multi-entry=39; same-asset=0; repeated key=0; order-sensitive=0.
- Difference/method: The corrective analyzer distinguishes multiple entries for different assets from same-asset ordering ambiguity.
- Applicability limit: A zero count is non-observation, not proof that a frame shape cannot occur.

### Q5 — Hash semantics

- Historical/original: **UNRESOLVED** — WS book matches/attempts=0/0; post-change=0/0; REST diagnostic=45/45.
- Corrective: **UNRESOLVED** — WS book matches/attempts=0/0; post-change=0/0; REST diagnostic=58/58.
- Difference/method: The same official hash algorithm and no-fabricated-input rule apply to both runs.
- Applicability limit: Only the cited first-party algorithm and payloads containing all its required inputs were eligible; no alternative hash meaning was inferred.

### Q6 — REST /book comparison

- Historical/original: **CONFIRMED** — successful/total REST responses=45/45; stable aligned exact/mismatch=15/0; unaligned diagnostics=30.
- Corrective: **UNRESOLVED** — successful/total REST responses=58/116; stable aligned exact/mismatch=52/2; unaligned diagnostics=4.
- Difference/method: The stable-window rule is retained; corrective candidate-targeted GETs add diagnostics without turning asynchronous mismatches into WebSocket failures.
- Applicability limit: REST and WebSocket are asynchronous; mismatches remain diagnostics unless the declared stable request window exists.

### Q7 — Optional WebSocket book metadata

- Historical/original: **UNRESOLVED** — 8 full-book events; optional field presence={"last_trade_price":8,"min_order_size":0,"neg_risk":0,"tick_size":8}.
- Corrective: **UNRESOLVED** — 4 full-book events; optional field presence={"last_trade_price":4,"min_order_size":0,"neg_risk":0,"tick_size":4}.
- Difference/method: The frequency method is unchanged; differences are sample observations only.
- Applicability limit: Presence frequencies apply only to observed full-book frames; absence is not universal.

### Q8 — Timestamp and processing-order behavior

- Historical/original: **CONFIRMED** — timestamped events=224; source regressions=0; same-timestamp groups=6; future-source=0; ingest/monotonic receive-order regressions=not recorded/not recorded; delay samples=224.
- Corrective: **UNRESOLVED** — timestamped events=43; source regressions=0; same-timestamp groups=10; future-source=0; ingest/monotonic receive-order regressions=0/0; delay samples=43.
- Difference/method: The numeric provenance is retained, but the corrective report labels it as contaminated source-to-processing delay and adds explicit ingest/monotonic processing-order checks.
- Applicability limit: Receive-order counters describe local probe processing only, do not prove gap-free delivery, and the delay distribution is not a venue/network latency estimate.


## Evidence classification

### Established

- First-party material establishes that the public Market WebSocket carries full `book` snapshots and `price_change` level updates, and explicitly describes zero-size changes as level removals and non-zero `size` as the new aggregate size.
- The current first-party schema documents `initial_dump` and `level` as optional subscription fields with defaults. Current official TypeScript bindings separately document an empty string as the raw absent-value form for optional best bid/ask decimals.
- Each run's identity, software provenance, raw-evidence path, record counts, and manifest digests establish what these named artifacts contain; they do not establish universal venue behavior.

### Suggested

- Numeric `best_ask=1` or `best_bid=0` coinciding with a locally empty side is suggested by first-party examples and bounded live observations, but no first-party source found explicitly defines those numbers as empty-side sentinels.
- A high best-price match rate is supportive but cannot independently establish aggregate replacement semantics or completeness.

### Project-derived

- Stable-window REST alignment, discriminating-update eligibility, exclusion rules, and the historical/corrective comparison are project-defined methods.
- Any interpretation of observed `best_ask=1` or `best_bid=0` as an empty side is limited to directly aligned candidates in the named run. This report does not claim that numeric 0/1 values are universal empty-side protocol sentinels.

## Current documentation discrepancy and subscription control

The current first-party Market WebSocket documentation describes `initial_dump` and `level` as optional subscription fields with documented defaults. The historical run sent both fields explicitly; the corrective configuration records whether it omitted them and relied on those defaults. This differs from the historical report's premise that these controls were undocumented. It is a documentation-version discrepancy and experimental control, not evidence that the same omission semantics applied universally or at every historical point.

Current reference: https://docs.polymarket.com/api-reference/wss/market

## Q8 measurement boundary

probe-observed source-to-processing delay contaminated by local clock offset, event-loop scheduling, socket/library buffering, backpressure, and synchronous flush/fsync durable-persistence overhead; not a venue/network latency estimate.

The two runs may still be compared for source-timestamp ordering diagnostics and for their separately recorded contaminated processing-delay distributions; neither distribution estimates venue or network latency.

| Metric | Historical/original run | Corrective run |
| --- | ---: | ---: |
| Samples | 224 | 43 |
| Minimum (ms) | 733.207 | 250.438 |
| P50 (ms) | 752.537 | 305.689 |
| P95 (ms) | 1294.112 | 6888.405 |
| Maximum (ms) | 9204.928 | 25509.132 |

## Significant counterexamples, exclusions, and errors

### Historical/original run

- Repeated raw-payload hashes retained: **15**. This all-frame count alone does not establish duplicate data events.
- Q3 exclusions preserved: **200** of **432** observed price-change entries were excluded; **232** were applied. Conclusions must not be generalized across the excluded evidence.
- Q3 counterexample: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.29","side":"BUY","size":"0"}`
- Q3 counterexample: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","change_index":1,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_bid":{"derived":null,"observed":"0"}},"multi_entry_semantics_ambiguous":true,"price":"0.71","side":"SELL","size":"0"}`
- Q3 counterexample: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":239,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.31","side":"BUY","size":"0"}`
- Q3 counterexample: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","change_index":1,"frame_entry_count":2,"ingest_sequence":239,"mismatches":{"best_bid":{"derived":null,"observed":"0"}},"multi_entry_semantics_ambiguous":true,"price":"0.69","side":"SELL","size":"0"}`
- Parser/operational error `best_bid_ask_mismatch`: **4** occurrence(s).
- Parser/operational error `price_change_without_valid_book`: **200** occurrence(s).
- Representative `best_bid_ask_mismatch` error: `{"code":"best_bid_ask_mismatch","details":{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.29","side":"BUY","size":"0"},"event_type":"price_change","ingest_sequence":11,"message":"reconstructed best bid/ask did not match event values"}`
- Representative `best_bid_ask_mismatch` error: `{"code":"best_bid_ask_mismatch","details":{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","change_index":1,"frame_entry_count":2,"ingest_sequence":11,"mismatches":{"best_bid":{"derived":null,"observed":"0"}},"multi_entry_semantics_ambiguous":true,"price":"0.71","side":"SELL","size":"0"},"event_type":"price_change","ingest_sequence":11,"message":"reconstructed best bid/ask did not match event values"}`
- Representative `best_bid_ask_mismatch` error: `{"code":"best_bid_ask_mismatch","details":{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","change_index":0,"frame_entry_count":2,"ingest_sequence":239,"mismatches":{"best_ask":{"derived":null,"observed":"1"}},"multi_entry_semantics_ambiguous":true,"price":"0.31","side":"BUY","size":"0"},"event_type":"price_change","ingest_sequence":239,"message":"reconstructed best bid/ask did not match event values"}`
- `best_bid_ask_mismatch`: 1 additional traceable instance(s) remain in the evidence summary.
- Representative `price_change_without_valid_book` error: `{"code":"price_change_without_valid_book","details":{},"event_type":"price_change","ingest_sequence":14,"message":"cannot apply change for uninitialized/invalid asset 49869160041323840584812036178393008034046781140245590791017996532351395946894"}`
- Representative `price_change_without_valid_book` error: `{"code":"price_change_without_valid_book","details":{},"event_type":"price_change","ingest_sequence":14,"message":"cannot apply change for uninitialized/invalid asset 39652532623602170846456682951463932352134896182641898905432425437612057868317"}`
- Representative `price_change_without_valid_book` error: `{"code":"price_change_without_valid_book","details":{},"event_type":"price_change","ingest_sequence":24,"message":"cannot apply change for uninitialized/invalid asset 49869160041323840584812036178393008034046781140245590791017996532351395946894"}`
- `price_change_without_valid_book`: 197 additional traceable instance(s) remain in the evidence summary.

### Corrective run

- Repeated raw-payload hashes retained: **15**. This all-frame count alone does not establish duplicate data events.
- Q3 exclusions preserved: **0** of **78** observed price-change entries were excluded; **78** were applied. Conclusions must not be generalized across the excluded evidence.
- Q3 updates superseded before independent validation: **18**.
- Q3 superseded-validation exclusion: `{"ingest_sequence":225,"kind":"nonzero_replacement","observed_size":"55080.8","previous_size":"55167.8","price":"0.84","side":"BUY","superseding_ingest_sequence":231}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":225,"kind":"nonzero_replacement","observed_size":"55080.8","previous_size":"55167.8","price":"0.16","side":"SELL","superseding_ingest_sequence":231}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":226,"kind":"nonzero_replacement","observed_size":"69610","previous_size":"69697","price":"0.83","side":"BUY","superseding_ingest_sequence":232}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":226,"kind":"nonzero_replacement","observed_size":"69610","previous_size":"69697","price":"0.17","side":"SELL","superseding_ingest_sequence":232}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":227,"kind":"nonzero_replacement","observed_size":"51085.46","previous_size":"51172.46","price":"0.13","side":"BUY","superseding_ingest_sequence":234}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":227,"kind":"nonzero_replacement","observed_size":"51085.46","previous_size":"51172.46","price":"0.87","side":"SELL","superseding_ingest_sequence":234}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":231,"kind":"nonzero_replacement","observed_size":"55027.8","previous_size":"55080.8","price":"0.84","side":"BUY","superseding_ingest_sequence":235}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":231,"kind":"nonzero_replacement","observed_size":"55027.8","previous_size":"55080.8","price":"0.16","side":"SELL","superseding_ingest_sequence":235}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":234,"kind":"nonzero_replacement","observed_size":"51065.46","previous_size":"51085.46","price":"0.13","side":"BUY","superseding_ingest_sequence":236}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":234,"kind":"nonzero_replacement","observed_size":"51065.46","previous_size":"51085.46","price":"0.87","side":"SELL","superseding_ingest_sequence":236}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":235,"kind":"nonzero_replacement","observed_size":"54997.8","previous_size":"55027.8","price":"0.84","side":"BUY","superseding_ingest_sequence":237}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":235,"kind":"nonzero_replacement","observed_size":"54997.8","previous_size":"55027.8","price":"0.16","side":"SELL","superseding_ingest_sequence":237}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":236,"kind":"nonzero_replacement","observed_size":"51085.46","previous_size":"51065.46","price":"0.13","side":"BUY","superseding_ingest_sequence":248}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":236,"kind":"nonzero_replacement","observed_size":"51085.46","previous_size":"51065.46","price":"0.87","side":"SELL","superseding_ingest_sequence":248}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":237,"kind":"nonzero_replacement","observed_size":"55027.8","previous_size":"54997.8","price":"0.84","side":"BUY","superseding_ingest_sequence":249}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":237,"kind":"nonzero_replacement","observed_size":"55027.8","previous_size":"54997.8","price":"0.16","side":"SELL","superseding_ingest_sequence":249}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":249,"kind":"nonzero_replacement","observed_size":"54997.8","previous_size":"55027.8","price":"0.84","side":"BUY","superseding_ingest_sequence":255}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":249,"kind":"nonzero_replacement","observed_size":"54997.8","previous_size":"55027.8","price":"0.16","side":"SELL","superseding_ingest_sequence":255}`
- Q3 bounded empty-side interpretation: status **UNRESOLVED**, observed candidates **0**, confirmed candidates **0**, unresolved candidates **0**.
- Q2 counterexample: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","first_state_event":null,"session_id":"session-001"}`
- Q2 counterexample: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","first_book_ingest_sequence":null,"first_state_event":null,"kind":"no_delta_observed_after_fresh_book","session_id":"session-001"}`
- Q2 counterexample: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","first_state_event":null,"session_id":"session-001"}`
- Q2 counterexample: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","first_book_ingest_sequence":null,"first_state_event":null,"kind":"no_delta_observed_after_fresh_book","session_id":"session-001"}`
- Q2 counterexample: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","first_state_event":null,"session_id":"session-002"}`
- Q2 counterexample: `{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317","first_book_ingest_sequence":null,"first_state_event":null,"kind":"no_delta_observed_after_fresh_book","session_id":"session-002"}`
- Q2 counterexample: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","first_state_event":null,"session_id":"session-002"}`
- Q2 counterexample: `{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894","first_book_ingest_sequence":null,"first_state_event":null,"kind":"no_delta_observed_after_fresh_book","session_id":"session-002"}`
- Q6 counterexample: `{"ingest_sequence":6,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":9,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":13,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":15,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":22,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":24,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":29,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":31,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":37,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":39,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":45,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":47,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":51,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":53,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":59,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":61,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":65,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":67,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":73,"kind":"non_2xx","status":404}`
- Q6 counterexample: `{"ingest_sequence":75,"kind":"non_2xx","status":404}`
- Parser/operational error `rest_non_2xx`: **58** occurrence(s).
- Representative `rest_non_2xx` error: `{"code":"rest_non_2xx","details":{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894"},"event_type":null,"ingest_sequence":6,"message":"REST /book returned HTTP 404"}`
- Representative `rest_non_2xx` error: `{"code":"rest_non_2xx","details":{"asset_id":"39652532623602170846456682951463932352134896182641898905432425437612057868317"},"event_type":null,"ingest_sequence":9,"message":"REST /book returned HTTP 404"}`
- Representative `rest_non_2xx` error: `{"code":"rest_non_2xx","details":{"asset_id":"49869160041323840584812036178393008034046781140245590791017996532351395946894"},"event_type":null,"ingest_sequence":13,"message":"REST /book returned HTTP 404"}`
- `rest_non_2xx`: 55 additional traceable instance(s) remain in the evidence summary.

## Scope limitations

- Both runs are bounded samples; non-observation does not establish impossibility.
- Neither a short successful connection nor matching top-of-book values proves gap-free delivery or full-depth correctness.
- Historical exclusions remain exclusions and cannot be converted into eligible evidence by the corrective run.
- No result here is a trading-performance result.

## Safety boundary

Both runs used only the fixed public Market WebSocket, `PING` heartbeats, and public `GET /book`. No authenticated endpoint, user WebSocket, API credential, wallet, signing key, order construction, simulation, placement, cancellation, live trading, or paper trading was used.

## Deviations from IP-001

None. The corrective work remains a bounded probe using the packet-authorized explicit-token mode. It does not implement a production collector, replay engine, or any code under `src/`.

## First-party references applied

- https://docs.polymarket.com/api-reference/wss/market
- https://github.com/Polymarket/agent-skills/blob/main/websocket.md
- https://github.com/Polymarket/ts-sdk/blob/main/packages/bindings/src/subscriptions/clob.ts
- https://docs.polymarket.com/api-reference/market-data/get-order-book
- https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/utilities.py
