# Market Feed Contract Probe: Historical and Corrective Evidence

Status: **comparative bounded read-only research report**

The historical run remains immutable evidence. The corrective run is additional evidence collected under a revised probe; it does not overwrite the historical run, its exclusions, counterexamples, status labels, or manifest.

## Run identities and provenance

| Property | Historical/original run | Corrective run |
| --- | --- | --- |
| Run ID | `20260826T015136.607229Z-45c482c8` | `20260826T165706.583173Z-309ba088` |
| UTC start | `2026-08-26T01:51:36.703084Z` | `2026-08-26T16:57:06.888976Z` |
| UTC end | `2026-08-26T01:54:39.070920Z` | `2026-08-26T17:00:12.219787Z` |
| Requested duration (seconds) | 180 | 180 |
| Actual duration (seconds) | 182.375 | 185.328 |
| REST interval (seconds) | 15 | 5 |
| Controlled reconnects | 1 | 1 |
| Sample/token IDs | `["35198549486569600595408965368290795524428922595732253580266052028158040373233","29633826701713552120011280893309822625942861174174976276686142480636225243753","49869160041323840584812036178393008034046781140245590791017996532351395946894","39652532623602170846456682951463932352134896182641898905432425437612057868317"]` | `["62061355308294300419793351866044847402806222151899039505296087876415516899191","1103341092391959166815561205360597691836301424464685361195497744684819483679","23545193122217391289335133742859160842475987880562465754342378273033942179195","111344475342969784116702975577723982549674037196345086059563486202499238579830"]` |
| Sample provenance | Selected 2026-08-26 from the public Gamma /markets response: active=true, closed=false, enableOrderBook=true, acceptingOrders=true; two highest-volume binary markets at discovery time. | Corrective active sample selected 2026-08-26 UTC from bounded public Gamma active=true, closed=false, enableOrderBook=true, acceptingOrders=true results; included the high-volume Hormuz binary pair and the Ox Alpha binary pair, whose contemporaneous public REST books exposed complementary empty ask/bid sides. |
| Initial dump subscription field | `true` (explicit) | omitted; documented default relied upon |
| WebSocket level subscription field | `2` (explicit) | omitted; documented default relied upon |
| Subscription control | not recorded | Minimal current first-party example payload; optional initial_dump, level, and custom_feature_enabled fields omitted so documented server defaults, rather than explicit probe controls, govern the run. |
| Subscription fields | `[]` | `["assets_ids","type"]` |
| Subscription payload | `{}` | `{"assets_ids":["62061355308294300419793351866044847402806222151899039505296087876415516899191","1103341092391959166815561205360597691836301424464685361195497744684819483679","23545193122217391289335133742859160842475987880562465754342378273033942179195","111344475342969784116702975577723982549674037196345086059563486202499238579830"],"type":"market"}` |
| Raw evidence path | `outputs\market-feed-contract\20260826T015136.607229Z-45c482c8` | `outputs\market-feed-contract\20260826T165706.583173Z-309ba088` |
| Raw capture boundary | WebSocket application text/bytes after protocol decompression | WebSocket application text/bytes after protocol decompression |
| Software provenance | `{"git_commit":"09c30078c0f645d9b8d796e51f7dd57487661376","git_dirty":false,"httpx":"0.28.1","probe_version":"0.1.0","python":"3.12.4","websockets":"16.0"}` | `{"git_commit":"b8223fbfa56e7f5fece0823ae69ff69af5dc1a25","git_dirty":false,"httpx":"0.28.1","probe_version":"0.2.0","python":"3.12.4","websockets":"16.0"}` |
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

Run directory: `outputs\market-feed-contract\20260826T165706.583173Z-309ba088`.

Record counts: `{"control_events":28,"rest_responses":121,"websocket_frames":90}`.

Manifest algorithm: `sha256`.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `control-events.jsonl` | 9668 | `a5b276dda6dfa1980414cf5579092f449694c47a58f72ed146902182ab7e0e03` |
| `raw-rest-book.jsonl` | 573275 | `ea3589f000b273e885a60dccd9c2b035bfca920f48c1946133cdb71e1f8c6c1f` |
| `raw-websocket.jsonl` | 182545 | `9f5020c7e8956a20ce76a30100295fa912d2a538979c8a5e4a9924d10e4f7ca4` |
| `resolved-config.json` | 3265 | `f87e6dac9b042a504149496a10821a5dc43e5b8c103735a4eed19253202c82ca` |
| `run-config.json` | 3265 | `f87e6dac9b042a504149496a10821a5dc43e5b8c103735a4eed19253202c82ca` |

## Q1-Q8 status delta

Statuses are bounded to each run's eligible evidence. A change in status is not a retroactive rewrite of the historical result.

| Question | Historical/original status | Corrective status | Delta |
| --- | --- | --- | --- |
| Q1 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q2 | **CONFIRMED** | **CONFIRMED** | `UNCHANGED` |
| Q3 | **UNRESOLVED** | **CONFIRMED** | `UNRESOLVED -> CONFIRMED` |
| Q4 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q5 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q6 | **CONFIRMED** | **CONFIRMED** | `UNCHANGED` |
| Q7 | **UNRESOLVED** | **UNRESOLVED** | `UNCHANGED` |
| Q8 | **CONFIRMED** | **CONFIRMED** | `UNCHANGED` |

## Per-question evidence, differences, and applicability

Counterexamples and exclusions are reproduced in the later significant-evidence section with raw ingest-sequence lineage where available.

### Q1 — Current event schema

- Historical/original: **UNRESOLVED** — 224 logical events across 2 event types; 0 candidate ordering-field presences.
- Corrective: **UNRESOLVED** — 80 logical events across 3 event types; 0 candidate ordering-field presences.
- Difference/method: The schema inventory method is unchanged; the corrective run supplies an independent current-production sample.
- Applicability limit: Field absence is bounded to the observed sample and does not prove that a field cannot appear.

### Q2 — Initial dump/reconnect

- Historical/original: **CONFIRMED** — 2 sessions / 8 token-session observations; book first in 8; later delta exercised in 8; clean controlled reconnect=not recorded.
- Corrective: **CONFIRMED** — 2 sessions / 8 token-session observations; book first in 8; later delta exercised in 8; clean controlled reconnect=true.
- Difference/method: The historical run explicitly sent optional controls. The corrective run sends only `assets_ids` and `type`, relying on currently documented defaults.
- Applicability limit: The result applies only to the named tokens, sessions, subscription payload, and clean reconnect; it is not a universal reconnect guarantee.

### Q3 — Price-level update semantics

- Historical/original: **UNRESOLVED** — observed/applied/excluded changes=432/232/200; BBO exact/genuine mismatch=228/4; validated non-zero/zero=192/0; empty-side candidates confirmed/observed=0/0; superseded before validation=0.
- Corrective: **CONFIRMED** — observed/applied/excluded changes=138/138/0; BBO exact/genuine mismatch=130/0; validated non-zero/zero=89/3; empty-side candidates confirmed/observed=8/8; superseded before validation=36.
- Difference/method: The corrective analyzer separates locally empty-side numeric 0/1 candidates from genuine mismatches, requires targeted aligned REST confirmation, and prevents superseded updates from receiving duplicate validation credit.
- Applicability limit: Only applied, non-superseded, eligible updates and explicitly aligned REST evidence support the label; candidate numeric boundaries are not universal protocol rules.

### Q4 — Multiple updates per frame

- Historical/original: **UNRESOLVED** — frames=216; multi-entry=216; same-asset=0; repeated key=0; order-sensitive=0.
- Corrective: **UNRESOLVED** — frames=69; multi-entry=69; same-asset=0; repeated key=0; order-sensitive=0.
- Difference/method: The corrective analyzer distinguishes multiple entries for different assets from same-asset ordering ambiguity.
- Applicability limit: A zero count is non-observation, not proof that a frame shape cannot occur.

### Q5 — Hash semantics

- Historical/original: **UNRESOLVED** — WS book matches/attempts=0/0; post-change=0/0; REST diagnostic=45/45.
- Corrective: **UNRESOLVED** — WS book matches/attempts=0/0; post-change=0/0; REST diagnostic=121/121.
- Difference/method: The same official hash algorithm and no-fabricated-input rule apply to both runs.
- Applicability limit: Only the cited first-party algorithm and payloads containing all its required inputs were eligible; no alternative hash meaning was inferred.

### Q6 — REST /book comparison

- Historical/original: **CONFIRMED** — successful/total REST responses=45/45; stable aligned exact/mismatch=15/0; unaligned diagnostics=30.
- Corrective: **CONFIRMED** — successful/total REST responses=121/121; stable aligned exact/mismatch=114/0; unaligned diagnostics=7.
- Difference/method: The stable-window rule is retained; corrective candidate-targeted GETs add diagnostics without turning asynchronous mismatches into WebSocket failures.
- Applicability limit: REST and WebSocket are asynchronous; mismatches remain diagnostics unless the declared stable request window exists.

### Q7 — Optional WebSocket book metadata

- Historical/original: **UNRESOLVED** — 8 full-book events; optional field presence={"last_trade_price":8,"min_order_size":0,"neg_risk":0,"tick_size":8}.
- Corrective: **UNRESOLVED** — 10 full-book events; optional field presence={"last_trade_price":8,"min_order_size":0,"neg_risk":0,"tick_size":8}.
- Difference/method: The frequency method is unchanged; differences are sample observations only.
- Applicability limit: Presence frequencies apply only to observed full-book frames; absence is not universal.

### Q8 — Timestamp and processing-order behavior

- Historical/original: **CONFIRMED** — timestamped events=224; source regressions=0; same-timestamp groups=6; future-source=0; ingest/monotonic receive-order regressions=not recorded/not recorded; delay samples=224.
- Corrective: **CONFIRMED** — timestamped events=80; source regressions=0; same-timestamp groups=16; future-source=0; ingest/monotonic receive-order regressions=0/0; delay samples=80.
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
| Samples | 224 | 80 |
| Minimum (ms) | 733.207 | 244.462 |
| P50 (ms) | 752.537 | 276.369 |
| P95 (ms) | 1294.112 | 4567.347 |
| Maximum (ms) | 9204.928 | 739343.28 |

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
- Q3 exclusions preserved: **0** of **138** observed price-change entries were excluded; **138** were applied. Conclusions must not be generalized across the excluded evidence.
- Q3 updates superseded before independent validation: **36**.
- Q3 superseded-validation exclusion: `{"ingest_sequence":16,"kind":"nonzero_replacement","observed_size":"154111.16","previous_size":"154097.16","price":"0.07","side":"BUY","superseding_ingest_sequence":18}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":16,"kind":"nonzero_replacement","observed_size":"154111.16","previous_size":"154097.16","price":"0.93","side":"SELL","superseding_ingest_sequence":18}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":21,"kind":"nonzero_replacement","observed_size":"1395","previous_size":"1795","price":"0.63","side":"BUY","superseding_ingest_sequence":24}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":21,"kind":"nonzero_replacement","observed_size":"1395","previous_size":"1795","price":"0.37","side":"SELL","superseding_ingest_sequence":24}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":23,"kind":"nonzero_replacement","observed_size":"999","previous_size":"4749","price":"0.54","side":"BUY","superseding_ingest_sequence":28}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":22,"kind":"zero_delete","price":"0.49","side":"BUY","superseding_ingest_sequence":30}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":20,"kind":"nonzero_replacement","observed_size":"1999","previous_size":"7751.84","price":"0.44","side":"BUY","superseding_ingest_sequence":31}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":19,"kind":"zero_delete","price":"0.39","side":"BUY","superseding_ingest_sequence":32}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":14,"kind":"nonzero_replacement","observed_size":"566.65","previous_size":"10244.96","price":"0.34","side":"BUY","superseding_ingest_sequence":33}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":12,"kind":"nonzero_replacement","observed_size":"541.66","previous_size":"21635.41","price":"0.24","side":"BUY","superseding_ingest_sequence":34}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":13,"kind":"nonzero_replacement","observed_size":"500","previous_size":"12719.83","price":"0.29","side":"BUY","superseding_ingest_sequence":35}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":35,"kind":"zero_delete","price":"0.29","side":"BUY","superseding_ingest_sequence":36}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":35,"kind":"zero_delete","price":"0.71","side":"SELL","superseding_ingest_sequence":36}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":34,"kind":"zero_delete","price":"0.24","side":"BUY","superseding_ingest_sequence":37}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":34,"kind":"zero_delete","price":"0.76","side":"SELL","superseding_ingest_sequence":37}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":11,"kind":"zero_delete","price":"0.19","side":"BUY","superseding_ingest_sequence":38}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":10,"kind":"nonzero_replacement","observed_size":"13242.85","previous_size":"53019.64","price":"0.14","side":"BUY","superseding_ingest_sequence":41}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":9,"kind":"nonzero_replacement","observed_size":"20432.63","previous_size":"117463.88","price":"0.06","side":"BUY","superseding_ingest_sequence":42}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":71,"kind":"nonzero_replacement","observed_size":"119044.02","previous_size":"154311.16","price":"0.07","side":"BUY","superseding_ingest_sequence":72}`
- Q3 superseded-validation exclusion: `{"ingest_sequence":71,"kind":"nonzero_replacement","observed_size":"119044.02","previous_size":"154311.16","price":"0.93","side":"SELL","superseding_ingest_sequence":72}`
- Q3 bounded empty-side interpretation: status **CONFIRMED**, observed candidates **8**, confirmed candidates **8**, unresolved candidates **0**.
- Q3 empty-side candidate: `{"asset_id":"62061355308294300419793351866044847402806222151899039505296087876415516899191","candidate_id":"134:0:best_ask","change_index":0,"component":"best_ask","derived_value":null,"ingest_sequence":134,"kind":"ask_one_when_empty","price":"0.999","rest_attempts":[{"full_depth_match":true,"ingest_sequence":135,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":135,"rest_validation":"CONFIRMED","session_id":"session-001","side":"BUY","size":"330687.26","state_version":2,"wire_value":"1"}`
- Q3 empty-side candidate: `{"asset_id":"1103341092391959166815561205360597691836301424464685361195497744684819483679","candidate_id":"134:1:best_bid","change_index":1,"component":"best_bid","derived_value":null,"ingest_sequence":134,"kind":"bid_zero_when_empty","price":"0.001","rest_attempts":[{"full_depth_match":true,"ingest_sequence":136,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":136,"rest_validation":"CONFIRMED","session_id":"session-001","side":"SELL","size":"330687.26","state_version":2,"wire_value":"0"}`
- Q3 empty-side candidate: `{"asset_id":"62061355308294300419793351866044847402806222151899039505296087876415516899191","candidate_id":"141:0:best_ask","change_index":0,"component":"best_ask","derived_value":null,"ingest_sequence":141,"kind":"ask_one_when_empty","price":"0.999","rest_attempts":[{"full_depth_match":true,"ingest_sequence":143,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":143,"rest_validation":"CONFIRMED","session_id":"session-001","side":"BUY","size":"330655.96","state_version":3,"wire_value":"1"}`
- Q3 empty-side candidate: `{"asset_id":"1103341092391959166815561205360597691836301424464685361195497744684819483679","candidate_id":"141:1:best_bid","change_index":1,"component":"best_bid","derived_value":null,"ingest_sequence":141,"kind":"bid_zero_when_empty","price":"0.001","rest_attempts":[{"full_depth_match":true,"ingest_sequence":144,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":144,"rest_validation":"CONFIRMED","session_id":"session-001","side":"SELL","size":"330655.96","state_version":3,"wire_value":"0"}`
- Q3 empty-side candidate: `{"asset_id":"1103341092391959166815561205360597691836301424464685361195497744684819483679","candidate_id":"210:0:best_bid","change_index":0,"component":"best_bid","derived_value":null,"ingest_sequence":210,"kind":"bid_zero_when_empty","price":"0.001","rest_attempts":[{"full_depth_match":true,"ingest_sequence":211,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":211,"rest_validation":"CONFIRMED","session_id":"session-002","side":"SELL","size":"330628.73","state_version":5,"wire_value":"0"}`
- Q3 empty-side candidate: `{"asset_id":"62061355308294300419793351866044847402806222151899039505296087876415516899191","candidate_id":"210:1:best_ask","change_index":1,"component":"best_ask","derived_value":null,"ingest_sequence":210,"kind":"ask_one_when_empty","price":"0.999","rest_attempts":[{"full_depth_match":true,"ingest_sequence":212,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":212,"rest_validation":"CONFIRMED","session_id":"session-002","side":"BUY","size":"330628.73","state_version":5,"wire_value":"1"}`
- Q3 empty-side candidate: `{"asset_id":"1103341092391959166815561205360597691836301424464685361195497744684819483679","candidate_id":"219:0:best_bid","change_index":0,"component":"best_bid","derived_value":null,"ingest_sequence":219,"kind":"bid_zero_when_empty","price":"0.001","rest_attempts":[{"full_depth_match":true,"ingest_sequence":221,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":221,"rest_validation":"CONFIRMED","session_id":"session-002","side":"SELL","size":"330655.96","state_version":6,"wire_value":"0"}`
- Q3 empty-side candidate: `{"asset_id":"62061355308294300419793351866044847402806222151899039505296087876415516899191","candidate_id":"219:1:best_ask","change_index":1,"component":"best_ask","derived_value":null,"ingest_sequence":219,"kind":"ask_one_when_empty","price":"0.999","rest_attempts":[{"full_depth_match":true,"ingest_sequence":220,"outcome":"CONFIRMED","relevant_rest_side_empty":true}],"rest_ingest_sequence":220,"rest_validation":"CONFIRMED","session_id":"session-002","side":"BUY","size":"330655.96","state_version":6,"wire_value":"1"}`

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
