# Cricsheet test fixtures

Real match files copied unchanged from Cricsheet downloads fetched on 2026-10-26.

**Attribution:** data from [Cricsheet](https://cricsheet.org/), made available under the
[Open Data Commons Attribution License (ODC-BY 1.0)](https://opendatacommons.org/licenses/by/1-0/).

| File | Competition | Edge case covered |
|---|---|---|
| `ipl/1082591.json` | IPL 2017 | Ordinary completed match; wides and no-balls (checks `actual_delivery`) |
| `ipl/1082625.json` | IPL 2017 | Tie decided by a super over (`outcome.eliminator`, `super_over` innings) |
| `ipl/1082648.json` | IPL 2017 | D/L method result; match listed with two dates |
| `ipl/1136564.json` | IPL 2018 | `miscounted_overs` on an innings |
| `ipl/1178424.json` | IPL 2019 | No result |
| `ipl/1359475.json` | IPL 2023 | Impact Player substitutions (`replacements.match`, reason `impact_player`) |
| `ipl/1359519.json` | IPL 2023 | No result after only one innings; an Impact Player used by a team that never batted |
| `ipl/1370352.json` | IPL 2023 | Concussion substitute |
| `ipl/335982.json` | IPL 2008 | Season labelled `"2007/08"`; the first IPL match |
| `t20i/1496587.json` | T20I 2026 | Schema version 1.3.0 (`registry.venues`) |
| `t20i/237242.json` | T20I 2006 | Tie decided by a bowl-out (`outcome.bowl_out`) |
| `t20i/1268191.json` | T20I 2021 | Result `Awarded`; innings `penalty_runs` |
| `t20i/1383095.json` | T20I 2023 | Two wickets on one delivery |
| `t20i/1452625.json` | T20I 2024 | A team listing only 10 players |
| `t20i/1115800.json` | T20I 2018 | `absent_hurt` on an innings; season labelled `"2017/18"` |

Schema versions older than 1.2.0 are not served by Cricsheet any more. Tests build them by
removing the newer fields from these files (see `tests/ingest/test_parse.py`).
