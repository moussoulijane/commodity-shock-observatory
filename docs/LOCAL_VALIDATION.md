# Local validation — 25 October 2026

The aggregate reference is [local-reference.json](evidence/local-reference.json). Raw source observations, origin-level forecasts and runtime lake/checkpoints remain local and ignored by Git. Reproduce them using the commands in the French guide.

| Check | Observed result | Boundary |
| --- | --- | --- |
| Source coverage | 13,796 observations: Brent 6,491; WTI 6,342; gold/copper/wheat 321 each | Downloaded snapshots from 2000 onward, daily / monthly frequencies |
| File → Kafka → Spark → serving parity | Zero missing observations; zero price mismatches | Snapshot parity, not forecast accuracy |
| SIGKILL recovery | 81 / 81 unique synthetic observations; eight duplicate messages do not add prices; two invalid/conflicting messages quarantined | One local Spark process; persistent checkpoints; no broker HA claim |
| Event-time aggregates | Counts / mean / minimum / maximum agree with an independent aggregate of recent synthetic deliveries | Operational delivery windows include valid re-deliveries |
| Load exercise | 5,000 synthetic events, 42.47 s until all are externally visible, about 117.7 events/s; visibility p95 42.47 s | Single broker, Spark local[2], two shuffle partitions, 2,000 offsets/trigger; includes production, polling and serving work |
| UI | Instrument selection, date ranges, alert direction/instrument filters, research results, empty LIVE state and mobile layout checked locally | Not an accessibility certification or cross-browser matrix |

## Economic findings

On 96 expanding-window evaluation origins per horizon, persistence has MAE **0.551 pp at one month** and **0.859 pp at three months**. Oil-augmented ridge gives **0.556 pp** and **0.925 pp**; XGBoost gives **0.871 pp** and **1.198 pp**. This experiment does not show an improvement over persistence. The data are today's revised snapshots with assumed publication lags: these are exploratory findings, not validated real-time forecasts. No causal conclusion follows.

The robust detector produces 180 historical alerts across the five instruments under the fixed rule. Their economic correctness is not independently labelled. A simple synthetic sensitivity experiment detects 11 inserted jumps; that controlled result is not an estimate of real-world precision or recall.

## What remains conditional

The initial-release ALFRED path requires a personal FRED API key and has not been exercised against that API in this run. The Twelve Data intraday adapter requires an entitled account and has not been validated live. The CI workflow is configured but has not run on GitHub because this project has not been published.

## Performance interpretation

The local batch size and full-history pandas signal rebuild are material bottlenecks. This implementation demonstrates inspectable contracts, deterministic serving, a real Kafka/Spark path and measured recovery. It does not establish that a distributed stack is economically necessary for five daily/monthly instruments. Before pursuing sustained high-volume intraday processing, benchmark incremental signal state and bulk sinks under the intended workload.
