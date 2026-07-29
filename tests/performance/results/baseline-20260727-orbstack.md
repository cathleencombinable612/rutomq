# rutomq OrbStack baseline — 2026-07-27

This is a reproducible engineering baseline, not a throughput commitment.
Raw measurements are in `baseline-20260727-orbstack.jsonl`.

## Inputs

- Kafka client: 4.2.0
- Records per case: 10,000
- Payload: 1,024 bytes
- Producers / partitions: 8 / 8
- Agent limit: 1 CPU, 512 MiB each
- Dependencies: fresh PostgreSQL 17 and MinIO per case
- Every produced record was consumed and uniquely validated twice

## Results

| Agents | Agent batch | Flush | records/s | MiB/s | p50 ms | p95 ms | p99 ms | PUTs | PG commit mean ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 MiB | 25 ms | 29,228.9 | 28.544 | 97.150 | 163.498 | 195.824 | 9 | 7.898 |
| 1 | 1 MiB | 250 ms | 12,500.9 | 12.208 | 120.617 | 206.068 | 419.185 | 9 | 5.840 |
| 1 | 8 MiB | 25 ms | 23,610.0 | 23.057 | 156.289 | 216.488 | 237.475 | 5 | 9.543 |
| 1 | 8 MiB | 250 ms | 8,053.3 | 7.865 | 588.041 | 866.247 | 1,112.740 | 4 | 9.598 |
| 3 | 1 MiB | 25 ms | 33,207.8 | 32.430 | 105.231 | 142.746 | 162.722 | 13 | 9.383 |
| 3 | 1 MiB | 250 ms | 6,670.9 | 6.515 | 315.775 | 1,107.635 | 1,344.920 | 17 | 5.120 |
| 3 | 8 MiB | 25 ms | 26,627.3 | 26.003 | 116.806 | 184.640 | 220.980 | 12 | 7.825 |
| 3 | 8 MiB | 250 ms | 5,684.1 | 5.551 | 554.257 | 1,331.101 | 1,337.515 | 16 | 7.330 |

## Observations

- The best measured case was 3 Agents, 1 MiB, and 25 ms: 33.2k records/s
  with 142.7 ms p95. It is a point measurement on this machine.
- Every 25 ms case outperformed its matching 250 ms case for both throughput
  and acknowledgement latency in this short, concurrent workload.
- Three Agents improved the 1 MiB / 25 ms case by about 13.6%, but did not
  scale linearly because PostgreSQL and MinIO were shared and the workload was
  only 10,000 records.
- Agent-local batching fragments uploads as Agents increase: comparable
  3-Agent cases made more PUTs than 1-Agent cases. Larger Agent batches reduced
  PUT count in the 1-Agent cases.
- PostgreSQL metadata-commit means remained between 5.1 and 9.6 ms. The larger
  end-to-end differences therefore came mainly from batching and scheduling,
  not metadata commit alone.
- Every case reported equal cache misses and subsequent cache hits, with zero
  eviction. The second validated read avoided an equivalent set of OpenDAL
  range GETs.

Run multiple iterations before using these numbers for capacity planning.
