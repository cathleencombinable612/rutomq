# OpenDAL S3 multipart acceptance

`run-orbstack.sh` starts a fresh MinIO service and runs the Rust 1.88 storage
integration test inside OrbStack. The test uploads an object slightly larger
than two 5 MiB parts with concurrency two, requires a multipart ETag, checks a
range crossing a part boundary, rejects replacement, and deletes the object.
It then runs a real broker Produce request spanning twelve partitions into one
10 MiB+ object and Fetches every checksum-verified range back through the Kafka
protocol.

```bash
tests/storage/run-orbstack.sh
```
