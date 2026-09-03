# kotatsu (Python bindings)

Async Python bindings for the Kotatsu reader. They read Tansu's native S3
storage **directly** — topics, messages, consumer groups and schemas — without
a Kafka broker, reusing the exact same read core as Kotatsu's HTTP API.

The bindings are built from the `backend` crate via [PyO3] + [maturin].
Methods are `async` and integrate with asyncio (FastAPI etc.) through
[`pyo3-async-runtimes`].

## Install

The wheel is built from this repo (no published package). From a checkout:

```bash
pip install maturin
cd bindings/python
maturin build --release --out dist
pip install dist/*.whl
```

`data-plane` builds the wheel in its own CI, pinned to a Kotatsu git tag — see
its build files.

## Usage

```python
import asyncio
import kotatsu

async def main():
    src = kotatsu.Source(
        bucket="tansu",
        cluster="my-cluster",
        endpoint="https://s3.example.com",   # omit for real AWS S3
        region="us-east-1",
        access_key="...",                     # omit to use the ambient AWS chain
        secret_key="...",
        kora_url="http://kora:8081",          # optional schema registry
    )

    print(await src.clusters())
    print(await src.topics(search="orders", limit=20, offset=0))
    print(await src.topic("orders"))
    print(await src.topic_groups("orders"))
    print(await src.groups())
    # Lag is opt-in: it reads the high watermark behind every committed offset.
    # With it, the whole result set is ranked worst-first — pass sort="name" to
    # keep alphabetical order and compute lag for the returned page only.
    print(await src.groups(lag=True))
    print(await src.group("my-consumer"))
    print(await src.schemas())
    print(await src.schema("orders-value"))
    print(await src.schema_version("orders-value", "1"))

    page = await src.messages(
        "orders",
        partition=None,             # None = every partition, merged
        offset="earliest",          # also sets which way the read travels
        limit=50,
        value_format="auto",        # auto | avro | json | raw
        value_contains="widget",    # optional forward-scan filter
        regex=False,
        max_scan=5000,
    )
    print(page["count"], page["scanned"], page["records"])
    # With partition=None the payload carries a per-partition summary instead of
    # a single watermark. `order` follows the read — "timestamp_asc" here, and
    # "timestamp_desc" from `latest` — and `order_best_effort` marks the merge
    # across partitions as best effort.
    print(page["partitions"], page["order"])

    # Every response says where each partition would resume. Hand those back as
    # `cursor` for the next window; `exhausted` is True once there is no more.
    while not page["exhausted"]:
        cursor = ",".join(
            f"{p['partition']}:{p['resume']}"
            for p in page["partitions"]
            if p["resume"] is not None
        )
        page = await src.messages("orders", offset="earliest", cursor=cursor, limit=50)
        print(page["count"])

asyncio.run(main())
```

All methods return plain Python objects (dicts/lists/scalars) and raise
`kotatsu.KotatsuError` on failure.

[PyO3]: https://pyo3.rs
[maturin]: https://www.maturin.rs
[`pyo3-async-runtimes`]: https://docs.rs/pyo3-async-runtimes
