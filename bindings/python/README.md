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
    print(await src.group("my-consumer"))
    print(await src.schemas())
    print(await src.schema("orders-value"))
    print(await src.schema_version("orders-value", "1"))

    page = await src.messages(
        "orders",
        partition=0,
        offset="earliest",
        limit=50,
        value_format="auto",        # auto | avro | json | raw
        value_contains="widget",    # optional forward-scan filter
        regex=False,
        max_scan=5000,
    )
    print(page["count"], page["scanned"], page["records"])

asyncio.run(main())
```

All methods return plain Python objects (dicts/lists/scalars) and raise
`kotatsu.KotatsuError` on failure.

[PyO3]: https://pyo3.rs
[maturin]: https://www.maturin.rs
[`pyo3-async-runtimes`]: https://docs.rs/pyo3-async-runtimes
