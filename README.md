# mkpipe-loader-elasticsearch

Elasticsearch loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into Elasticsearch indices using `elasticsearch-py` bulk helpers.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  es_target:
    variant: elasticsearch
    host: localhost
    port: 9200
    user: elastic
    password: mypassword
```

With API key authentication:

```yaml
connections:
  es_target:
    variant: elasticsearch
    host: localhost
    port: 9200
    api_key: 'your-api-key'
    extra:
      scheme: https
      verify_certs: false
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_es
    source: pg_source
    destination: es_target
    tables:
      - name: public.events
        target_name: stg_events
        replication_method: full
        batchsize: 5000
```

---

## Write Strategy

Control how data is written to Elasticsearch:

```yaml
      - name: public.events
        target_name: stg_events
        write_strategy: upsert       # append | replace | upsert
        write_key: [event_id]        # required for upsert
```

| Strategy | Elasticsearch Behavior |
|---|---|
| `append` | Bulk insert documents (default for incremental) |
| `replace` | Delete the index, then bulk insert (default for full) |
| `upsert` | Bulk insert with document `_id` derived from `write_key` columns. Existing documents with the same `_id` are overwritten. |

> **Note:** For `upsert`, the document `_id` is constructed by joining `write_key` column values with `_`. This leverages Elasticsearch's native idempotent indexing — re-indexing a document with the same `_id` replaces it.

---

## Write Throughput

Write throughput is controlled by `batchsize` — the number of documents sent per `bulk` API call:

```yaml
      - name: public.events
        target_name: stg_events
        replication_method: full
        batchsize: 10000    # default: 10000 docs per bulk request
```

### Performance Notes

- Larger `batchsize` → fewer round-trips → higher throughput, but more memory per request.
- 5,000–10,000 is a safe default for most workloads.
- Elasticsearch loader uses `df.collect()` to gather data on the driver before writing — for very large datasets consider loading into a JDBC target instead.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | Elasticsearch destination index name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `batchsize` | int | `10000` | Documents per `bulk` API call |
| `write_strategy` | string | — | `append`, `replace`, `upsert` |
| `write_key` | list | — | Key columns for upsert (used as document `_id`) |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |
