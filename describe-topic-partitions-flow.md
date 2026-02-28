# DescribeTopicPartitions Flow

End-to-end explanation of how topic and partition listing works in this broker.

## 1. Request arrives in `handle_client` (line 1047)

When `api_key == 75` (DESCRIBE_TOPIC_PARTITIONS):

1. Parse the request header v2 (flexible) — this API always uses flexible headers
2. Parse the request body via `parse_describe_topic_partitions_request` — extracts the list of topic names the client is asking about and the `response_partition_limit`
3. Call `load_cluster_metadata()` to read what topics/partitions actually exist
4. Build the response via `build_describe_topic_partitions_response`
5. Send it back with response header v1 (`build_response_v1` — includes TAG_BUFFER)

## 2. Parsing the request (`parse_describe_topic_partitions_request`, line 394)

The wire format is:

```
topics: COMPACT_ARRAY of { name: COMPACT_STRING, TAG_BUFFER }
response_partition_limit: INT32
cursor: nullable struct (0xff = null, otherwise topic_name + partition_index + TAG_BUFFER)
TAG_BUFFER
```

Step by step:

1. **Read topic count** — unsigned varint, COMPACT_ARRAY encoding so `actual_count = raw - 1`. If raw is 0, it's a null array (no topics).
2. **Loop through topics** — for each, read the topic name (COMPACT_STRING) and skip the per-topic TAG_BUFFER. Push each name into `req.topic_names`.
3. **Read `response_partition_limit`** — 4-byte big-endian INT32. This caps how many partitions the broker should return across all topics (for pagination).
4. **Read cursor** — a nullable struct for pagination. First byte `0xff` means null (fresh request, not continuing a paginated response). Otherwise, it contains the topic name and partition index where the client left off. Currently the code just skips this data.
5. **Skip trailing TAG_BUFFER**.

The result: a `DescribeTopicPartitionsRequest` with a list of topic names and a partition limit.

## 3. Loading metadata (`load_cluster_metadata`, line 796)

This is where the broker discovers what topics and partitions actually exist. It reads the KRaft `__cluster_metadata` log file — the same log that a real Kafka cluster uses to persist metadata.

### RecordBatch framing

The log file is a sequence of **RecordBatch** frames. Each batch:

```
offset:  0  — baseOffset (8 bytes, not used)
offset:  8  — batchLength (4 bytes, big-endian)
offset: 12  — partitionLeaderEpoch, magic, crc, attributes,
               lastOffsetDelta, timestamps, producerId, etc.
offset: 57  — recordsCount (4 bytes, big-endian)
offset: 61  — records start here
```

`batchLength` covers bytes from offset 12 to the end, so total batch size = `12 + batchLength`. The code walks batch by batch: `pos += total_batch_size`.

### Individual record parsing (inside each batch)

Each record within a batch uses **zigzag-encoded varints** (not regular unsigned varints — this is the RecordBatch record format, not the protocol request format):

```
length:          zigzag varint (total record body size)
attributes:      1 byte (skipped)
timestampDelta:  zigzag varint (skipped)
offsetDelta:     zigzag varint (skipped)
keyLength:       zigzag varint (-1 or length, skip key bytes)
valueLength:     zigzag varint
value:           valueLength bytes — this is the metadata record
```

The code reads each field just to advance the offset to get to the **value** — that's where the actual metadata record lives.

### Metadata record value parsing

Each value starts with three unsigned varints:

```
frame_version:  unsigned varint (e.g., 0)
api_key:        unsigned varint — identifies the record type
version:        unsigned varint — schema version
body:           remainder
```

The code cares about two `api_key` values:

**TopicRecord (api_key = 2)** — parsed by `parse_topic_record` (line 718):

```
name:      COMPACT_STRING — the topic name
topic_id:  UUID (16 bytes) — unique identifier
TAG_BUFFER
```

Stored in `by_uuid[topic_id] = TopicInfo { name, topic_id }`.

**PartitionRecord (api_key = 3)** — parsed by `parse_partition_record` (line 745):

```
partition_id:       INT32
topic_id:           UUID (16 bytes) — links to which topic this partition belongs
replicas:           COMPACT_ARRAY of INT32
isr:                COMPACT_ARRAY of INT32
removing_replicas:  COMPACT_ARRAY of INT32 (skipped)
adding_replicas:    COMPACT_ARRAY of INT32 (skipped)
leader:             INT32
leader_epoch:       INT32
partition_epoch:    INT32
TAG_BUFFER
```

Stored as `by_uuid[topic_uuid].partitions.push_back(pinfo)`.

### Building the name map (line 946)

After processing all batches, the code has a `map<UUID, TopicInfo>` where each TopicInfo may have a name (from TopicRecord) and partitions (from PartitionRecords). The final step converts this to `map<string, TopicInfo>` keyed by topic name — skipping any entries with empty names (partition records that arrived without a matching topic record).

The key insight: **TopicRecords and PartitionRecords are separate records linked by UUID**. A topic might appear as one TopicRecord followed by N PartitionRecords. They can appear in any order across batches, which is why the code accumulates into `by_uuid` first and resolves at the end.

## 4. Building the response (`build_describe_topic_partitions_response`, line 502)

For each topic name the client requested, it looks up `metadata.find(topic_name)`:

- **Topic found** (`it != metadata.end()`): Currently hits the `else` block at line 545 with just a `// TODO` — the actual found-topic response isn't implemented yet. Then it **falls through** to the code below (line 548) which unconditionally writes an UNKNOWN_TOPIC error entry. This is a bug — found topics get a TODO placeholder followed by a duplicate error entry.

- **Topic not found**: Writes an error entry with:
  - `error_code`: ERROR_UNKNOWN_TOPIC_OR_PARTITION (3)
  - `name`: the requested topic name (COMPACT_NULLABLE_STRING)
  - `topic_id`: 16 zero bytes (no UUID since topic doesn't exist)
  - `is_internal`: false
  - `partitions`: empty COMPACT_ARRAY (encoded as `0x01` = length 0)
  - `topic_authorized_operations`: 0
  - `TAG_BUFFER`

  Then **falls through** and writes the same fields again — so each unknown topic currently produces **two** topic entries in the response. This is also a bug.

After all topics:

- `next_cursor`: null (`0xff`) — no pagination
- Final `TAG_BUFFER`

## Summary of the data flow

```
Client request (topic names)
        |
        v
parse_describe_topic_partitions_request
        |
        v
load_cluster_metadata
   |-- read RecordBatch frames from KRaft log
   |-- parse individual records (zigzag varints)
   |-- TopicRecord (api_key=2) -> name + UUID
   |-- PartitionRecord (api_key=3) -> UUID + partition details
   +-- join by UUID -> map<name, TopicInfo>
        |
        v
build_describe_topic_partitions_response
   |-- for each requested topic:
   |   |-- found in metadata -> TODO (not implemented)
   |   +-- not found -> UNKNOWN_TOPIC_OR_PARTITION error
   |-- next_cursor = null
   +-- TAG_BUFFER
        |
        v
build_response_v1 (header: correlation_id + TAG_BUFFER)
        |
        v
send to client
```
