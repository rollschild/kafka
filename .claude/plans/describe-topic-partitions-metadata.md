# Plan: Parse `__cluster_metadata` log and return real topic partition metadata

## Context

Currently, `DescribeTopicPartitions` always returns `UNKNOWN_TOPIC_OR_PARTITION` with empty partitions for every topic. We need to parse Kafka's KRaft metadata log file to discover actual topics, their UUIDs, and partition info, then return real metadata in the response.

## File to modify

- `src/main.cpp`

## Implementation Steps

### 1. Add includes (after existing includes, around line 14)

```cpp
#include <array>
#include <fstream>
#include <map>
```

### 2. Add data structures (after `DescribeTopicPartitionsRequest` struct, around line 91)

```cpp
struct PartitionInfo {
    int32_t partition_id;
    int32_t leader_id;
    int32_t leader_epoch;
    int32_t partition_epoch;
    std::vector<int32_t> replicas;
    std::vector<int32_t> isr;
};

struct TopicInfo {
    std::string name;
    std::array<uint8_t, 16> topic_id;  // UUID as raw bytes
    std::vector<PartitionInfo> partitions;
};
```

### 3. Add zigzag varint decoder (after `read_unsigned_varint`, around line 206)

Records within RecordBatches use zigzag-encoded signed varints (unlike the unsigned varints in the Kafka wire protocol).

```cpp
/**
 * Read zigzag-encoded signed varint from buffer.
 * Used in Kafka RecordBatch individual records.
 * Zigzag: (encoded >>> 1) ^ -(encoded & 1)
 */
size_t read_zigzag_varint(const uint8_t* data, size_t len, size_t offset,
                          int32_t& value) {
    uint32_t raw;
    size_t bytes_read = read_unsigned_varint(data, len, offset, raw);
    if (bytes_read == 0) return 0;
    value = static_cast<int32_t>((raw >> 1) ^ -(raw & 1));
    return bytes_read;
}
```

### 4. Add helper to read a COMPACT_ARRAY of INT32 (after `skip_tag_buffer`, around line 275)

```cpp
bool read_compact_array_int32(const uint8_t* data, size_t len, size_t& offset,
                              std::vector<int32_t>& out) {
    uint32_t num_raw;
    size_t varint_bytes = read_unsigned_varint(data, len, offset, num_raw);
    if (varint_bytes == 0) return false;
    offset += varint_bytes;

    if (num_raw == 0) return true;  // null array
    uint32_t count = num_raw - 1;
    for (uint32_t i = 0; i < count; ++i) {
        if (offset + 4 > len) return false;
        int32_t val = (data[offset] << 24) | (data[offset + 1] << 16) |
                      (data[offset + 2] << 8) | data[offset + 3];
        offset += 4;
        out.push_back(val);
    }
    return true;
}
```

### 5. Add metadata log parser (after `read_compact_array_int32`)

```cpp
const std::string METADATA_LOG_PATH =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

constexpr uint32_t METADATA_TOPIC_RECORD = 2;
constexpr uint32_t METADATA_PARTITION_RECORD = 3;

/**
 * Parse a TopicRecord value body (after frame_version, api_key, version).
 * Fields: name (COMPACT_STRING), topic_id (UUID, 16 bytes), TAG_BUFFER
 */
bool parse_topic_record(const uint8_t* data, size_t len, size_t offset,
                        TopicInfo& info) {
    if (!read_compact_nullable_string(data, len, offset, info.name)) {
        return false;
    }
    if (offset + 16 > len) return false;
    std::copy(data + offset, data + offset + 16, info.topic_id.begin());
    offset += 16;
    return skip_tag_buffer(data, len, offset);
}

/**
 * Parse a PartitionRecord value body (after frame_version, api_key, version).
 * Fields: partition_id (INT32), topic_id (UUID), replicas (COMPACT_ARRAY),
 *         isr (COMPACT_ARRAY), removing_replicas (COMPACT_ARRAY),
 *         adding_replicas (COMPACT_ARRAY), leader (INT32),
 *         leader_epoch (INT32), partition_epoch (INT32), TAG_BUFFER
 */
bool parse_partition_record(const uint8_t* data, size_t len, size_t offset,
                            std::array<uint8_t, 16>& topic_id,
                            PartitionInfo& info) {
    if (offset + 4 > len) return false;
    info.partition_id = (data[offset] << 24) | (data[offset + 1] << 16) |
                        (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;

    if (offset + 16 > len) return false;
    std::copy(data + offset, data + offset + 16, topic_id.begin());
    offset += 16;

    // replicas
    if (!read_compact_array_int32(data, len, offset, info.replicas)) {
        return false;
    }
    // isr
    if (!read_compact_array_int32(data, len, offset, info.isr)) {
        return false;
    }
    // removing_replicas (skip)
    std::vector<int32_t> removing;
    if (!read_compact_array_int32(data, len, offset, removing)) {
        return false;
    }
    // adding_replicas (skip)
    std::vector<int32_t> adding;
    if (!read_compact_array_int32(data, len, offset, adding)) {
        return false;
    }

    if (offset + 12 > len) return false;
    info.leader_id = (data[offset] << 24) | (data[offset + 1] << 16) |
                     (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;
    info.leader_epoch = (data[offset] << 24) | (data[offset + 1] << 16) |
                        (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;
    info.partition_epoch = (data[offset] << 24) | (data[offset + 1] << 16) |
                           (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;

    return skip_tag_buffer(data, len, offset);
}

/**
 * Load cluster metadata from the __cluster_metadata log file.
 * Parses RecordBatch structures, extracts TopicRecord and PartitionRecord.
 * Returns a map of topic_name -> TopicInfo.
 *
 * RecordBatch header (61 bytes):
 *   baseOffset(8), batchLength(4), partitionLeaderEpoch(4), magic(1),
 *   crc(4), attributes(2), lastOffsetDelta(4), baseTimestamp(8),
 *   maxTimestamp(8), producerId(8), producerEpoch(2), baseSequence(4),
 *   recordsCount(4)
 *
 * Each Record:
 *   length(zigzag varint), attributes(1), timestampDelta(zigzag varint),
 *   offsetDelta(zigzag varint), keyLength(zigzag varint), key(bytes),
 *   valueLength(zigzag varint), value(bytes), headersCount(zigzag varint)
 *
 * Each metadata record value:
 *   frame_version(uvarint), api_key(uvarint), version(uvarint), body
 */
std::map<std::string, TopicInfo> load_cluster_metadata() {
    std::map<std::string, TopicInfo> result;

    std::ifstream file(METADATA_LOG_PATH, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Could not open metadata log: " << METADATA_LOG_PATH
                  << std::endl;
        return result;
    }

    // Read entire file into memory
    std::vector<uint8_t> file_data(
        (std::istreambuf_iterator<char>(file)),
        std::istreambuf_iterator<char>());
    file.close();

    // uuid -> TopicInfo (intermediate map keyed by UUID)
    std::map<std::array<uint8_t, 16>, TopicInfo> by_uuid;

    size_t pos = 0;
    while (pos + 61 <= file_data.size()) {
        const uint8_t* batch = file_data.data() + pos;

        // batchLength at offset 8 (covers from partitionLeaderEpoch through end)
        int32_t batch_length = (batch[8] << 24) | (batch[9] << 16) |
                               (batch[10] << 8) | batch[11];
        if (batch_length <= 0) break;

        size_t total_batch_size = 12 + static_cast<size_t>(batch_length);
        if (pos + total_batch_size > file_data.size()) break;

        // recordsCount at offset 57
        int32_t records_count = (batch[57] << 24) | (batch[58] << 16) |
                                (batch[59] << 8) | batch[60];

        // Records start at offset 61 within the batch
        size_t rec_offset = pos + 61;

        for (int32_t r = 0; r < records_count; ++r) {
            // Record: length (zigzag varint)
            int32_t rec_length;
            size_t vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                           rec_offset, rec_length);
            if (vb == 0 || rec_length < 0) break;
            rec_offset += vb;

            size_t rec_start = rec_offset;
            size_t rec_end = rec_start + static_cast<size_t>(rec_length);
            if (rec_end > file_data.size()) break;

            // attributes (1 byte)
            rec_offset += 1;

            // timestampDelta (zigzag varint)
            int32_t ts_delta;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, ts_delta);
            if (vb == 0) break;
            rec_offset += vb;

            // offsetDelta (zigzag varint)
            int32_t off_delta;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, off_delta);
            if (vb == 0) break;
            rec_offset += vb;

            // keyLength (zigzag varint)
            int32_t key_length;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, key_length);
            if (vb == 0) break;
            rec_offset += vb;

            // skip key bytes
            if (key_length > 0) {
                rec_offset += static_cast<size_t>(key_length);
            }

            // valueLength (zigzag varint)
            int32_t value_length;
            vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                    rec_offset, value_length);
            if (vb == 0) break;
            rec_offset += vb;

            if (value_length > 0) {
                const uint8_t* value_data = file_data.data() + rec_offset;
                size_t value_len = static_cast<size_t>(value_length);

                // Parse metadata record value:
                // frame_version (uvarint), api_key (uvarint), version (uvarint), body
                size_t v_offset = 0;
                uint32_t frame_version, api_key, version;

                size_t fvb =
                    read_unsigned_varint(value_data, value_len, v_offset,
                                        frame_version);
                if (fvb == 0) { rec_offset = rec_end; continue; }
                v_offset += fvb;

                fvb = read_unsigned_varint(value_data, value_len, v_offset,
                                           api_key);
                if (fvb == 0) { rec_offset = rec_end; continue; }
                v_offset += fvb;

                fvb = read_unsigned_varint(value_data, value_len, v_offset,
                                           version);
                if (fvb == 0) { rec_offset = rec_end; continue; }
                v_offset += fvb;

                if (api_key == METADATA_TOPIC_RECORD) {
                    TopicInfo info;
                    if (parse_topic_record(value_data, value_len, v_offset,
                                           info)) {
                        by_uuid[info.topic_id] = info;
                    }
                } else if (api_key == METADATA_PARTITION_RECORD) {
                    std::array<uint8_t, 16> topic_uuid;
                    PartitionInfo pinfo;
                    if (parse_partition_record(value_data, value_len, v_offset,
                                              topic_uuid, pinfo)) {
                        by_uuid[topic_uuid].partitions.push_back(pinfo);
                    }
                }

                rec_offset = rec_end;
            } else {
                rec_offset = rec_end;
            }
        }

        pos += total_batch_size;
    }

    // Build name -> TopicInfo map
    for (auto& [uuid, info] : by_uuid) {
        if (!info.name.empty()) {
            info.topic_id = uuid;
            result[info.name] = info;
        }
    }

    return result;
}
```

### 6. Replace `build_describe_topic_partitions_response` (around line 451)

Change signature to accept metadata map, and return real partition data for found topics:

```cpp
std::vector<uint8_t> build_describe_topic_partitions_response(
    const DescribeTopicPartitionsRequest& req,
    const std::map<std::string, TopicInfo>& metadata) {
    std::vector<uint8_t> body;

    // throttle_time_ms (INT32) - no throttling
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // topics: COMPACT_ARRAY (length = N + 1)
    body.push_back(static_cast<uint8_t>(req.topic_names.size() + 1));

    for (const auto& topic_name : req.topic_names) {
        auto it = metadata.find(topic_name);

        if (it == metadata.end()) {
            // Topic not found - return UNKNOWN_TOPIC_OR_PARTITION
            body.push_back((ERROR_UNKNOWN_TOPIC_OR_PARTITION >> 8) & 0xFF);
            body.push_back(ERROR_UNKNOWN_TOPIC_OR_PARTITION & 0xFF);

            // name: COMPACT_NULLABLE_STRING
            body.push_back(static_cast<uint8_t>(topic_name.size() + 1));
            body.insert(body.end(), topic_name.begin(), topic_name.end());

            // topic_id: UUID (16 bytes of zeros)
            for (int i = 0; i < 16; ++i) body.push_back(0x00);

            // is_internal: BOOLEAN (false)
            body.push_back(0x00);

            // partitions: COMPACT_ARRAY (empty = 1)
            body.push_back(0x01);

            // topic_authorized_operations: INT32
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);

            // TAG_BUFFER
            body.push_back(0x00);
        } else {
            const TopicInfo& info = it->second;

            // error_code: ERROR_NONE
            body.push_back(0x00);
            body.push_back(0x00);

            // name: COMPACT_NULLABLE_STRING
            body.push_back(static_cast<uint8_t>(topic_name.size() + 1));
            body.insert(body.end(), topic_name.begin(), topic_name.end());

            // topic_id: UUID (16 bytes)
            body.insert(body.end(), info.topic_id.begin(),
                        info.topic_id.end());

            // is_internal: BOOLEAN (false)
            body.push_back(0x00);

            // partitions: COMPACT_ARRAY (length = N + 1)
            body.push_back(
                static_cast<uint8_t>(info.partitions.size() + 1));

            for (const auto& p : info.partitions) {
                // error_code (INT16): ERROR_NONE
                body.push_back(0x00);
                body.push_back(0x00);

                // partition_index (INT32)
                body.push_back((p.partition_id >> 24) & 0xFF);
                body.push_back((p.partition_id >> 16) & 0xFF);
                body.push_back((p.partition_id >> 8) & 0xFF);
                body.push_back(p.partition_id & 0xFF);

                // leader_id (INT32)
                body.push_back((p.leader_id >> 24) & 0xFF);
                body.push_back((p.leader_id >> 16) & 0xFF);
                body.push_back((p.leader_id >> 8) & 0xFF);
                body.push_back(p.leader_id & 0xFF);

                // leader_epoch (INT32)
                body.push_back((p.leader_epoch >> 24) & 0xFF);
                body.push_back((p.leader_epoch >> 16) & 0xFF);
                body.push_back((p.leader_epoch >> 8) & 0xFF);
                body.push_back(p.leader_epoch & 0xFF);

                // replica_nodes: COMPACT_ARRAY of INT32
                body.push_back(
                    static_cast<uint8_t>(p.replicas.size() + 1));
                for (int32_t r : p.replicas) {
                    body.push_back((r >> 24) & 0xFF);
                    body.push_back((r >> 16) & 0xFF);
                    body.push_back((r >> 8) & 0xFF);
                    body.push_back(r & 0xFF);
                }

                // isr_nodes: COMPACT_ARRAY of INT32
                body.push_back(static_cast<uint8_t>(p.isr.size() + 1));
                for (int32_t i : p.isr) {
                    body.push_back((i >> 24) & 0xFF);
                    body.push_back((i >> 16) & 0xFF);
                    body.push_back((i >> 8) & 0xFF);
                    body.push_back(i & 0xFF);
                }

                // eligible_leader_replicas: COMPACT_ARRAY (empty)
                body.push_back(0x01);

                // last_known_elr: COMPACT_ARRAY (empty)
                body.push_back(0x01);

                // offline_replicas: COMPACT_ARRAY (empty)
                body.push_back(0x01);

                // TAG_BUFFER
                body.push_back(0x00);
            }

            // topic_authorized_operations: INT32
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);

            // TAG_BUFFER
            body.push_back(0x00);
        }
    }

    // next_cursor: null
    body.push_back(0xff);

    // TAG_BUFFER
    body.push_back(0x00);

    return body;
}
```

### 7. Update call site in `handle_client` (around line 698-716)

Change from:

```cpp
            std::vector<uint8_t> body =
                build_describe_topic_partitions_response(req);
```

to:

```cpp
            auto metadata = load_cluster_metadata();
            std::vector<uint8_t> body =
                build_describe_topic_partitions_response(req, metadata);
```

## Reusable existing functions

- `read_unsigned_varint()` — for unsigned varints in metadata record values
- `read_compact_nullable_string()` — for COMPACT_STRING fields in TopicRecord
- `skip_tag_buffer()` — for TAG_BUFFER at end of TopicRecord/PartitionRecord

## Kafka RecordBatch format reference

A `.log` file is a sequence of RecordBatch structures (no file header).

**RecordBatch header (61 bytes, all big-endian):**

| Offset | Field                  | Size   |
|--------|------------------------|--------|
| 0      | baseOffset             | int64  |
| 8      | batchLength            | int32  |
| 12     | partitionLeaderEpoch   | int32  |
| 16     | magic                  | int8   |
| 17     | crc                    | uint32 |
| 21     | attributes             | int16  |
| 23     | lastOffsetDelta        | int32  |
| 27     | baseTimestamp           | int64  |
| 35     | maxTimestamp            | int64  |
| 43     | producerId             | int64  |
| 51     | producerEpoch          | int16  |
| 53     | baseSequence           | int32  |
| 57     | recordsCount           | int32  |

Total batch size on disk: `12 + batchLength`.

**Metadata record value format (MetadataRecordSerde):**

`[frame_version: uvarint] [api_key: uvarint] [version: uvarint] [message bytes]`

- apiKey 2 = TopicRecord: COMPACT_STRING name + 16-byte UUID + TAG_BUFFER
- apiKey 3 = PartitionRecord: int32 partitionId + 16-byte UUID + COMPACT_ARRAY replicas + COMPACT_ARRAY isr + COMPACT_ARRAY removingReplicas + COMPACT_ARRAY addingReplicas + int32 leader + int32 leaderEpoch + int32 partitionEpoch + TAG_BUFFER

## Verification

1. `cmake . -B build && cmake --build build` — must compile cleanly with `-Werror`
2. Run with a Kafka broker in KRaft mode that has created topics, verify `DescribeTopicPartitions` returns correct metadata
