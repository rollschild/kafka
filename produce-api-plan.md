# Plan: Add Produce API (key 0, v0–11) stub support

## Context
Add Produce API handling as a stub — parse requests, return success for known topics/partitions and error for unknown ones. No actual message storage. The Produce entry must appear in the ApiVersions response with max_version=11.

## File: `src/main.cpp`

---

### Change 1: Add PRODUCE constant (line 25)

```cpp
// before:
constexpr int16_t FETCH = 1;

// after:
constexpr int16_t PRODUCE = 0;
constexpr int16_t FETCH = 1;
```

---

### Change 2: Add to SUPPORTED_API_VERSIONS (lines 72–77)

```cpp
// before:
const std::vector<ApiVersionRange> SUPPORTED_API_VERSIONS = {
    {API_VERSIONS, 0, 4},
    {DESCRIBE_TOPIC_PARTITIONS, 0, 0},
    {FETCH, 0, 16},
    // add more here
};

// after:
const std::vector<ApiVersionRange> SUPPORTED_API_VERSIONS = {
    {API_VERSIONS, 0, 4},
    {DESCRIBE_TOPIC_PARTITIONS, 0, 0},
    {FETCH, 0, 16},
    {PRODUCE, 0, 11},
};
```

---

### Change 3: Add Produce structs (after FetchRequest, ~line 85)

Insert after the closing `};` of `FetchRequest`:

```cpp
struct ProduceRequestPartition {
    int32_t partition_index;
};

struct ProduceRequestTopic {
    std::string name;
    std::vector<ProduceRequestPartition> partitions;
};

struct ProduceRequest {
    int16_t acks;
    int32_t timeout_ms;
    std::vector<ProduceRequestTopic> topics;
};
```

---

### Change 4: Update `uses_flexible_header()` (lines 745–750)

```cpp
// before:
bool uses_flexible_header(int16_t api_key, int16_t api_version) {
    if (api_key == API_VERSIONS) return api_version > 3;
    if (api_key == DESCRIBE_TOPIC_PARTITIONS) return true;
    if (api_key == FETCH) return api_version >= 12;
    return false;
}

// after:
bool uses_flexible_header(int16_t api_key, int16_t api_version) {
    if (api_key == API_VERSIONS) return api_version > 3;
    if (api_key == DESCRIBE_TOPIC_PARTITIONS) return true;
    if (api_key == FETCH) return api_version >= 12;
    if (api_key == PRODUCE) return api_version >= 9;
    return false;
}
```

---

### Change 5: Add `parse_produce_request()` (before `build_fetch_response`, ~line 1198)

```cpp
/**
 * Parse Produce request body.
 * v0-2: acks(INT16) + timeout(INT32) + topics(ARRAY of {name(STRING),
 *        partitions(ARRAY of {index(INT32), records(RECORDS)})})
 * v3-8: transactional_id(NULLABLE_STRING) prepended, rest same
 * v9-11: transactional_id(COMPACT_NULLABLE_STRING), COMPACT_ARRAY,
 *         COMPACT_STRING, TAG_BUFFERs
 */
bool parse_produce_request(const uint8_t* data, size_t len, size_t offset,
                            ProduceRequest& req, int16_t api_version) {
    if (api_version >= 9) {
        // v9+: flexible encoding
        // transactional_id (COMPACT_NULLABLE_STRING)
        std::optional<std::string> txn_id;
        if (!read_compact_nullable_string(data, len, offset, txn_id)) {
            return false;
        }

        // acks (INT16)
        if (offset + 2 > len) return false;
        req.acks = (data[offset] << 8) | data[offset + 1];
        offset += 2;

        // timeout_ms (INT32)
        if (offset + 4 > len) return false;
        req.timeout_ms = (data[offset] << 24) | (data[offset + 1] << 16) |
                         (data[offset + 2] << 8) | data[offset + 3];
        offset += 4;

        // topic_data: COMPACT_ARRAY
        uint32_t num_topics_raw;
        size_t varint_bytes =
            read_unsigned_varint(data, len, offset, num_topics_raw);
        if (varint_bytes == 0) return false;
        offset += varint_bytes;

        if (num_topics_raw > 0) {
            uint32_t num_topics = num_topics_raw - 1;
            for (uint32_t i = 0; i < num_topics; ++i) {
                // name (COMPACT_STRING)
                std::optional<std::string> topic_name;
                if (!read_compact_nullable_string(data, len, offset,
                                                  topic_name)) {
                    return false;
                }
                ProduceRequestTopic topic;
                topic.name = topic_name.value_or("");

                // partition_data: COMPACT_ARRAY
                uint32_t num_parts_raw;
                varint_bytes =
                    read_unsigned_varint(data, len, offset, num_parts_raw);
                if (varint_bytes == 0) return false;
                offset += varint_bytes;

                if (num_parts_raw > 0) {
                    uint32_t num_parts = num_parts_raw - 1;
                    for (uint32_t j = 0; j < num_parts; ++j) {
                        // partition_index (INT32)
                        if (offset + 4 > len) return false;
                        int32_t part_idx =
                            (data[offset] << 24) | (data[offset + 1] << 16) |
                            (data[offset + 2] << 8) | data[offset + 3];
                        offset += 4;
                        topic.partitions.push_back({part_idx});

                        // records (COMPACT_RECORDS): length as unsigned varint
                        uint32_t records_len;
                        varint_bytes = read_unsigned_varint(data, len, offset,
                                                           records_len);
                        if (varint_bytes == 0) return false;
                        offset += varint_bytes;
                        if (records_len > 0) {
                            uint32_t actual_len = records_len - 1;
                            if (offset + actual_len > len) return false;
                            offset += actual_len;  // skip record data
                        }

                        // TAG_BUFFER
                        if (!skip_tag_buffer(data, len, offset)) return false;
                    }
                }

                // TAG_BUFFER per topic
                if (!skip_tag_buffer(data, len, offset)) return false;

                req.topics.push_back(std::move(topic));
            }
        }
    } else {
        // v0-8: non-flexible encoding
        if (api_version >= 3) {
            // transactional_id (NULLABLE_STRING: INT16 length, -1 = null)
            if (offset + 2 > len) return false;
            int16_t txn_len = (data[offset] << 8) | data[offset + 1];
            offset += 2;
            if (txn_len > 0) {
                if (offset + static_cast<size_t>(txn_len) > len) return false;
                offset += txn_len;
            }
        }

        // acks (INT16)
        if (offset + 2 > len) return false;
        req.acks = (data[offset] << 8) | data[offset + 1];
        offset += 2;

        // timeout_ms (INT32)
        if (offset + 4 > len) return false;
        req.timeout_ms = (data[offset] << 24) | (data[offset + 1] << 16) |
                         (data[offset + 2] << 8) | data[offset + 3];
        offset += 4;

        // topic_data: ARRAY (INT32 count)
        if (offset + 4 > len) return false;
        int32_t num_topics = (data[offset] << 24) | (data[offset + 1] << 16) |
                             (data[offset + 2] << 8) | data[offset + 3];
        offset += 4;

        for (int32_t i = 0; i < num_topics; ++i) {
            // name: STRING (INT16 length prefix)
            if (offset + 2 > len) return false;
            int16_t name_len = (data[offset] << 8) | data[offset + 1];
            offset += 2;
            if (name_len < 0 || offset + static_cast<size_t>(name_len) > len)
                return false;
            ProduceRequestTopic topic;
            topic.name = std::string(
                reinterpret_cast<const char*>(data + offset), name_len);
            offset += name_len;

            // partition_data: ARRAY (INT32 count)
            if (offset + 4 > len) return false;
            int32_t num_parts =
                (data[offset] << 24) | (data[offset + 1] << 16) |
                (data[offset + 2] << 8) | data[offset + 3];
            offset += 4;

            for (int32_t j = 0; j < num_parts; ++j) {
                // partition_index (INT32)
                if (offset + 4 > len) return false;
                int32_t part_idx =
                    (data[offset] << 24) | (data[offset + 1] << 16) |
                    (data[offset + 2] << 8) | data[offset + 3];
                offset += 4;
                topic.partitions.push_back({part_idx});

                // records: RECORDS (INT32 size prefix + data)
                if (offset + 4 > len) return false;
                int32_t records_size =
                    (data[offset] << 24) | (data[offset + 1] << 16) |
                    (data[offset + 2] << 8) | data[offset + 3];
                offset += 4;
                if (records_size > 0) {
                    if (offset + static_cast<size_t>(records_size) > len)
                        return false;
                    offset += records_size;  // skip record data
                }
            }

            req.topics.push_back(std::move(topic));
        }
    }
    return true;
}
```

---

### Change 6: Add `build_produce_response()` (right after `parse_produce_request`)

```cpp
/**
 * Build Produce response body.
 * v0:   topics(ARRAY of {name, partitions(ARRAY of {index, error_code,
 *       base_offset})})
 * v1+:  + throttle_time_ms at end
 * v2+:  + log_append_time per partition
 * v5+:  + log_start_offset per partition
 * v8+:  + record_errors(ARRAY, empty) + error_message(NULLABLE_STRING) per
 *       partition
 * v9+:  COMPACT_ARRAY, COMPACT_STRING, TAG_BUFFERs
 */
std::vector<uint8_t> build_produce_response(
    const ProduceRequest& req,
    const std::map<std::string, TopicInfo>& metadata, int16_t api_version) {
    std::vector<uint8_t> body;
    bool flexible = api_version >= 9;

    // topics array
    if (flexible) {
        body.push_back(static_cast<uint8_t>(req.topics.size() + 1));
    } else {
        int32_t n = static_cast<int32_t>(req.topics.size());
        body.push_back((n >> 24) & 0xFF);
        body.push_back((n >> 16) & 0xFF);
        body.push_back((n >> 8) & 0xFF);
        body.push_back(n & 0xFF);
    }

    for (const auto& topic : req.topics) {
        // topic name
        if (flexible) {
            // COMPACT_STRING: length + 1 as unsigned varint
            append_unsigned_varint(
                body, static_cast<uint32_t>(topic.name.size() + 1));
        } else {
            int16_t name_len = static_cast<int16_t>(topic.name.size());
            body.push_back((name_len >> 8) & 0xFF);
            body.push_back(name_len & 0xFF);
        }
        body.insert(body.end(), topic.name.begin(), topic.name.end());

        auto it = metadata.find(topic.name);
        bool topic_known = (it != metadata.end());

        // partitions array
        if (flexible) {
            body.push_back(
                static_cast<uint8_t>(topic.partitions.size() + 1));
        } else {
            int32_t n = static_cast<int32_t>(topic.partitions.size());
            body.push_back((n >> 24) & 0xFF);
            body.push_back((n >> 16) & 0xFF);
            body.push_back((n >> 8) & 0xFF);
            body.push_back(n & 0xFF);
        }

        for (const auto& part : topic.partitions) {
            // partition_index (INT32)
            body.push_back((part.partition_index >> 24) & 0xFF);
            body.push_back((part.partition_index >> 16) & 0xFF);
            body.push_back((part.partition_index >> 8) & 0xFF);
            body.push_back(part.partition_index & 0xFF);

            // error_code (INT16)
            int16_t error = topic_known ? ERROR_NONE
                                        : ERROR_UNKNOWN_TOPIC_OR_PARTITION;
            body.push_back((error >> 8) & 0xFF);
            body.push_back(error & 0xFF);

            // base_offset (INT64): 0
            for (int i = 0; i < 8; ++i) body.push_back(0x00);

            // log_append_time (INT64): -1 (v2+)
            if (api_version >= 2) {
                for (int i = 0; i < 8; ++i) body.push_back(0xFF);
            }

            // log_start_offset (INT64): 0 (v5+)
            if (api_version >= 5) {
                for (int i = 0; i < 8; ++i) body.push_back(0x00);
            }

            // record_errors: empty array (v8+)
            if (api_version >= 8) {
                if (flexible) {
                    body.push_back(0x01);  // COMPACT_ARRAY: 0+1 = empty
                } else {
                    // ARRAY: count = 0
                    body.push_back(0x00);
                    body.push_back(0x00);
                    body.push_back(0x00);
                    body.push_back(0x00);
                }
            }

            // error_message: null (v8+)
            if (api_version >= 8) {
                if (flexible) {
                    body.push_back(0x00);  // COMPACT_NULLABLE_STRING: 0 = null
                } else {
                    // NULLABLE_STRING: -1 = null
                    body.push_back(0xFF);
                    body.push_back(0xFF);
                }
            }

            // TAG_BUFFER per partition (v9+)
            if (flexible) {
                body.push_back(0x00);
            }
        }

        // TAG_BUFFER per topic (v9+)
        if (flexible) {
            body.push_back(0x00);
        }
    }

    // throttle_time_ms (INT32): 0 (v1+)
    if (api_version >= 1) {
        body.push_back(0x00);
        body.push_back(0x00);
        body.push_back(0x00);
        body.push_back(0x00);
    }

    // TAG_BUFFER (v9+)
    if (flexible) {
        body.push_back(0x00);
    }

    return body;
}
```

---

### Change 7: Add dispatch in `handle_client()` (after the Fetch block, ~line 1496)

Insert after the closing `}` of the Fetch branch:

```cpp
        } else if (header.request_api_key == PRODUCE) {
            ProduceRequest req;
            if (!parse_produce_request(request.data(), request.size(),
                                       header_end_offset, req,
                                       header.request_api_version)) {
                std::cerr << "Failed to parse Produce request!" << std::endl;
                break;
            }
            auto metadata = load_cluster_metadata();
            std::vector<uint8_t> body = build_produce_response(
                req, metadata, header.request_api_version);
            std::vector<uint8_t> res =
                (header.request_api_version >= 9)
                    ? build_response_v1(header.correlation_id, body)
                    : build_response(header.correlation_id, body);
            if (!write_exact(client_fd, res.data(), res.size())) {
                std::cerr << "Failed to send Produce response!" << std::endl;
                break;
            }
        }
```

---

## Verification
1. `cmake . -B build && cmake --build build` — must compile cleanly with `-Werror`
2. Run the server and send an ApiVersions request; verify response includes Produce entry (key=0, min=0, max=11) alongside ApiVersions (key=18, min=0, max=4)
