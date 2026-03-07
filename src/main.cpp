#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// the ApiVersions request
constexpr int16_t PRODUCE = 0;
constexpr int16_t FETCH = 1;
constexpr int16_t API_VERSIONS = 18;

// DescribeTopicPartitions
constexpr int16_t DESCRIBE_TOPIC_PARTITIONS = 75;
constexpr int16_t ERROR_UNKNOWN_TOPIC_OR_PARTITION = 3;

constexpr int16_t ERROR_NONE = 0;
constexpr int16_t ERROR_UNSUPPORTED_VERSION = 35;
constexpr int16_t ERROR_UNKNOWN_TOPIC_ID = 100;

const std::string METADATA_LOG_PATH =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
constexpr uint32_t METADATA_TOPIC_RECORD = 2;
constexpr uint32_t METADATA_PARTITION_RECORD = 3;

/**
 * Full wire format for ApiVersions Request v4:
 * - Message size (4 bytes, big endian)
 * - Request Header v2
 *   - request_api_key
 *   - request_api_version
 *   - correlation_id
 *   - client_id
 *   - TAG_BUFFER
 * - Request body
 *   - client_software_name (COMPACT_STRING)
 *   - client_software_version (COMPACT_STRING)
 *   - TAG_BUFFER
 */

struct RequestHeader {
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
    std::string client_id;
};

struct ApiVersionsRequestBody {
    std::string client_software_name;
    std::string client_softare_version;
};

struct ApiVersionRange {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

const std::vector<ApiVersionRange> SUPPORTED_API_VERSIONS = {
    {API_VERSIONS, 0, 4},
    {DESCRIBE_TOPIC_PARTITIONS, 0, 0},
    {FETCH, 0, 16},
    {PRODUCE, 0, 11}
    // add more here
};

struct FetchRequestTopic {
    std::array<uint8_t, 16> topic_id;
    std::vector<int32_t> partitions;
};
struct FetchRequest {
    std::vector<FetchRequestTopic> topics;
};

struct ProduceRequestPartition {
    int32_t partition_index;
    std::vector<uint8_t> records;
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

/**
 DescribeTopicPartitions Request (API key 75, version 0), using flexible header
 v2

  Message size (4 bytes, big-endian)
  Request Header v2:
    request_api_key (INT16)
    request_api_version (INT16)
    correlation_id (INT32)
    client_id (NULLABLE_STRING - int16 length prefix)
    TAG_BUFFER
  Request Body:
    topics (COMPACT_ARRAY):
      for each topic:
        name (COMPACT_STRING)
        TAG_BUFFER
    response_partition_limit (INT32)
    cursor (nullable struct):
      0xff = null, otherwise:
        topic_name (COMPACT_STRING)
        partition_index (INT32)
        TAG_BUFFER
    TAG_BUFFER
*/
struct DescribeTopicPartitionsRequest {
    std::vector<std::string> topic_names;
    int32_t response_partition_limit;
};

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
    std::array<uint8_t, 16> topic_id;
    std::vector<PartitionInfo> partitions;
};

bool is_version_supported(int16_t api_key, int16_t version) {
    for (const auto& api : SUPPORTED_API_VERSIONS) {
        if (api.api_key == api_key) {
            return version >= api.min_version && version <= api.max_version;
        }
    }
    return false;
}

/**
 * Read exactly n bytes from socket _into_ buffer
 */
bool read_exact(int fd, u_int8_t* buffer, size_t n) {
    size_t total_read = 0;
    while (total_read < n) {
        ssize_t bytes_read = read(fd, buffer + total_read, n - total_read);
        if (bytes_read <= 0) {
            return false;
        }
        total_read += bytes_read;
    }
    return true;
}

/**
 * Write exactly n bytes _from_ buffer _to_ socket
 */
bool write_exact(int fd, const uint8_t* buffer, size_t n) {
    size_t total_written = 0;
    while (total_written < n) {
        ssize_t bytes_written =
            write(fd, buffer + total_written, n - total_written);
        if (bytes_written <= 0) {
            return false;
        }
        total_written += bytes_written;
    }
    return true;
}

/**
 * Used for ApiVersion v0-2
 */
bool parse_request_header_v1(const uint8_t* data, size_t len,
                             RequestHeader& header, size_t& offset) {
    if (len < 8) {
        // api_key (2) + api_version (2) + correlation_id (4)
        return false;
    }
    header.request_api_key = (data[0] << 8) | data[1];
    header.request_api_version = (data[2] << 8) | data[3];
    header.correlation_id =
        (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7];
    offset = 8;

    // for v2
    if (len < offset + 2) {
        // need at least 2 more bytes for client_id length
        // in kafka's protocol, a NULLABLE_STRING is encoded as:
        //   - int16 (2 bytes) - length of the string
        //   - N bytes
        return false;
    }
    int16_t client_id_len = (data[offset] << 8) | data[offset + 1];
    offset += 2;

    if (client_id_len == -1) {
        header.client_id = "";  // null string
    } else if (client_id_len >= 0) {
        if (len < offset + static_cast<size_t>(client_id_len)) {
            return false;
        }
        header.client_id = std::string(
            reinterpret_cast<const char*>(data + offset), client_id_len);
        offset += client_id_len;
    } else {
        return false;
    }
    return true;
}

/**
 * Read unsigned varint from buffer
 * Returns number of bytes consumed, 0 on error
 * Unsigned varint:
 *   - space-efficient encoding
 *   - smaller numbers use fewer bytes
 *   - 7 bits for data
 *   - 1 bit (MSB/high bit) as a continuation flag
 *     - 1 = more bytes follow
 *     - 0 = last byte
 * varints are LITTLE ENDIAN!
 */
size_t read_unsigned_varint(const uint8_t* data, size_t len, size_t offset,
                            uint32_t& value) {
    value = 0;
    int shift = 0;
    size_t bytes_read = 0;
    while (offset + bytes_read < len) {
        uint8_t b = data[offset + bytes_read];
        bytes_read++;
        // shifting to left since the bytes arrive in little-endian order
        // here, least significant bits come first
        value |= static_cast<uint32_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {  // highest bit is 0
            return bytes_read;
        }
        shift += 7;
        if (shift > 28) {
            return 0;  // overflow
        }
    }
    return 0;  // incomplete
}

void append_unsigned_varint(std::vector<uint8_t>& buf, uint32_t value) {
    // why?
    while (value > 0x7F) {
        buf.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(value));
}

/**
 * Read zigzag-encoded signed varint from buffer.
 * Used in RecordBatch individual records.
 * Zigzag:
 *   - decode: (encoded >>> 1) ^ -(encoded & 1)
 *   - encode: (n << 1) ^ (n >> 31)
 *   - Encoding: signed -> encoded:
 *     - 0 -> 0
 *     - -1 -> 1
 *     - 1 -> 2
 *     - -2 -> 3
 *     - 2 -> 4
 * Small negative (and positive) numbers to unsigned value.
 * the sign bit is store in the lower 0 bit in the final result
 * `encoded >>> 1` is a logical (unsigned) right shift — recovers the magnitude
 * `encoded & 1` extracts the sign bit (0 = positive, 1 = negative)
 * `-(encoded & 1)` produces `0x00000000` or `0xFFFFFFFF`
 * XOR flips all bits back for negatives, does nothing for positives
 */
size_t read_zigzag_varint(const uint8_t* data, size_t len, size_t offset,
                          int32_t& value) {
    uint32_t raw;
    size_t bytes_read = read_unsigned_varint(data, len, offset, raw);
    if (bytes_read == 0) {
        return 0;
    }
    value = static_cast<int32_t>((raw >> 1) ^ -(raw & 1));
    return bytes_read;
}

/**
 * Read COMPACT_NULLABLE_STRING from buffer
 * Encoding: unsigned varint length (0=null, N=N-1 chars) + data
 */
bool read_compact_nullable_string(const uint8_t* data, size_t len,
                                  size_t& offset,
                                  std::optional<std::string>& out) {
    uint32_t length;
    size_t varint_bytes = read_unsigned_varint(data, len, offset, length);
    if (varint_bytes == 0) {
        return false;
    }
    offset += varint_bytes;

    if (length == 0) {
        out = std::nullopt;  // null string
        return true;
    }

    // COMPACT encoding: actual_length = encoded_length - 1
    // convetion of Kafka
    // encoded_length = 0 means null (no string at all)
    // encoded_length = 1 means string of length 0 (empty string "")
    length--;
    if (offset + length > len) {
        return false;
    }

    out = std::string(reinterpret_cast<const char*>(data + offset), length);
    offset += length;
    return true;
}

/**
 * Skip TAG_BUFFER in buffer
 * Encoding: unsigned varint num_fields, then for each field:
 *   - tag (uvarint)
 *   - size (uvarint)
 *   - data (size bytes)
 */
bool skip_tag_buffer(const uint8_t* data, size_t len, size_t& offset) {
    uint32_t num_tags;
    size_t varint_bytes = read_unsigned_varint(data, len, offset, num_tags);
    if (varint_bytes == 0) {
        return false;
    }
    offset += varint_bytes;

    for (uint32_t i = 0; i < num_tags; ++i) {
        uint32_t tag, size;
        varint_bytes = read_unsigned_varint(data, len, offset, tag);
        if (varint_bytes == 0) {
            return false;
        }
        offset += varint_bytes;

        varint_bytes = read_unsigned_varint(data, len, offset, size);
        if (varint_bytes == 0) {
            return false;
        }
        offset += varint_bytes;

        if (offset + size > len) {
            return false;
        }
        offset += size;
    }
    return true;
}

/**
 * parse request header v2 (flexible)
 * used for ApiVersions v3
 */
bool parse_request_header_v2(const uint8_t* data, size_t len,
                             RequestHeader& header, size_t& offset) {
    if (len < 8) {
        // api_key (2) + api_version (2) + correlation_id (4)
        return false;
    }
    header.request_api_key = (data[0] << 8) | data[1];
    header.request_api_version = (data[2] << 8) | data[3];
    header.correlation_id =
        (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7];
    offset = 8;

    // regular NULLABLE_STRING for client_id
    if (len < offset + 2) return false;
    int16_t client_id_len = (data[offset] << 8) | data[offset + 1];
    offset += 2;
    if (client_id_len == -1) {
        header.client_id = "";  // null string
    } else if (client_id_len >= 0) {
        if (len < offset + static_cast<size_t>(client_id_len)) {
            return false;
        }
        header.client_id = std::string(
            reinterpret_cast<const char*>(data + offset), client_id_len);
        offset += client_id_len;
    } else {
        return false;
    }

    // TAG_BUFFER in header
    if (!skip_tag_buffer(data, len, offset)) {
        return false;
    }

    return true;
}

/**
 * Parse ApiVersions request body v3+
 */
bool parse_api_versions_request_body_v3(const uint8_t* data, size_t len,
                                        size_t offset,
                                        ApiVersionsRequestBody& body) {
    // client_software_name (COMPACT_STRING)
    std::optional<std::string> name;
    if (!read_compact_nullable_string(data, len, offset, name)) {
        return false;
    }
    body.client_software_name = name.value_or("");
    // client_software_version (COMPACT_STRING)
    std::optional<std::string> version;
    if (!read_compact_nullable_string(data, len, offset, version)) {
        return false;
    }
    body.client_softare_version = version.value_or("");

    // TAG_BUFFER
    if (!skip_tag_buffer(data, len, offset)) {
        return false;
    }

    return true;
}

/**
topics: COMPACT_ARRAY of { name: COMPACT_STRING, TAG_BUFFER }
response_partition_limit: INT32
cursor: nullable struct (0xff = null, otherwise topic_name + partition_index +
TAG_BUFFER) TAG_BUFFER
*/
bool parse_describe_topic_partitions_request(
    const uint8_t* data, size_t len, size_t offset,
    DescribeTopicPartitionsRequest& req) {
    // topics: COMPACT_ARRAY
    uint32_t num_topics_raw;
    size_t varint_bytes =
        read_unsigned_varint(data, len, offset, num_topics_raw);
    if (varint_bytes == 0) {
        return false;
    }
    offset += varint_bytes;

    if (num_topics_raw > 0) {
        uint32_t num_topics = num_topics_raw - 1;
        for (uint32_t i = 0; i < num_topics; ++i) {
            std::optional<std::string> topic_name;
            if (!read_compact_nullable_string(data, len, offset, topic_name)) {
                return false;
            }
            if (!topic_name.has_value()) {
                return false;  // null topic name is invalid
            }
            req.topic_names.push_back(std::move(*topic_name));

            // TAG_BUFFER per topic entry
            if (!skip_tag_buffer(data, len, offset)) {
                return false;
            }
        }
    }

    // response_partition_limit (INT32)
    // tells the broker the max number of partitions the client wants back
    // across all topics
    if (offset + 4 > len) {
        return false;
    }
    req.response_partition_limit = (data[offset] << 24) |
                                   (data[offset + 1] << 16) |
                                   (data[offset + 2] << 8) | data[offset + 3];
    offset += 4;

    // cursor (nullable struct)
    // a nullable struct for pagination
    // the first byte indicates presense: 0xff = null
    // anything else = curour is present, meaning the client is continuing a
    // previous paginated response
    if (offset >= len) {
        return false;
    }
    uint8_t cursor_tag = data[offset];
    offset++;
    if (cursor_tag != 0xff) {
        // cursor present - skip topic_name + partition_index + TAG_BUFFER
        std::optional<std::string> cursor_topic;
        if (!read_compact_nullable_string(data, len, offset, cursor_topic)) {
            return false;
        }
        if (offset + 4 > len) {
            return false;
        }
        offset += 4;
        if (!skip_tag_buffer(data, len, offset)) {
            return false;
        }
    }

    // TAG_BUFFER
    if (!skip_tag_buffer(data, len, offset)) {
        return false;
    }

    return true;
}

/**
 DescribeTopicPartitions response, using response header v1:

  Message size (4 bytes, big-endian)
  Response Header v1:
    correlation_id (INT32)
    TAG_BUFFER
  Response Body:
    throttle_time_ms (INT32)
    topics (COMPACT_ARRAY):
      for each topic:
        error_code (INT16)
        name (COMPACT_NULLABLE_STRING)
        topic_id (UUID - 16 bytes)
        is_internal (BOOLEAN)
        partitions (COMPACT_ARRAY):
          for each partition:
            error_code (INT16)
            partition_index (INT32)
            leader_id (INT32)
            leader_epoch (INT32)
            replica_nodes (COMPACT_ARRAY of INT32)
            isr_nodes (COMPACT_ARRAY of INT32)
            eligible_leader_replicas (COMPACT_ARRAY of INT32)
            last_known_elr (COMPACT_ARRAY of INT32)
            offline_replicas (COMPACT_ARRAY of INT32)
            TAG_BUFFER
        topic_authorized_operations (INT32)
        TAG_BUFFER
    next_cursor (nullable struct):
      0xff = null, otherwise:
        topic_name (COMPACT_STRING)
        partition_index (INT32)
        TAG_BUFFER
    TAG_BUFFER
*/
std::vector<uint8_t> build_describe_topic_partitions_response(
    const DescribeTopicPartitionsRequest& req,
    const std::map<std::string, TopicInfo>& metadata) {
    std::vector<uint8_t> body;

    // throttle_time_ms (INT32)
    // NO throttling
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // topics: COMPACT_ARRAY (length = N - 1)
    body.push_back(static_cast<uint8_t>(req.topic_names.size() + 1));

    std::vector<std::string> sorted_names = req.topic_names;
    std::sort(sorted_names.begin(), sorted_names.end());
    for (const auto& topic_name : sorted_names) {
        auto it = metadata.find(topic_name);
        if (it == metadata.end()) {
            // Topic not found - return ERROR_UNKNOWN_TOPIC_OR_PARTITION
            body.push_back((ERROR_UNKNOWN_TOPIC_OR_PARTITION >> 8) & 0xFF);
            body.push_back(ERROR_UNKNOWN_TOPIC_OR_PARTITION & 0xFF);

            // name: COMPACT_NULLABLE_STRING
            body.push_back(static_cast<uint8_t>(topic_name.size() + 1));
            body.insert(body.end(), topic_name.begin(), topic_name.end());

            // topic_id: UUID (16 bytes of zeros)
            for (int i = 0; i < 16; ++i) body.push_back(0x00);

            // is_internal: BOOLEAN (false)
            body.push_back(0x00);

            // partition: COMPACT_ARRAY (empty = 1)
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
            // actual len is encoded as len + 1, as 0 is reserved for null
            body.push_back(static_cast<uint8_t>(topic_name.size() + 1));
            body.insert(body.end(), topic_name.begin(), topic_name.end());

            // topic_id: UUID (16 bytes)
            body.insert(body.end(), info.topic_id.begin(), info.topic_id.end());

            // is_internal: BOOLEAN (false)
            body.push_back(0x00);

            // partitions: COMPACT_ARRAY (empty = 1, meaning 0 elements)
            // length = N + 1
            body.push_back(static_cast<uint8_t>(info.partitions.size() + 1));

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
                body.push_back(static_cast<uint8_t>(p.replicas.size() + 1));

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

/**
 * Build response with header v0 (just correlation_id) and optional body
 */
std::vector<uint8_t> build_response(int32_t correlation_id,
                                    const std::vector<uint8_t>& body = {}) {
    // response: message_size (4 bytes) + correlation_id (4) + body
    int32_t message_size = 4 + body.size();

    std::vector<uint8_t> res;
    res.reserve(4 + message_size);

    // message size (big endian)
    res.push_back((message_size >> 24) & 0xFF);
    res.push_back((message_size >> 16) & 0xFF);
    res.push_back((message_size >> 8) & 0xFF);
    res.push_back(message_size & 0xFF);

    // correlation_id
    res.push_back((correlation_id >> 24) & 0xFF);
    res.push_back((correlation_id >> 16) & 0xFF);
    res.push_back((correlation_id >> 8) & 0xFF);
    res.push_back(correlation_id & 0xFF);

    // body
    res.insert(res.end(), body.begin(), body.end());

    return res;
}

std::vector<uint8_t> build_api_versions_response_v4(int16_t error_code) {
    std::vector<uint8_t> body;
    // error_code (int16)
    body.push_back((error_code >> 8) & 0xFF);
    body.push_back(error_code & 0xFF);

    // api_keys COMPACT_ARRAY: length = N + 1
    body.push_back(static_cast<uint8_t>(SUPPORTED_API_VERSIONS.size() + 1));

    for (const auto& api : SUPPORTED_API_VERSIONS) {
        // api_key (int16)
        body.push_back((api.api_key >> 8) & 0xFF);
        body.push_back(api.api_key & 0xFF);
        // min_version (int16)
        body.push_back((api.min_version >> 8) & 0xFF);
        body.push_back(api.min_version & 0xFF);
        // max_version (int16)
        body.push_back((api.max_version >> 8) & 0xFF);
        body.push_back(api.max_version & 0xFF);
        // TAG_BUFFER (empty)
        body.push_back(0x00);  // TAG_BUFFER
    }

    // throttle_time_ms (int32)
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // TAG_BUFFER
    body.push_back(0x00);

    return body;
}

bool uses_flexible_header(int16_t api_key, int16_t api_version) {
    if (api_key == API_VERSIONS) return api_version > 3;
    if (api_key == DESCRIBE_TOPIC_PARTITIONS) return true;
    if (api_key == FETCH) return api_version >= 12;
    if (api_key == PRODUCE) return api_version >= 9;
    return false;
}

std::vector<uint8_t> build_response_v1(int32_t correlation_id,
                                       const std::vector<uint8_t>& body = {}) {
    // message_size + correlation_id + TAG_BUFFER + body
    // extra 1 byte is TAG_BUFFER
    int32_t message_size = 4 + 1 + body.size();

    std::vector<uint8_t> res;
    res.reserve(4 +
                message_size);  // 4 bytes for the actual message_size itself

    // message size
    res.push_back((message_size >> 24) & 0xFF);
    res.push_back((message_size >> 16) & 0xFF);
    res.push_back((message_size >> 8) & 0xFF);
    res.push_back(message_size & 0xFF);

    // correlation_id
    res.push_back((correlation_id >> 24) & 0xFF);
    res.push_back((correlation_id >> 16) & 0xFF);
    res.push_back((correlation_id >> 8) & 0xFF);
    res.push_back(correlation_id & 0xFF);

    // TAG_BUFFER
    res.push_back(0x00);

    // body
    res.insert(res.end(), body.begin(), body.end());

    return res;
}

/**
 * Helper to read a COMPACT_ARRAY of INT32 (after `skip_tag_buffer`)
 */
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

std::vector<uint8_t> read_partition_log(const std::string& topic_name,
                                        int32_t partition_id) {
    std::string path = "/tmp/kraft-combined-logs/" + topic_name + "-" +
                       std::to_string(partition_id) +
                       "/00000000000000000000.log";
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return {};
    }

    return {std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>()};
}

/**
 * Append a RecordBatch to the partition log file, updating baseOffset.
 * CRC (offset 17) covers bytes 21+, so modifying baseOffset (0 - 7) is safe.
 * Returns the assigned baseOffset, or -1 on error.
 */
int64_t write_partition_log(const std::string& topic_name, int32_t partition_id,
                            const std::vector<uint8_t>& record_batch) {
    if (record_batch.size() < 61) {
        std::cerr << "RecordBatch too small: " << record_batch.size()
                  << " bytes" << std::endl;
        return -1;
    }

    std::string dir = "/tmp/kraft-combined-logs/" + topic_name + "-" +
                      std::to_string(partition_id);
    std::string path = dir + "/00000000000000000000.log";

    // create directory if it does not exist
    std::filesystem::create_directories(dir);

    // Determine next offset by walking existing batches
    int64_t next_offset = 0;
    auto existing = read_partition_log(topic_name, partition_id);
    if (!existing.empty()) {
        size_t pos = 0;
        while (pos + 61 <= existing.size()) {
            const uint8_t* b = existing.data() + pos;
            int64_t batch_base = ((int64_t)b[0] << 56) | ((int64_t)b[1] << 48) |
                                 ((int64_t)b[2] << 40) | ((int64_t)b[3] << 32) |
                                 ((int64_t)b[4] << 24) | ((int64_t)b[5] << 16) |
                                 ((int64_t)b[6] << 8) | (int64_t)b[7];
            int32_t batch_length =
                (b[8] << 24) | (b[9] << 16) | (b[10] << 8) | b[11];
            if (batch_length <= 0) {
                break;
            }
            size_t total_batch_size = 12 + static_cast<size_t>(batch_length);
            if (pos + total_batch_size > existing.size()) break;

            int32_t last_offset_delta =
                (b[23] << 24) | (b[24] << 16) | (b[25] << 8) | b[26];
            int64_t batch_end = batch_base + last_offset_delta + 1;
            if (batch_end > next_offset) {
                next_offset = batch_end;
            }
            pos += total_batch_size;
        }
    }

    // Patch baseOffset (bytes 0 - 7) in a copy
    std::vector<uint8_t> batch_copy = record_batch;
    for (int i = 0; i < 8; ++i) {
        batch_copy[i] = (next_offset >> (56 - i * 8)) & 0xFF;
    }

    // Append to log file
    // std::ios::app - seek to end before each write
    std::ofstream file(path, std::ios::binary | std::ios::app);
    if (!file) {
        std::cerr << "Failed to open log file: " << path << std::endl;
        return -1;
    }
    file.write(reinterpret_cast<const char*>(batch_copy.data()),
               static_cast<std::streamsize>(batch_copy.size()));
    file.flush();
    if (!file.good()) {
        std::cerr << "Failed to write log file: " << path << std::endl;
        return -1;
    }

    return next_offset;
}

/**
 * Parse a TopicRecord value body (after frame_version, api_key, version).
 * Fields:
 *   - name (COMPACT_STRING)
 *   - topic_id (UUID, 16 bytes)
 *   - TAG_BUFFER
 */
bool parse_topic_record(const uint8_t* data, size_t len, size_t offset,
                        TopicInfo& info) {
    std::optional<std::string> name;
    if (!read_compact_nullable_string(data, len, offset, name)) {
        return false;
    }
    info.name = name.value_or("");
    if (offset + 16 > len) {
        return false;
    }
    std::copy(data + offset, data + offset + 16, info.topic_id.begin());
    offset += 16;
    return skip_tag_buffer(data, len, offset);
}

/**
 * Parse a PartitionRecord value body (after frame_version, api_key, version).
 * Fields:
 *   - partition_id (INT32)
 *   - topic_id (UUID)
 *   - replicas (COMPACT_ARRAY)
 *   - isr (COMPACT_ARRAY)
 *   - removing_replicas (COMPACT_ARRAY)
 *   - adding_replicas (COMPACT_ARRAY)
 *   - leader (INT32)
 *   - leader_epoch (INT32)
 *   - partition_epoch (INT32)
 *   - TAG_BUFFER
 */
bool parse_partition_record(const uint8_t* data, size_t len, size_t offset,
                            std::array<uint8_t, 16>& topic_id,
                            PartitionInfo& info) {
    if (offset + 4 > len) {
        return false;
    }
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
 * Parse Fetch Request v16 body (after header)
 * Body layout:
 *   max_wait_ms (INT32)
 *   min_bytes (INT32)
 *   max_bytes (INT32)
 *   isolation_level (INT8)
 *   session_id (INT32)
 *   session_epoch (INT32)
 *   topics (COMPACT_ARRAY):
 *     topic_id (UUID, 16 bytes)
 *     partitions (COMPACT_ARRAY):
 *       partition (INT32)
 *       current_leader_epoch (INT32)
 *       fetch_offset (INT64)
 *       last_fetched_epoch (INT32)
 *       log_start_offset (INT64)
 *       partition_max_bytes (INT32)
 *       TAG_BUFFER
 *     TAG_BUFFER
 *   forgotten_topics_data (COMPACT_ARRAY) — skipped
 *   rack_id (COMPACT_STRING) — skipped
 *   TAG_BUFFER
 */
bool parse_fetch_request(const uint8_t* data, size_t len, size_t offset,
                         FetchRequest& req) {
    // max_wait_ms(4) + min_bytes(4) + max_bytes(4) + isolation_level(1) +
    // session_id(4) + session_epoch(4) = 21
    if (offset + 21 > len) {
        return false;
    }
    offset += 21;

    // topics: COMPACT_ARRAY
    uint32_t num_topics_raw;
    size_t varint_bytes =
        read_unsigned_varint(data, len, offset, num_topics_raw);
    if (varint_bytes == 0) return false;
    offset += varint_bytes;

    if (num_topics_raw > 0) {
        uint32_t num_topics = num_topics_raw - 1;
        for (uint32_t i = 0; i < num_topics; ++i) {
            // topic_id: UUID (16 bytes)
            if (offset + 16 > len) {
                return false;
            }
            std::array<uint8_t, 16> topic_id;
            std::copy(data + offset, data + offset + 16, topic_id.begin());
            offset += 16;
            req.topics.push_back({topic_id, {}});

            // partitions: COMPACT_ARRAY
            uint32_t num_partitions_raw;
            varint_bytes =
                read_unsigned_varint(data, len, offset, num_partitions_raw);
            if (varint_bytes == 0) return false;
            offset += varint_bytes;

            if (num_partitions_raw > 0) {
                uint32_t num_partitions = num_partitions_raw - 1;
                for (uint32_t j = 0; j < num_partitions; ++j) {
                    // partition(4) + current_leader_epoch (4) + fetch_offset(8)
                    // + last_fetched_epoch(4) + log_start_offset(8) +
                    // partition_max_bytes(4)
                    if (offset + 32 > len) return false;
                    int32_t partition_index =
                        (data[offset] << 24) | (data[offset + 1] << 16) |
                        (data[offset + 2] << 8) | data[offset + 3];
                    req.topics.back().partitions.push_back(partition_index);
                    offset += 32;
                    // TAG_BUFFER
                    if (!skip_tag_buffer(data, len, offset)) return false;
                }
            }

            // TAG_BUFFER per topic
            if (!skip_tag_buffer(data, len, offset)) return false;
        }
    }

    return true;
}

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
        size_t vb = read_unsigned_varint(data, len, offset, num_topics_raw);
        if (vb == 0) return false;
        offset += vb;

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
                vb = read_unsigned_varint(data, len, offset, num_parts_raw);
                if (vb == 0) return false;
                offset += vb;

                if (num_parts_raw > 0) {
                    uint32_t num_parts = num_parts_raw - 1;
                    for (uint32_t j = 0; j < num_parts; ++j) {
                        // partition_index (INT32)
                        if (offset + 4 > len) return false;
                        int32_t part_idx =
                            (data[offset] << 24) | (data[offset + 1] << 16) |
                            (data[offset + 2] << 8) | data[offset + 3];
                        offset += 4;

                        // records (COMPACT_RECORDS): length as unsigned varint
                        uint32_t records_len;
                        vb = read_unsigned_varint(data, len, offset,
                                                  records_len);
                        if (vb == 0) return false;
                        offset += vb;
                        std::vector<uint8_t> rec_data;
                        if (records_len > 0) {
                            uint32_t actual_len = records_len - 1;
                            if (offset + actual_len > len) return false;
                            rec_data.assign(data + offset,
                                            data + offset + actual_len);
                            offset += actual_len;
                        }
                        topic.partitions.push_back(
                            {part_idx, std::move(rec_data)});

                        // TAG_BUFFER
                        if (!skip_tag_buffer(data, len, offset)) {
                            return false;
                        }
                    }
                }
                // TAG_BUFFER
                if (!skip_tag_buffer(data, len, offset)) {
                    return false;
                }

                req.topics.push_back(std::move(topic));
            }
        }
    } else {
        // v0-8: non-flexible encoding
        if (api_version >= 3) {
            // transactional_id (NULLABLE_STRING: INT16 length, -1 = null)
            if (offset + 2 > len) {
                return false;
            }
            int16_t txn_len = (data[offset] << 8) | data[offset + 1];
            offset += 2;
            if (txn_len > 0) {
                if (offset + static_cast<size_t>(txn_len) > len) {
                    return false;
                }
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

        // topic_data: ARRAY
        if (offset + 4 > len) {
            return false;
        }
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
            int32_t num_parts = (data[offset] << 24) |
                                (data[offset + 1] << 16) |
                                (data[offset + 2] << 8) | data[offset + 3];
            offset += 4;

            for (int32_t j = 0; j < num_parts; ++j) {
                // partition_index (INT32)
                if (offset + 4 > len) return false;
                int32_t part_idx = (data[offset] << 24) |
                                   (data[offset + 1] << 16) |
                                   (data[offset + 2] << 8) | data[offset + 3];
                offset += 4;

                // records: RECORDS (INT32 size prefix + data)
                if (offset + 4 > len) return false;
                int32_t records_size =
                    (data[offset] << 24) | (data[offset + 1] << 16) |
                    (data[offset + 2] << 8) | data[offset + 3];
                offset += 4;
                std::vector<uint8_t> rec_data;
                if (records_size > 0) {
                    if (offset + static_cast<size_t>(records_size) > len) {
                        return false;
                    }
                    rec_data.assign(data + offset,
                                    data + offset + records_size);
                    offset += records_size;
                }
                topic.partitions.push_back({part_idx, std::move(rec_data)});
            }

            req.topics.push_back(std::move(topic));
        }
    }

    return true;
}

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
    const ProduceRequest& req, const std::map<std::string, TopicInfo>& metadata,
    int16_t api_version) {
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
            body.push_back(static_cast<uint8_t>(topic.partitions.size() + 1));
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
            // int16_t error =
            // topic_known ? ERROR_NONE : ERROR_UNKNOWN_TOPIC_OR_PARTITION;
            int16_t error = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
            if (topic_known) {
                const TopicInfo& info = it->second;
                for (const auto& p : info.partitions) {
                    if (p.partition_id == part.partition_index) {
                        error = ERROR_NONE;
                        break;
                    }
                }
            }
            body.push_back((error >> 8) & 0xFF);
            body.push_back(error & 0xFF);

            // Compute base_offset and log_start_offset from partition log
            int64_t base_offset = -1;
            int64_t log_start_offset = -1;
            if (error == ERROR_NONE) {
                auto log_data =
                    read_partition_log(topic.name, part.partition_index);
                if (!log_data.empty() && log_data.size() >= 61) {
                    const uint8_t* b = log_data.data();
                    // log_start_offset = baseOffset of first batch
                    log_start_offset =
                        ((int64_t)b[0] << 56) | ((int64_t)b[1] << 48) |
                        ((int64_t)b[2] << 40) | ((int64_t)b[3] << 32) |
                        ((int64_t)b[4] << 24) | ((int64_t)b[5] << 16) |
                        ((int64_t)b[6] << 8) | (int64_t)b[7];

                    // Walk to last batch to get base_offset of just-written
                    // batch
                    size_t pos = 0;
                    int64_t last_batch_base = log_start_offset;
                    while (pos + 61 <= log_data.size()) {
                        const uint8_t* hdr = log_data.data() + pos;
                        last_batch_base =
                            ((int64_t)hdr[0] << 56) | ((int64_t)hdr[1] << 48) |
                            ((int64_t)hdr[2] << 40) | ((int64_t)hdr[3] << 32) |
                            ((int64_t)hdr[4] << 24) | ((int64_t)hdr[5] << 16) |
                            ((int64_t)hdr[6] << 8) | (int64_t)hdr[7];
                        int32_t bl = (hdr[8] << 24) | (hdr[9] << 16) |
                                     (hdr[10] << 8) | hdr[11];
                        if (bl <= 0) break;
                        size_t total = 12 + static_cast<size_t>(bl);
                        if (pos + total > log_data.size()) break;
                        pos += total;
                    }
                    // base_offset = baseOffset of the last batch (just written)
                    base_offset = last_batch_base;
                } else {
                    // empty or missing log - first record goes at offset 0
                    base_offset = 0;
                    log_start_offset = 0;
                }
            }

            // base_offset
            for (int i = 56; i >= 0; i -= 8)
                body.push_back((base_offset >> i) & 0xFF);

            // log_append_time (INT64): -1 (v2+)
            if (api_version >= 2) {
                for (int i = 0; i < 8; ++i) body.push_back(0xFF);
            }

            // log_start_offset (INT64) (v5+)
            if (api_version >= 5) {
                for (int i = 56; i >= 0; i -= 8)
                    body.push_back((log_start_offset >> i) & 0xFF);
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

        // TAG_BUFFER per partition (v9+)
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

    // TAG_BUFFER per partition (v9+)
    if (flexible) {
        body.push_back(0x00);
    }

    return body;
}

/*
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
*/

/**
 * Load cluster metadata from the `__cluster_metadata` log file,
 * which is a sequence of **RecordBatch** frames.
 * Each **RecordBatch** frame:
 * offset:  0  — baseOffset (8 bytes, not used)
 * offset:  8  — batchLength (4 bytes, big-endian)
 * offset: 12  — partitionLeaderEpoch, magic, crc, attributes,
 *                lastOffsetDelta, timestamps, producerId, etc.
 * offset: 57  — recordsCount (4 bytes, big-endian)
 * offset: 61  — records start here
 */
std::map<std::string, TopicInfo> load_cluster_metadata() {
    std::map<std::string, TopicInfo> res;

    std::ifstream file(METADATA_LOG_PATH, std::ios::binary);
    if (!file) {
        std::cerr << "Could not open metadata log: " << METADATA_LOG_PATH
                  << std::endl;
        return res;
    }

    // read entire file stream into memory
    // use the vector range constructor: vector(begin_iterator, end_iter)
    std::vector<uint8_t> file_data{
        // iterator pointing to beginning of the file's stream buffer
        std::istreambuf_iterator<char>(file),
        // default-constructed: the "end" sentinel (EOF)
        std::istreambuf_iterator<char>()};
    file.close();

    // uuid -> TopicInfo
    std::map<std::array<uint8_t, 16>, TopicInfo> by_uuid;

    size_t pos = 0;
    while (pos + 61 <= file_data.size()) {
        const uint8_t* batch = file_data.data() + pos;

        // batchLength at offset 8 (covers from partitionLeaderEpoch through
        // end)
        int32_t batch_length =
            (batch[8] << 24) | (batch[9] << 16) | (batch[10] << 8) | batch[11];
        if (batch_length <= 0) break;

        size_t total_batch_size = 12 + static_cast<size_t>(batch_length);
        if (pos + total_batch_size > file_data.size()) break;

        // recordsCount at offset 57
        int32_t records_count = (batch[57] << 24) | (batch[58] << 16) |
                                (batch[59] << 8) | batch[60];

        // records start at offset 61 within the batch
        size_t rec_offset = pos + 61;
        for (int32_t r = 0; r < records_count; ++r) {
            // Record: length (zigzag varint)
            int32_t rec_len;
            size_t vb = read_zigzag_varint(file_data.data(), file_data.size(),
                                           rec_offset, rec_len);
            if (vb == 0 || rec_len < 0) {
                break;
            }
            rec_offset += vb;  // skip `length` field

            size_t rec_start = rec_offset;
            size_t rec_end = rec_start + static_cast<size_t>(rec_len);
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

                // parse metadta record value
                // frame_version (uvarint), api_key (uvarint), version
                // (uvarint), body
                size_t v_offset = 0;
                uint32_t frame_version, api_key, version;
                size_t fvb = read_unsigned_varint(value_data, value_len,
                                                  v_offset, frame_version);
                if (fvb == 0) {
                    rec_offset = rec_end;
                    continue;
                }
                v_offset += fvb;

                fvb = read_unsigned_varint(value_data, value_len, v_offset,
                                           api_key);
                if (fvb == 0) {
                    rec_offset = rec_end;
                    continue;
                }
                v_offset += fvb;

                fvb = read_unsigned_varint(value_data, value_len, v_offset,
                                           version);
                if (fvb == 0) {
                    rec_offset = rec_end;
                    continue;
                }
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

    // build name -> TopicInfo map
    for (auto& [uuid, info] : by_uuid) {
        if (!info.name.empty()) {
            info.topic_id = uuid;
            res[info.name] = info;
        }
    }

    return res;
}

/**
 * Fetch Response v16 (flexible, response header v1):
 *   throttle_time_ms (INT32)
 *   error_code (INT16)
 *   session_id (INT32)
 *   responses (COMPACT_ARRAY):
 *     topic_id (UUID)
 *     partitions (COMPACT_ARRAY):
 *       partition_index (INT32)
 *       error_code (INT16)
 *       high_watermark (INT64)
 *       last_stable_offset (INT64)
 *       log_start_offset (INT64)
 *       aborted_transactions (COMPACT_NULLABLE_ARRAY)
 *       preferred_read_replica (INT32)
 *       records (COMPACT_RECORDS)
 *       TAG_BUFFER
 *     TAG_BUFFER
 *   TAG_BUFFER
 */
std::vector<uint8_t> build_fetch_response(
    const FetchRequest& req, const std::map<std::string, TopicInfo>& metadata) {
    std::vector<uint8_t> body;

    // build UUID -> TopicInfo lookup
    // why?
    std::map<std::array<uint8_t, 16>, const TopicInfo*> by_uuid;
    for (const auto& [name, info] : metadata) {
        by_uuid[info.topic_id] = &info;
    }

    // throttle_time_ms (INT32)
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // error_code (INT16)
    body.push_back(0x00);
    body.push_back(0x00);

    // session_id (INT32)
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // responses: COMPACT_ARRAY
    body.push_back(static_cast<uint8_t>(req.topics.size() + 1));

    for (const auto& topic : req.topics) {
        // topic_id: UUID (16 bytes)
        body.insert(body.end(), topic.topic_id.begin(), topic.topic_id.end());

        auto it = by_uuid.find(topic.topic_id);
        bool topic_known = (it != by_uuid.end());
        if (topic_known) {
            const TopicInfo* topic_info = it->second;
            // partition partitions: COMPACT_ARRAY
            body.push_back(static_cast<uint8_t>(topic.partitions.size() + 1));
            for (int32_t part_idx : topic.partitions) {
                // partition_index (INT32)
                body.push_back((part_idx >> 24) & 0xFF);
                body.push_back((part_idx >> 16) & 0xFF);
                body.push_back((part_idx >> 8) & 0xFF);
                body.push_back(part_idx & 0xFF);

                auto log_data = read_partition_log(topic_info->name, part_idx);

                if (!log_data.empty() && log_data.size() >= 61) {
                    // compute high_watermark from last RecordBatch in the file
                    // For single-batch case: baseOffset(8 bytes) +
                    // lastOffsetDelta(4 bytes at offset 23) + 1
                    const uint8_t* b = log_data.data();
                    int64_t base_offset =
                        ((int64_t)b[0] << 56) | ((int64_t)b[1] << 48) |
                        ((int64_t)b[2] << 40) | ((int64_t)b[3] << 32) |
                        ((int64_t)b[4] << 24) | ((int64_t)b[5] << 16) |
                        ((int64_t)b[6] << 8) | (int64_t)b[7];
                    int32_t last_offset_data =
                        (b[23] << 24) | (b[24] << 16) | (b[25] << 8) | b[26];
                    int64_t high_watermark = base_offset + last_offset_data + 1;

                    // error_code (INT16): ERROR_NONE
                    body.push_back(0x00);
                    body.push_back(0x00);

                    // high_watermark (INT64)
                    for (int i = 56; i >= 0; i -= 8)
                        body.push_back((high_watermark >> i) & 0xFF);
                    // last_stable_offset (INT64)
                    for (int i = 56; i >= 0; i -= 8)
                        body.push_back((high_watermark >> i) & 0xFF);
                    // log_start_offset (INT64): 0
                    for (int i = 0; i < 8; ++i) body.push_back(0x00);

                    // aborted_transactions: COMPACT_NULLABLE_ARRAY (null = 0)
                    body.push_back(0x00);

                    // preferred_read_replica (INT32): -1
                    body.push_back(0xFF);
                    body.push_back(0xFF);
                    body.push_back(0xFF);
                    body.push_back(0xFF);

                    // records: COMPACT_RECORDS (null = 0)
                    append_unsigned_varint(
                        body, static_cast<uint32_t>(log_data.size() + 1));
                    body.insert(body.end(), log_data.begin(), log_data.end());
                } else {
                    // no data - zero offsets, null records
                    // error_code (INT16): ERROR_NONE
                    body.push_back(0x00);
                    body.push_back(0x00);

                    // high_watermark (INT64): 0
                    for (int i = 0; i < 8; ++i) body.push_back(0x00);
                    // last_stable_offset (INT64): 0
                    for (int i = 0; i < 8; ++i) body.push_back(0x00);
                    // log_start_offset (INT64): 0
                    for (int i = 0; i < 8; ++i) body.push_back(0x00);

                    // aborted_transactions: COMPACT_NULLABLE_ARRAY (null = 0)
                    body.push_back(0x00);

                    // preferred_read_replica (INT32): -1
                    body.push_back(0xFF);
                    body.push_back(0xFF);
                    body.push_back(0xFF);
                    body.push_back(0xFF);

                    // records: COMPACT_RECORDS (null = 0)
                    body.push_back(0x00);
                }

                // TAG_BUFFER (partition)
                body.push_back(0x00);
            }
        } else {
            // Unkown topic - single partition with error
            // partitions: COMPACT_ARRAY (1 element, encoded as 2)
            body.push_back(0x02);

            // partition_index (INT32): 0
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);
            body.push_back(0x00);

            // error_code (INT16): 100 (UNKNOWN_TOPIC_ID)
            body.push_back((ERROR_UNKNOWN_TOPIC_ID >> 8) & 0xFF);
            body.push_back(ERROR_UNKNOWN_TOPIC_ID & 0xFF);

            // high_watermark (INT64): -1
            for (int i = 0; i < 8; ++i) body.push_back(0xFF);
            // last_stable_offset (INT64): -1
            for (int i = 0; i < 8; ++i) body.push_back(0xFF);
            // log_start_offset (INT64): -1
            for (int i = 0; i < 8; ++i) body.push_back(0xFF);

            // aborted_transactions: COMPACT_NULLABLE_ARRAY (null = 0)
            body.push_back(0x00);

            // preferred_read_replica (INT32): -1
            body.push_back(0xFF);
            body.push_back(0xFF);
            body.push_back(0xFF);
            body.push_back(0xFF);

            // records: COMPACT_RECORDS (null = 0)
            body.push_back(0x00);

            // TAG_BUFFER (partition)
            body.push_back(0x00);
        }

        // TAG_BUFFER (topic)
        body.push_back(0x00);
    }

    // TAG_BUFFER
    body.push_back(0x00);

    return body;
}

void handle_client(int client_fd) {
    while (true) {
        // read message size (4 bytes, big-endian)
        uint8_t size_buf[4];
        if (!read_exact(client_fd, size_buf, 4)) {
            break;  // client disconnected or error
        }

        int32_t message_size = (size_buf[0] << 24) | (size_buf[1] << 16) |
                               (size_buf[2] << 8) | size_buf[3];

        if (message_size <= 0 || message_size > 1024 * 1024) {
            std::cerr << "Invalid message size: " << message_size << std::endl;
            break;
        }

        // read request body
        std::vector<uint8_t> request(message_size);
        if (!read_exact(client_fd, request.data(), message_size)) {
            std::cerr << "Failed to request body!" << std::endl;
            break;
        }

        // parse request header
        // need to peek at api_version to decide header format
        // first read fixed header fields to get api_version
        if (request.size() < 4) {
            // peek at least api_key and api_version
            std::cerr << "Request too small!" << std::endl;
            break;
        }

        int16_t api_key = (request[0] << 8) | request[1];
        int16_t api_version = (request[2] << 8) | request[3];
        RequestHeader header;
        size_t header_end_offset = 0;

        // ApiVersions v3+ uses flexible header (v2), v0-2 uses non-flexible
        // (v1)
        bool use_flexible_header = uses_flexible_header(api_key, api_version);
        if (use_flexible_header) {
            if (!parse_request_header_v2(request.data(), request.size(), header,
                                         header_end_offset)) {
                std::cerr << "Failed to parse request header v2!" << std::endl;
                break;
            }

        } else {
            if (!parse_request_header_v1(request.data(), request.size(), header,
                                         header_end_offset)) {
                std::cerr << "Failed to parse request header v1!" << std::endl;
                break;
            }
        }

        std::cerr << "Received request: api_key=" << header.request_api_key
                  << " api_version=" << header.request_api_version
                  << " correlation_id=" << header.correlation_id
                  << " client_id=" << header.client_id << "\n";

        // build and send response with header v0 (empty body for now)
        if (header.request_api_key == API_VERSIONS) {
            // parse request body for v3+
            if (header.request_api_version >= 3) {
                ApiVersionsRequestBody req_body;
                if (!parse_api_versions_request_body_v3(
                        request.data(), request.size(), header_end_offset,
                        req_body)) {
                    std::cerr << "Failed to parse ApiVersions request body!"
                              << std::endl;
                    break;
                }

                std::cerr << "  client_software_name="
                          << req_body.client_software_name
                          << " client_software_version="
                          << req_body.client_softare_version << std::endl;
            }
            int16_t error_code =
                is_version_supported(header.request_api_key,
                                     header.request_api_version)
                    ? ERROR_NONE
                    : ERROR_UNSUPPORTED_VERSION;
            std::vector<uint8_t> body =
                build_api_versions_response_v4(error_code);
            std::vector<uint8_t> res =
                build_response(header.correlation_id, body);
            if (!write_exact(client_fd, res.data(), res.size())) {
                std::cerr << "Failed to send response" << std::endl;
                break;
            }
        } else if (header.request_api_key == DESCRIBE_TOPIC_PARTITIONS) {
            // request body parsing
            DescribeTopicPartitionsRequest req;
            if (!parse_describe_topic_partitions_request(
                    request.data(), request.size(), header_end_offset, req)) {
                std::cerr << "Failed to parse DescribeTopicPartitions request!"
                          << std::endl;
                break;
            }
            std::cerr << "  topics requested:";
            for (const auto& t : req.topic_names) {
                std::cerr << " " << t;
            }
            std::cerr << std::endl;

            // load what topics/partitions actually exist
            auto metadata = load_cluster_metadata();

            std::vector<uint8_t> body =
                build_describe_topic_partitions_response(req, metadata);
            std::vector<uint8_t> res =
                build_response_v1(header.correlation_id, body);
            if (!write_exact(client_fd, res.data(), res.size())) {
                std::cerr << "Failed to send response!" << std::endl;
                break;
            }
        } else if (header.request_api_key == FETCH) {
            FetchRequest req;
            if (!parse_fetch_request(request.data(), request.size(),
                                     header_end_offset, req)) {
                std::cerr << "Failed to parse Fetch request!" << std::endl;
                break;
            }
            auto metadata = load_cluster_metadata();
            std::vector<uint8_t> body = build_fetch_response(req, metadata);
            std::vector<uint8_t> res =
                build_response_v1(header.correlation_id, body);
            if (!write_exact(client_fd, res.data(), res.size())) {
                std::cerr << "Failed to send Fetch response!" << std::endl;
                break;
            }
        } else if (header.request_api_key == PRODUCE) {
            ProduceRequest req;
            if (!parse_produce_request(request.data(), request.size(),
                                       header_end_offset, req,
                                       header.request_api_version)) {
                std::cerr << "Failed to parse Produce request!" << std::endl;
                break;
            }

            auto metadata = load_cluster_metadata();

            // Write record batches to disk before building resposne
            for (const auto& topic : req.topics) {
                auto it = metadata.find(topic.name);
                if (it == metadata.end()) continue;

                const TopicInfo& info = it->second;
                for (const auto& part : topic.partitions) {
                    if (part.records.empty()) continue;

                    bool partition_exists = false;
                    for (const auto& p : info.partitions) {
                        if (p.partition_id == part.partition_index) {
                            partition_exists = true;
                            break;
                        }
                    }
                    if (!partition_exists) continue;

                    int64_t assigned = write_partition_log(
                        topic.name, part.partition_index, part.records);
                    if (assigned < 0) {
                        std::cerr << "Failed to write records for "
                                  << topic.name << "-" << part.partition_index
                                  << std::endl;
                    }
                }
            }

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
    }
}

int main(/* int argc, char* argv[] */) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr),
             sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    std::cerr << "Logs from your program will appear here!\n";

    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);

        int client_fd =
            accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr),
                   &client_addr_len);
        if (client_fd < 0) {
            std::cerr << "accept failed!\n";
            continue;
        }
        std::cout << "Client connected\n";
        std::thread([client_fd]() {
            handle_client(client_fd);
            close(client_fd);
            std::cerr << "Client disconnected\n";
        }).detach();
    }

    close(server_fd);
    return 0;
}
