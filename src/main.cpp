#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// the ApiVersions request
constexpr int16_t API_VERSIONS = 18;
// DescribeTopicPartitions
constexpr int16_t DESCRIBE_TOPIC_PARTITIONS = 75;
constexpr int16_t ERROR_UNKNOWN_TOPIC_OR_PARTITION = 3;

constexpr int16_t ERROR_NONE = 0;
constexpr int16_t ERROR_UNSUPPORTED_VERSION = 35;

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
    // add more here
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

/**
 * Read COMPACT_NULLABLE_STRING from buffer
 * Encoding: unsigned varint length (0=null, N=N-1 chars) + data
 */
bool read_compact_nullable_string(const uint8_t* data, size_t len,
                                  size_t& offset, std::string& out) {
    uint32_t length;
    size_t varint_bytes = read_unsigned_varint(data, len, offset, length);
    if (varint_bytes == 0) {
        return false;
    }
    offset += varint_bytes;

    if (length == 0) {
        out = "";  // null string
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
    if (!read_compact_nullable_string(data, len, offset,
                                      body.client_software_name)) {
        return false;
    }
    // client_software_version (COMPACT_STRING)
    if (!read_compact_nullable_string(data, len, offset,
                                      body.client_softare_version)) {
        return false;
    }

    // TAG_BUFFER
    if (!skip_tag_buffer(data, len, offset)) {
        return false;
    }

    return true;
}

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
            std::string topic_name;
            if (!read_compact_nullable_string(data, len, offset, topic_name)) {
                return false;
            }
            req.topic_names.push_back(std::move(topic_name));

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
        std::string cursor_topic;
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
    const DescribeTopicPartitionsRequest& req) {
    std::vector<uint8_t> body;

    // throttle_time_ms (INT32)
    // NO throttling
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);
    body.push_back(0x00);

    // topics: COMPACT_ARRAY (length = N - 1)
    body.push_back(static_cast<uint8_t>(req.topic_names.size() + 1));

    for (const auto& topic_name : req.topic_names) {
        // error_code (INT16)
        // because NO topics actually exist in this broker
        body.push_back((ERROR_UNKNOWN_TOPIC_OR_PARTITION >> 8) & 0xFF);
        body.push_back(ERROR_UNKNOWN_TOPIC_OR_PARTITION & 0xFF);

        // name: COMPACT_NULLABLE_STRING
        // actual len is encoded as len + 1, as 0 is reserved for null
        body.push_back(static_cast<uint8_t>(topic_name.size() + 1));
        body.insert(body.end(), topic_name.begin(), topic_name.end());

        // topic_id: UUID (16 bytes of zeros)
        for (int i = 0; i < 16; ++i) {
            body.push_back(0x00);  // since topic does not exist
        }

        // is_interal: BOOLEAN (false)
        body.push_back(0x00);

        // partitions: COMPACT_ARRAY (empty = 1, meaning 0 elements)
        body.push_back(0x01);

        // topic_authorized_operations: INT32
        body.push_back(0x00);
        body.push_back(0x00);
        body.push_back(0x00);
        body.push_back(0x00);

        // TAG_BUFFER
        body.push_back(0.00);
    }

    // next_cursor: null
    body.push_back(0xff);

    // TAG_BUFFER
    body.push_back(0.00);

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

            std::vector<uint8_t> body =
                build_describe_topic_partitions_response(req);
            std::vector<uint8_t> res =
                build_response_v1(header.correlation_id, body);
            if (!write(client_fd, res.data(), res.size())) {
                std::cerr << "Failed to send response!" << std::endl;
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
