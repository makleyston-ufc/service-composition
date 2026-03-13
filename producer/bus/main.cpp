#include <chrono>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <mosquitto.h>

namespace {
constexpr const char* kBrokerHost = "mosquitto";
constexpr int kBrokerPort = 1883;
constexpr int kKeepalive = 30;
constexpr const char* kPubTopic = "transport/bus/telemetry";
constexpr const char* kSubTopic = "commands/bus";
constexpr const char* kDatasetPath = "dataset/tempo_real_convencional_json_070326052133.json";
constexpr const char* kStreamId = "urn:stream:transport:bus:tempo-real";
constexpr int kPublishIntervalSeconds = 5;

using Row = std::unordered_map<std::string, std::string>;
std::string g_sub_topic = kSubTopic;
std::string g_pub_topic = kPubTopic;

std::string read_env_string(const char* key, const char* fallback) {
    const char* value = std::getenv(key);
    return (value && *value) ? std::string(value) : std::string(fallback);
}

int read_env_int(const char* key, int fallback) {
    const char* value = std::getenv(key);
    if (!value || !*value) return fallback;
    try {
        return std::stoi(value);
    } catch (...) {
        return fallback;
    }
}

void on_connect(mosquitto* m, void* /*obj*/, int rc) {
    if (rc == 0) {
        std::cout << "[bus] conectado ao broker MQTT" << std::endl;
        mosquitto_subscribe(m, nullptr, g_sub_topic.c_str(), 0);
    } else {
        std::cerr << "[bus] falha na conexão, código=" << rc << std::endl;
    }
}

void on_message(mosquitto* /*m*/, void* /*obj*/, const mosquitto_message* msg) {
    if (!msg || !msg->payload) return;
    std::string payload(static_cast<char*>(msg->payload), msg->payloadlen);
    std::cout << "[bus] comando recebido em " << msg->topic << ": " << payload << std::endl;
}

bool is_number_literal(const std::string& s) {
    if (s.empty()) return false;
    std::size_t i = 0;
    if (s[i] == '+' || s[i] == '-') ++i;
    bool has_digit = false;
    bool has_dot = false;
    for (; i < s.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(s[i]);
        if (std::isdigit(c)) {
            has_digit = true;
            continue;
        }
        if (c == '.' && !has_dot) {
            has_dot = true;
            continue;
        }
        return false;
    }
    return has_digit;
}

std::string trim(const std::string& s) {
    std::size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
    std::size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return s.substr(start, end - start);
}

bool parse_string_token(const std::string& text, std::size_t& i, std::string& out) {
    if (i >= text.size() || text[i] != '"') return false;
    ++i;
    out.clear();
    while (i < text.size()) {
        char c = text[i++];
        if (c == '\\') {
            if (i >= text.size()) return false;
            char esc = text[i++];
            switch (esc) {
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                case '/': out.push_back('/'); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                default: return false;
            }
            continue;
        }
        if (c == '"') return true;
        out.push_back(c);
    }
    return false;
}

void skip_spaces(const std::string& text, std::size_t& i) {
    while (i < text.size() && std::isspace(static_cast<unsigned char>(text[i]))) ++i;
}

std::vector<Row> parse_dataset_json(const std::string& text) {
    std::vector<Row> rows;
    std::size_t i = 0;
    skip_spaces(text, i);
    if (i >= text.size() || text[i] != '[') return rows;
    ++i;

    while (i < text.size()) {
        skip_spaces(text, i);
        if (i < text.size() && text[i] == ']') break;
        if (i >= text.size() || text[i] != '{') break;
        ++i;

        Row row;
        while (i < text.size()) {
            skip_spaces(text, i);
            if (i < text.size() && text[i] == '}') {
                ++i;
                rows.push_back(std::move(row));
                break;
            }

            std::string key;
            if (!parse_string_token(text, i, key)) return rows;
            skip_spaces(text, i);
            if (i >= text.size() || text[i] != ':') return rows;
            ++i;
            skip_spaces(text, i);

            std::string value;
            if (i < text.size() && text[i] == '"') {
                if (!parse_string_token(text, i, value)) return rows;
            } else {
                std::size_t start = i;
                while (i < text.size() && text[i] != ',' && text[i] != '}') ++i;
                value = trim(text.substr(start, i - start));
            }

            row[key] = value;

            skip_spaces(text, i);
            if (i < text.size() && text[i] == ',') {
                ++i;
                continue;
            }
            if (i < text.size() && text[i] == '}') {
                ++i;
                rows.push_back(std::move(row));
                break;
            }
        }

        skip_spaces(text, i);
        if (i < text.size() && text[i] == ',') {
            ++i;
            continue;
        }
        if (i < text.size() && text[i] == ']') break;
    }

    return rows;
}

std::string hr_to_iso8601_br(const std::string& hr) {
    if (hr.size() != 14) return "";
    for (char c : hr) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return "";
    }

    std::ostringstream oss;
    oss << hr.substr(0, 4) << '-' << hr.substr(4, 2) << '-' << hr.substr(6, 2)
        << 'T' << hr.substr(8, 2) << ':' << hr.substr(10, 2) << ':' << hr.substr(12, 2)
        << "-03:00";
    return oss.str();
}

void append_json_number_or_string(std::ostringstream& oss, const std::string& key, const std::string& value, bool& first) {
    if (value.empty()) return;
    if (!first) oss << ',';
    first = false;

    oss << '"' << key << "\":";
    if (is_number_literal(value)) {
        oss << value;
    } else {
        oss << '"' << value << '"';
    }
}

std::string build_iotstream_message(const Row& row) {
    auto get = [&](const char* key) -> std::string {
        auto it = row.find(key);
        return it == row.end() ? "" : it->second;
    };

    const std::string nv = get("NV");
    const std::string hr = get("HR");
    const std::string iso = hr_to_iso8601_br(hr);

    std::ostringstream msg;
    msg << '{';
    msg << "\"@context\":{";
    msg << "\"sosa\":\"http://www.w3.org/ns/sosa/\",";
    msg << "\"iotstream\":\"https://w3id.org/iot/ontology/iot-stream#\",";
    msg << "\"xsd\":\"http://www.w3.org/2001/XMLSchema#\"";
    msg << "},";

    msg << "\"@id\":\"urn:bus:obs:" << (nv.empty() ? "unknown" : nv) << ':'
        << (hr.empty() ? "unknown" : hr) << "\",";
    msg << "\"@type\":\"sosa:Observation\",";
    msg << "\"iotstream:stream\":\"" << kStreamId << "\",";
    msg << "\"sosa:hasFeatureOfInterest\":\"urn:bus:vehicle:"
        << (nv.empty() ? "unknown" : nv) << "\",";

    if (!iso.empty()) {
        msg << "\"sosa:resultTime\":{";
        msg << "\"@type\":\"xsd:dateTime\",";
        msg << "\"@value\":\"" << iso << "\"";
        msg << "},";
    }

    msg << "\"sosa:hasSimpleResult\":{";
    bool first = true;
    append_json_number_or_string(msg, "EV", get("EV"), first);
    append_json_number_or_string(msg, "HR", get("HR"), first);
    append_json_number_or_string(msg, "LT", get("LT"), first);
    append_json_number_or_string(msg, "LG", get("LG"), first);
    append_json_number_or_string(msg, "NV", get("NV"), first);
    append_json_number_or_string(msg, "VL", get("VL"), first);
    append_json_number_or_string(msg, "NL", get("NL"), first);
    append_json_number_or_string(msg, "DG", get("DG"), first);
    append_json_number_or_string(msg, "SV", get("SV"), first);
    append_json_number_or_string(msg, "DT", get("DT"), first);
    msg << '}';

    msg << '}';
    return msg.str();
}

std::vector<Row> load_rows_from_dataset(const std::string& path) {
    std::ifstream in(path);
    if (!in.is_open()) {
        std::cerr << "[bus] falha ao abrir dataset: " << path << std::endl;
        return {};
    }

    std::ostringstream buffer;
    buffer << in.rdbuf();
    auto rows = parse_dataset_json(buffer.str());
    if (rows.empty()) {
        std::cerr << "[bus] dataset vazio ou inválido: " << path << std::endl;
    } else {
        std::cout << "[bus] dataset carregado com " << rows.size() << " registros" << std::endl;
    }
    return rows;
}
}  // namespace

int main() {
    const std::string broker_host = read_env_string("MQTT_HOST", kBrokerHost);
    const int broker_port = read_env_int("MQTT_PORT", kBrokerPort);
    g_sub_topic = read_env_string("MQTT_SUB_TOPIC", kSubTopic);
    g_pub_topic = read_env_string("MQTT_PUB_TOPIC", kPubTopic);

    auto rows = load_rows_from_dataset(kDatasetPath);
    if (rows.empty()) return 1;

    mosquitto_lib_init();

    mosquitto* mosq = mosquitto_new("producer-bus", true, nullptr);
    if (!mosq) {
        std::cerr << "[bus] erro ao criar cliente mosquitto" << std::endl;
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);

    int connect_rc = MOSQ_ERR_UNKNOWN;
    while ((connect_rc = mosquitto_connect(mosq, broker_host.c_str(), broker_port, kKeepalive)) != MOSQ_ERR_SUCCESS) {
        std::cerr << "[bus] não foi possível conectar no broker (rc=" << connect_rc
                  << "). Tentando novamente em 2s..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    mosquitto_loop_start(mosq);

    std::size_t idx = 0;
    while (true) {
        const Row& row = rows[idx];
        std::string message = build_iotstream_message(row);

        const int pub_rc = mosquitto_publish(mosq,
                                             nullptr,
                                             g_pub_topic.c_str(),
                                             static_cast<int>(message.size()),
                                             message.c_str(),
                                             0,
                                             false);
        if (pub_rc != MOSQ_ERR_SUCCESS) {
            std::cerr << "[bus] falha ao publicar item " << idx + 1 << " (rc=" << pub_rc << ")" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }

        auto it_nv = row.find("NV");
        auto it_hr = row.find("HR");
        std::cout << "[bus] publicou item " << idx + 1 << '/' << rows.size()
                  << " (NV=" << (it_nv != row.end() ? it_nv->second : "?")
                  << ", HR=" << (it_hr != row.end() ? it_hr->second : "?") << ')' << std::endl;

        idx = (idx + 1) % rows.size();
        std::this_thread::sleep_for(std::chrono::seconds(kPublishIntervalSeconds));
    }

    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}
