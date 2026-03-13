#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <filesystem>
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
namespace fs = std::filesystem;

constexpr const char* kBrokerHost = "mosquitto";
constexpr int kBrokerPort = 1883;
constexpr int kKeepalive = 30;
constexpr const char* kPubTopic = "transport/radar/telemetry";
constexpr const char* kSubTopic = "commands/radar";
constexpr const char* kDatasetDir = "dataset/20230901";
constexpr const char* kStreamId = "urn:stream:transport:radar:traffic-observations";
constexpr int kPublishIntervalSeconds = 1;

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
        std::cout << "[radar] conectado ao broker MQTT" << std::endl;
        mosquitto_subscribe(m, nullptr, g_sub_topic.c_str(), 0);
    } else {
        std::cerr << "[radar] falha na conexão, código=" << rc << std::endl;
    }
}

void on_message(mosquitto* /*m*/, void* /*obj*/, const mosquitto_message* msg) {
    if (!msg || !msg->payload) return;
    std::string payload(static_cast<char*>(msg->payload), msg->payloadlen);
    std::cout << "[radar] comando recebido em " << msg->topic << ": " << payload << std::endl;
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

std::string json_escape(const std::string& s) {
    std::ostringstream oss;
    for (char c : s) {
        switch (c) {
            case '\\': oss << "\\\\"; break;
            case '"': oss << "\\\""; break;
            case '\b': oss << "\\b"; break;
            case '\f': oss << "\\f"; break;
            case '\n': oss << "\\n"; break;
            case '\r': oss << "\\r"; break;
            case '\t': oss << "\\t"; break;
            default: oss << c; break;
        }
    }
    return oss.str();
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

std::string get_value(const Row& row, const char* key) {
    auto it = row.find(key);
    return it == row.end() ? "" : trim(it->second);
}

std::string timestamp_with_offset(const std::string& date_time, const std::string& millisecond) {
    if (date_time.size() != 19) return "";
    if (millisecond.empty() || !is_number_literal(millisecond)) return "";

    int ms = 0;
    try {
        ms = std::stoi(millisecond);
    } catch (...) {
        return "";
    }

    if (ms < 0) ms = 0;
    if (ms > 999) ms = 999;

    std::ostringstream oss;
    oss << date_time << '.';
    if (ms < 100) oss << '0';
    if (ms < 10) oss << '0';
    oss << ms << "-03:00";
    return oss.str();
}

std::string observation_id(const Row& row) {
    std::string eqp = get_value(row, "ID EQP");
    std::string date_time = get_value(row, "DATA HORA");
    std::string millisecond = get_value(row, "MILESEGUNDO");

    for (char& c : date_time) {
        if (c == ':') c = '-';
    }

    return "urn:radar:observation:" +
           (eqp.empty() ? std::string("unknown") : eqp) + ':' +
           (date_time.empty() ? std::string("unknown-time") : date_time) + '.' +
           (millisecond.empty() ? std::string("000") : millisecond);
}

void append_json_number_or_string(std::ostringstream& oss,
                                  const std::string& key,
                                  const std::string& value,
                                  bool& first) {
    if (value.empty()) return;
    if (!first) oss << ',';
    first = false;

    oss << '"' << json_escape(key) << "\":";
    if (is_number_literal(value)) {
        oss << value;
    } else {
        oss << '"' << json_escape(value) << '"';
    }
}

std::string build_iotstream_message(const Row& row) {
    const std::string id_eqp = get_value(row, "ID EQP");
    const std::string data_hora = get_value(row, "DATA HORA");
    const std::string milisegundo = get_value(row, "MILESEGUNDO");
    const std::string id_endereco = get_value(row, "ID DE ENDEREÇO");
    const std::string result_time = timestamp_with_offset(data_hora, milisegundo);

    std::ostringstream msg;
    msg << '{';
    msg << "\"@context\":{";
    msg << "\"sosa\":\"http://www.w3.org/ns/sosa/\",";
    msg << "\"iotstream\":\"https://w3id.org/iot/ontology/iot-stream#\",";
    msg << "\"xsd\":\"http://www.w3.org/2001/XMLSchema#\"";
    msg << "},";
    msg << "\"@id\":\"" << json_escape(observation_id(row)) << "\",";
    msg << "\"@type\":\"sosa:Observation\",";
    msg << "\"iotstream:stream\":\"" << kStreamId << "\",";
    msg << "\"sosa:madeBySensor\":\"urn:radar:sensor:"
        << json_escape(id_eqp.empty() ? std::string("unknown") : id_eqp) << "\",";
    msg << "\"sosa:hasFeatureOfInterest\":\"urn:radar:location:"
        << json_escape(id_endereco.empty() ? std::string("unknown") : id_endereco) << "\",";

    if (!result_time.empty()) {
        msg << "\"sosa:resultTime\":{";
        msg << "\"@type\":\"xsd:dateTime\",";
        msg << "\"@value\":\"" << json_escape(result_time) << "\"";
        msg << "},";
    }

    msg << "\"sosa:hasSimpleResult\":{";
    bool first = true;
    append_json_number_or_string(msg, "idEquipamento", id_eqp, first);
    append_json_number_or_string(msg, "dataHora", data_hora, first);
    append_json_number_or_string(msg, "milisegundo", milisegundo, first);
    append_json_number_or_string(msg, "faixa", get_value(row, "FAIXA"), first);
    append_json_number_or_string(msg, "idEndereco", id_endereco, first);
    append_json_number_or_string(msg, "velocidadeVia", get_value(row, "VELOCIDADE DA VIA"), first);
    append_json_number_or_string(msg, "velocidadeAferida", get_value(row, "VELOCIDADE AFERIDA"), first);
    append_json_number_or_string(msg, "classificacao", get_value(row, "CLASSIFICAÇÃO"), first);
    append_json_number_or_string(msg, "tamanho", get_value(row, "TAMANHO"), first);
    append_json_number_or_string(msg, "numeroSerie", get_value(row, "NUMERO DE SÉRIE"), first);
    append_json_number_or_string(msg, "latitude", get_value(row, "LATITUDE"), first);
    append_json_number_or_string(msg, "longitude", get_value(row, "LONGITUDE"), first);
    append_json_number_or_string(msg, "endereco", get_value(row, "ENDEREÇO"), first);
    append_json_number_or_string(msg, "sentido", get_value(row, "SENTIDO"), first);
    msg << '}';
    msg << '}';
    return msg.str();
}

std::vector<fs::path> dataset_files(const std::string& directory) {
    std::vector<fs::path> files;
    std::error_code ec;
    if (!fs::exists(directory, ec) || !fs::is_directory(directory, ec)) {
        std::cerr << "[radar] diretório de dataset inválido: " << directory << std::endl;
        return files;
    }

    for (const auto& entry : fs::directory_iterator(directory, ec)) {
        if (ec) break;
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() == ".json") {
            files.push_back(entry.path());
        }
    }

    std::sort(files.begin(), files.end());
    return files;
}

std::vector<Row> load_rows_from_file(const fs::path& file) {
    std::ifstream in(file);
    if (!in.is_open()) {
        std::cerr << "[radar] falha ao abrir dataset: " << file << std::endl;
        return {};
    }

    std::ostringstream buffer;
    buffer << in.rdbuf();
    auto rows = parse_dataset_json(buffer.str());
    if (rows.empty()) {
        std::cerr << "[radar] arquivo vazio ou inválido: " << file << std::endl;
        return {};
    }

    std::cout << "[radar] carregou " << rows.size() << " registros de "
              << file.filename().string() << std::endl;
    return rows;
}

bool publish_row(mosquitto* mosq,
                 const Row& row,
                 std::size_t item_index,
                 std::size_t total_items,
                 const std::string& file_name) {
    std::string message = build_iotstream_message(row);
    const int rc = mosquitto_publish(mosq,
                                     nullptr,
                                     g_pub_topic.c_str(),
                                     static_cast<int>(message.size()),
                                     message.c_str(),
                                     0,
                                     false);
    if (rc != MOSQ_ERR_SUCCESS) {
        std::cerr << "[radar] falha ao publicar item " << item_index + 1
                  << " de " << file_name << ", código=" << rc << std::endl;
        return false;
    }

    std::cout << "[radar] publicou item " << item_index + 1 << '/' << total_items
              << " de " << file_name << ": " << message << std::endl;
    return true;
}
}  // namespace

int main() {
    const std::string broker_host = read_env_string("MQTT_HOST", kBrokerHost);
    const int broker_port = read_env_int("MQTT_PORT", kBrokerPort);
    g_sub_topic = read_env_string("MQTT_SUB_TOPIC", kSubTopic);
    g_pub_topic = read_env_string("MQTT_PUB_TOPIC", kPubTopic);

    const auto files = dataset_files(kDatasetDir);
    if (files.empty()) {
        std::cerr << "[radar] nenhum arquivo json encontrado em " << kDatasetDir << std::endl;
        return 1;
    }

    mosquitto_lib_init();

    mosquitto* mosq = mosquitto_new("producer-radar", true, nullptr);
    if (!mosq) {
        std::cerr << "[radar] erro ao criar cliente mosquitto" << std::endl;
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);

    int connect_rc = MOSQ_ERR_UNKNOWN;
    while ((connect_rc = mosquitto_connect(mosq, broker_host.c_str(), broker_port, kKeepalive)) != MOSQ_ERR_SUCCESS) {
        std::cerr << "[radar] não foi possível conectar no broker (rc=" << connect_rc
                  << "). Tentando novamente em 2s..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    mosquitto_loop_start(mosq);

    while (true) {
        for (const auto& file : files) {
            auto rows = load_rows_from_file(file);
            if (rows.empty()) continue;

            for (std::size_t idx = 0; idx < rows.size(); ++idx) {
                if (!publish_row(mosq, rows[idx], idx, rows.size(), file.filename().string())) {
                    mosquitto_loop_stop(mosq, true);
                    mosquitto_destroy(mosq);
                    mosquitto_lib_cleanup();
                    return 1;
                }
                std::this_thread::sleep_for(std::chrono::seconds(kPublishIntervalSeconds));
            }
        }
    }

    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}
