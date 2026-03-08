#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include <mosquitto.h>

namespace {
constexpr const char* kBrokerHost = "mosquitto";
constexpr int kBrokerPort = 1883;
constexpr int kKeepalive = 30;
constexpr const char* kPubTopic = "transport/bus/telemetry";
constexpr const char* kSubTopic = "commands/bus";

void on_connect(mosquitto* m, void* /*obj*/, int rc) {
    if (rc == 0) {
        std::cout << "[bus] conectado ao broker MQTT" << std::endl;
        mosquitto_subscribe(m, nullptr, kSubTopic, 0);
    } else {
        std::cerr << "[bus] falha na conexão, código=" << rc << std::endl;
    }
}

void on_message(mosquitto* /*m*/, void* /*obj*/, const mosquitto_message* msg) {
    if (!msg || !msg->payload) return;
    std::string payload(static_cast<char*>(msg->payload), msg->payloadlen);
    std::cout << "[bus] comando recebido em " << msg->topic << ": " << payload << std::endl;
}
}  // namespace

int main() {
    mosquitto_lib_init();

    mosquitto* mosq = mosquitto_new("producer-bus", true, nullptr);
    if (!mosq) {
        std::cerr << "[bus] erro ao criar cliente mosquitto" << std::endl;
        return 1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_message_callback_set(mosq, on_message);

    if (mosquitto_connect(mosq, kBrokerHost, kBrokerPort, kKeepalive) != MOSQ_ERR_SUCCESS) {
        std::cerr << "[bus] não foi possível conectar no broker" << std::endl;
        mosquitto_destroy(mosq);
        mosquitto_lib_cleanup();
        return 1;
    }

    mosquitto_loop_start(mosq);

    int sequence = 0;
    while (true) {
        std::string message = "bus_online seq=" + std::to_string(sequence++);
        mosquitto_publish(mosq, nullptr, kPubTopic, static_cast<int>(message.size()), message.c_str(), 0, false);
        std::cout << "[bus] publicou: " << message << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    mosquitto_loop_stop(mosq, true);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}
