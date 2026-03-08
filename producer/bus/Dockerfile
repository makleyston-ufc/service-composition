FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ cmake make libmosquitto-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY main.cpp /app/main.cpp
RUN g++ -O2 -std=c++17 /app/main.cpp -lmosquitto -o /app/producer-bus

CMD ["/app/producer-bus"]
