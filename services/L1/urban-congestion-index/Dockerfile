FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt==2.1.0 joblib==1.5.3

COPY ufcity_microservice_lib /app/ufcity_microservice_lib
COPY L1/urban-congestion-index /app/service

CMD ["python", "/app/service/service.py"]
