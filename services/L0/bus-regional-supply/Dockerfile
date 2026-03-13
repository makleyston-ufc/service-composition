FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt==2.1.0 \
    joblib==1.5.3 \
    numpy==2.2.6 \
    scikit-learn==1.7.2

COPY ufcity_microservice_lib /app/ufcity_microservice_lib
COPY L0/bus-regional-supply /app/service

CMD ["python", "/app/service/service.py"]
