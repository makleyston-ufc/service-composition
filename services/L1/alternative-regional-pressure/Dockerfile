FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt==2.1.0 \
    joblib==1.5.3 \
    numpy==2.4.3 \
    pandas==3.0.1 \
    scikit-learn==1.8.0

COPY ufcity_microservice_lib /app/ufcity_microservice_lib
COPY L1/alternative-regional-pressure /app/service

CMD ["python", "/app/service/service.py"]
