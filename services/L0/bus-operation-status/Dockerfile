FROM python:3.12-slim

WORKDIR /app

COPY L0/bus-operation-status/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ufcity_microservice_lib /app/ufcity_microservice_lib
COPY L0/bus-operation-status/service.py /app/bus-operation-status/service.py
COPY L0/bus-operation-status/bus_status_model.joblib /app/bus-operation-status/bus_status_model.joblib

CMD ["python", "/app/bus-operation-status/service.py"]
