FROM python:3.12-slim

WORKDIR /app

COPY L0/traffic-flow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ufcity_microservice_lib /app/ufcity_microservice_lib
COPY L0/traffic-flow/service.py /app/traffic-flow/service.py
COPY L0/traffic-flow/traffic_flow_model.joblib /app/traffic-flow/traffic_flow_model.joblib

CMD ["python", "/app/traffic-flow/service.py"]
