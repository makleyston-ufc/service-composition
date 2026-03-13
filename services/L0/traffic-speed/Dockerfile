FROM python:3.12-slim

WORKDIR /app

COPY L0/traffic-speed/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ufcity_microservice_lib /app/ufcity_microservice_lib
COPY L0/traffic-speed /app/service

CMD ["python", "/app/service/service.py"]
