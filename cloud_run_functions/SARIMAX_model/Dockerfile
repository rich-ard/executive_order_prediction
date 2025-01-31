FROM python:3.12.8-bookworm

# displays logs immediately to the stream, they wont be part of the buffer
ENV PYTHONUNBUFFERED=True

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY model_building.py model_building.py
COPY .env .env

ENTRYPOINT ["/usr/bin/env", "python", "model_building.py"]