FROM python:3.12-slim

ARG HOST_UID=1000
ARG HOST_GID=1000

WORKDIR /webapp

RUN apt-get update \
    && apt-get install -y libmagickwand-dev curl htop \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /webapp/requirements.txt
RUN pip install --no-cache-dir -r /webapp/requirements.txt

COPY . /webapp/

RUN groupadd --gid ${HOST_GID} appgroup \
    && useradd --uid ${HOST_UID} --gid ${HOST_GID} \
               --home-dir /webapp --create-home \
               --shell /bin/bash appuser \
    && chown -R appuser:appgroup /webapp

USER appuser

ENV PYTHONPATH=/webapp

EXPOSE 8050
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8050", "src.main.app:server"]
