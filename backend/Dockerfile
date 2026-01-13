FROM python:3.12-slim

ARG HOST_UID=1000
ARG HOST_GID=1000

RUN apt-get update && apt-get install -y \
    libmagickwand-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /backend

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN groupadd -g ${HOST_GID} appgroup || true && \
    useradd -u ${HOST_UID} -g ${HOST_GID} -m appuser || true

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
