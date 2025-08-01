ARG EMBEDDING_MODE=NONE
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}

RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /embedding

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY requirements-gpu.txt ./

COPY . /embedding

RUN --mount=type=cache,id=hf_cache,target=/root/.cache/huggingface \
    if [ "$EMBEDDING_MODE" = "GPU" ]; then \
      pip install --no-cache-dir -r requirements-gpu.txt && \
      python3 download_model.py; \
    else \
      echo "== Skipping GPU deps (mode=$EMBEDDING_MODE) =="; \
    fi


EXPOSE 8000

ENTRYPOINT ["uvicorn"]
CMD ["main:app", "--host", "0.0.0.0", "--port", "8000"]
