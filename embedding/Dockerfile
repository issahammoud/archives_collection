FROM vllm/vllm-openai:latest

WORKDIR /embedding

COPY requirements.txt /embedding/requirements.txt

RUN pip3 install --no-cache-dir -r /embedding/requirements.txt

COPY . /embedding

RUN python3 - <<'EOF'
from transformers import AutoModel
AutoModel.from_pretrained(
    "jinaai/jina-embeddings-v3",
    trust_remote_code=True
)
EOF

EXPOSE 8000

ENTRYPOINT ["uvicorn"]
CMD ["main:app", "--host", "0.0.0.0", "--port", "8000"]
