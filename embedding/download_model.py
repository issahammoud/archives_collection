from transformers import AutoModel

AutoModel.from_pretrained(
    "jinaai/jina-embeddings-v3",
    trust_remote_code=True
)