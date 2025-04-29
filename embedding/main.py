import orjson
import numpy as np
from vllm import LLM, EngineArgs
from typing import List, Any
from pydantic import BaseModel
from fastapi import FastAPI, Response
from transformers import AutoTokenizer

model_name = "jinaai/jina-embeddings-v3"

app = FastAPI()

tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)

args = EngineArgs(
    model="jinaai/jina-embeddings-v3",
    task="embedding",
    dtype="bfloat16",
    trust_remote_code=True,
)

# instantiate the LLM (and engine) in one go
model = LLM(**vars(args))


class EmbeddingRequest(BaseModel):
    data: List[str]


class EmbeddingORJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_SERIALIZE_NUMPY)


def truncate_text(text, max_tokens=8192):
    encoded = tokenizer(
        text, truncation=True, max_length=max_tokens, return_tensors=None
    )
    ids = encoded["input_ids"]
    return tokenizer.decode(ids, skip_special_tokens=True)


def truncate_req(req, max_tokens=8192):
    new_req = []
    for text in req:
        new_req.append(truncate_text(text, max_tokens))
    return new_req


@app.post("/v1/embeddings", response_class=EmbeddingORJSONResponse)
async def generate_embedding(req: EmbeddingRequest):
    outputs = model.embed(truncate_req(req.data))
    embeddings = [output.outputs.embedding for output in outputs]

    return EmbeddingORJSONResponse({"embeddings": embeddings})


@app.get("/health")
def health():
    return {"status": "ok"}
