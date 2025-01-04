import torch
import orjson
import numpy as np
from typing import List, Any
from fastapi import FastAPI, Response
from pydantic import BaseModel
from transformers import AutoTokenizer, AutoModel


app = FastAPI()


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
tokenizer = AutoTokenizer.from_pretrained("h4c5/sts-camembert-base")
model = AutoModel.from_pretrained("h4c5/sts-camembert-base").to(device)

print(f"The API is using device: {device}")

model.eval()


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )


class EmbeddingRequest(BaseModel):
    text: List[str]


class EmbeddingORJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_SERIALIZE_NUMPY)


@app.post("/embed", response_class=EmbeddingORJSONResponse)
async def generate_embedding(req: EmbeddingRequest):
    encoded_input = tokenizer(
        req.text, padding=True, truncation=True, return_tensors="pt"
    ).to(device)
    with torch.no_grad():
        model_output = model(**encoded_input)
        embeddings = mean_pooling(model_output, encoded_input["attention_mask"])

    return EmbeddingORJSONResponse(
        {"embeddings": embeddings.detach().cpu().numpy().astype(np.float32)}
    )
