import orjson
from typing import List, Any
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Response

from providers import get_provider


app = FastAPI()
provider = get_provider()


class EmbedRequest(BaseModel):
    data: List[str]


class ORJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, option=orjson.OPT_SERIALIZE_NUMPY)


@app.post("/v1/embeddings", response_class=ORJSONResponse)
async def embed(req: EmbedRequest):
    try:
        embs = await provider.embed(req.data)
        return {"embeddings": embs}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/health")
def health():
    return {"status": "ok"}
