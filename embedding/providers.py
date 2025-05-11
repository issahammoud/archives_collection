import os
from typing import List


class EmbeddingProvider:
    async def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError


class NoneProvider(EmbeddingProvider):
    async def embed(self, texts: List[str]):
        return None


class GPUProvider(EmbeddingProvider):
    def __init__(self, model_name="jinaai/jina-embeddings-v3"):
        try:
            from transformers import AutoTokenizer
            from vllm import LLM, EngineArgs
        except ImportError as e:
            raise RuntimeError("GPU mode requires vllm and transformers") from e

        self.tokenizer = AutoTokenizer.from_pretrained(
            model_name, trust_remote_code=True
        )
        args = EngineArgs(
            model=model_name,
            task="embedding",
            dtype="bfloat16",
            trust_remote_code=True,
        )
        self.model = LLM(**vars(args))

    def truncate(self, texts: List[str], max_tokens: int = 8192) -> List[str]:
        out = []
        for t in texts:
            enc = self.tokenizer(t, truncation=True, max_length=max_tokens, return_tensors=None)
            out.append(self.tokenizer.decode(enc["input_ids"], skip_special_tokens=True))
        return out

    async def embed(self, texts: List[str]):
        truncated = self.truncate(texts)
        outputs = self.model.embed(truncated)
        return [o.outputs.embedding for o in outputs]


def get_provider() -> EmbeddingProvider:
    mode = os.getenv("EMBEDDING_MODE", "NONE").lower()
    if mode == "gpu":
        return GPUProvider()
    return NoneProvider()