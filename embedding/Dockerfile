FROM vllm/vllm-openai:latest

WORKDIR /embedding


COPY requirements.txt /embedding/requirements.txt

RUN pip3 install --no-cache-dir -r /embedding/requirements.txt

COPY . /embedding

EXPOSE 8000

ENTRYPOINT ["uvicorn"]
CMD ["main:app", "--host", "0.0.0.0", "--port", "8000"]

