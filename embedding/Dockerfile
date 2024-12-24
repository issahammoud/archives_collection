FROM nvidia/cuda:12.4.0-runtime-ubuntu22.04

WORKDIR /embedding

RUN apt-get update && apt-get install -y \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y \
    python3.10 \
    python3.10-dev \
    python3.10-distutils \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /embedding/requirements.txt

RUN pip3 install torch torchvision --index-url https://download.pytorch.org/whl/cu124
RUN pip3 install --no-cache-dir -r /embedding/requirements.txt

COPY . /embedding

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
