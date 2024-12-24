FROM python:3.10-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY . /workspace

RUN pip install pre-commit black flake8

RUN if [ -d ".git" ]; then git config --global --add safe.directory /workspace; fi

RUN pre-commit install

CMD ["pre-commit", "run", "--all-files"]
