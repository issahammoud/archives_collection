FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    git \
    libmagickwand-dev && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app

COPY . /app/

RUN pip install --no-cache-dir -r install_tools/requirements.txt

RUN pre-commit install

EXPOSE 8050

# CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8050", "--reload", "src.main.app:app"]
CMD ["python3", "src/main/app.py"]
