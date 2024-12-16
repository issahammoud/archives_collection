FROM python:3.10-slim

WORKDIR /webapp

RUN apt-get update && \
    apt-get install -y \
    git \
    libmagickwand-dev && \
    rm -rf /var/lib/apt/lists/*


COPY requirements.txt /webapp/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . /webapp/

ENV PYTHONPATH=/webapp

EXPOSE 8050

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8050", "src.main.app:server"]
# CMD ["python3", "src/main/app.py"]
