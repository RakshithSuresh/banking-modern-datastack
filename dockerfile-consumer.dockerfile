FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/ ./consumer/

CMD ["python", "consumer/kafka_to_s3.py"]