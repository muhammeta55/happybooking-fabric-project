FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stream_producer.py .

# Default rate; override with -e EVENTS_PER_SECOND=10 at runtime
ENV EVENTS_PER_SECOND=5

CMD ["python", "stream_producer.py"]