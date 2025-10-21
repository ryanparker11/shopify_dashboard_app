# syntax=docker/dockerfile:1
FROM python:3.11-slim
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY commerce_app ./commerce_app
ENV PORT=8080
EXPOSE 8080
ARG BUILD_ID
ENV BUILD_ID=$BUILD_ID
#test

CMD ["python","-m","uvicorn","commerce_app.app:app","--host","0.0.0.0","--port","8080", "--proxy-headers", "--log-level", "info"]
