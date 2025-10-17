# syntax=docker/dockerfile:1
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/ ./api/
ENV PYTHONPATH=/app
EXPOSE 8080
CMD ["python","-m","uvicorn","shopify_dashboard_app.api.rest.app:app","--host","0.0.0.0","--port","8080"]
