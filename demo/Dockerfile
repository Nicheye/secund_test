FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN pip install "fastapi>=0.115,<1.0" "uvicorn>=0.34,<1.0" "httpx>=0.28,<1.0"

COPY demo ./demo

RUN useradd --system --no-create-home app
USER app

EXPOSE 9100
CMD ["uvicorn", "demo.server:app", "--host", "0.0.0.0", "--port", "9100"]
