FROM python:3.13-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE ./
COPY ducklauncher ./ducklauncher

RUN pip install --no-cache-dir .

EXPOSE 8000 8001

ENTRYPOINT ["ducklauncher"]
