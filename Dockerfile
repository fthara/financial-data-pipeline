FROM python:3.11-slim

ARG UID=1000
ARG GID=1000

RUN apt-get update && apt-get install -y \
    openjdk-21-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g ${GID} app && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash app

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

# 👇 força o venv no lugar certo e usa o Python do sistema
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV UV_PYTHON=/usr/local/bin/python

RUN uv sync --frozen

COPY . .

ENV AIRFLOW_HOME=/app/airflow
ENV PATH="/app/.venv/bin:$PATH"

# Remover .venv e resincronizar para corrigir os shebangs para o container
RUN rm -rf .venv && uv sync --frozen

RUN mkdir -p $AIRFLOW_HOME/logs && chown -R app:app /app

USER app

EXPOSE 8080

CMD ["airflow", "api-server"]