FROM python:3.11-slim

ARG UID=1000
ARG GID=1000

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Criar usuário não-root com UID/GID configuráveis (Airflow exige usuário real)
RUN groupadd -g ${GID} app && \
    useradd -u ${UID} -g ${GID} -m -s /bin/bash app

# Spark precisa de Java
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Instalar uv
RUN pip install uv

WORKDIR /app

# Copiar arquivos de dependência
COPY pyproject.toml uv.lock ./

# Instalar dependências com uv
RUN uv sync

# Copiar projeto
COPY . .

# Airflow config
ENV AIRFLOW_HOME=/app/airflow
ENV PATH="/app/.venv/bin:$PATH"

RUN mkdir -p $AIRFLOW_HOME/logs && chown -R app:app /app

USER app

EXPOSE 8080

CMD ["airflow", "api-server"]