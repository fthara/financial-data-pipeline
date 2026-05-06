# Financial Data Pipeline

Pipeline de dados financeiros com Apache Airflow + PySpark, usando dados do Yahoo Finance (yfinance).

## Visao geral

Este projeto executa um fluxo ETL em DAG:

1. Extracao de precos, dividendos e splits
2. Transformacao e consolidacao em camada silver
3. Geracao de metricas em camada gold

A DAG principal esta em [dags/dag.py](dags/dag.py) e roda diariamente as 12:00 (fuso America/Sao_Paulo).

## Arquitetura

- Orquestracao: Apache Airflow
- Processamento: PySpark
- Ingestao de mercado: yfinance
- Armazenamento: Parquet particionado por data
- Ambiente: Docker Compose

Servicos no compose:

- airflow-init
- airflow-webserver
- airflow-scheduler
- airflow-dag-processor

Arquivos principais:

- [dags/dag.py](dags/dag.py): definicao da DAG e dependencias
- [dags/extract.py](dags/extract.py): ingestao de dados do yfinance
- [dags/transform.py](dags/transform.py): camada silver
- [dags/load.py](dags/load.py): camada gold (performance, indicators, ranking)
- [docker-compose.yml](docker-compose.yml): servicos, volumes e env vars do Airflow
- [Dockerfile](Dockerfile): imagem base com Python, Java e dependencias

## Estrutura de dados

No container, os dados do pipeline sao escritos em:

- /app/data/financial_data_pipeline/bronze
- /app/data/financial_data_pipeline/silver
- /app/data/financial_data_pipeline/gold

A lista de tickers usada na extracao vem de:

- /app/data_fixed/tickers_ibovespa.parquet

No projeto local, esse arquivo existe em [data_fixed/tickers_ibovespa.parquet](data_fixed/tickers_ibovespa.parquet).

## Pre-requisitos

- Docker e Docker Compose plugin
- Acesso a internet para baixar dados do Yahoo Finance
- Volume de dados host disponivel no caminho configurado em [docker-compose.yml](docker-compose.yml)

Observacao importante:

Atualmente o compose monta o host path absoluto abaixo em /app/data:

- /home/fernando/fernando/projects/data:/app/data

Se voce estiver em outra maquina, ajuste esse caminho no compose.

## Como executar

1. Subir os servicos:

```bash
docker compose up --build -d
```

2. Abrir Airflow UI:

- http://localhost:8080

3. Habilitar a DAG financial-pipeline (se necessario) e aguardar o agendamento diario.

4. Para executar manualmente:

```bash
docker compose exec -T airflow-webserver airflow dags trigger financial-pipeline
```

5. Ver logs do scheduler:

```bash
docker compose logs -f airflow-scheduler
```

## Agendamento

A DAG esta configurada com:

- schedule: 0 12 * * *
- timezone: America/Sao_Paulo
- catchup: false

Isso significa 1 execucao por dia, as 12:00.

## Dependencias Python

Definidas em [pyproject.toml](pyproject.toml):

- apache-airflow
- pyspark
- pandas
- requests
- yfinance
- pyarrow

## Troubleshooting rapido

### 1) Task falha com mensagem de log sem traceback util

Verifique logs do scheduler e da task:

```bash
docker compose logs --tail=300 airflow-scheduler
docker compose logs --tail=300 airflow-webserver
```

### 2) Connection refused entre scheduler e API

Confirme no compose:

- AIRFLOW__CORE__EXECUTOR=LocalExecutor
- AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/

Depois recrie os containers:

```bash
docker compose up -d --force-recreate
```

### 3) Arquivo de tickers nao encontrado

Confirme existencia de [data_fixed/tickers_ibovespa.parquet](data_fixed/tickers_ibovespa.parquet) e se o path interno /app/data_fixed esta disponivel no container.

### 4) Falha por path de dados /app/data

Confirme se o volume host configurado em [docker-compose.yml](docker-compose.yml) existe na sua maquina.
