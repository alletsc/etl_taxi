# ETL TAXIORIO

Este repostitório tem como objetivo realizar a extração de dados de um banco local (MongoDB), carga dos dados no bucket da GCP, transformação e carga no BigQuery dos dados de táxis do Rio de Janeiro.

Para isso seguiremos os seguintes passos:

1. Extração dos dados do banco de dados de origem (MongoDB)
2. Carga dos dados no bucket da GCP
3. Criação e carga dos dados brutos no BigQuery
4. Leitura dos dados brutos do BigQuery e transformação dos dados
5. Criação de novas tabelas no BigQuery com os dados transformados

Após a execução dos passos acima, teremos os dados de táxi do Rio de Janeiro disponíveis no BigQuery para análises e futuros dashboards.

## Para utilizar esse repositorio siga os passos abaixo:

1. Clone o repositório

```bash
git clone https://github.com/alletsc/etl_taxi.git
```

2. Crie um ambiente virtual e instale as dependências

```bash
cd etl_taxi
python3 -m venv .venv
source .venv/bin/activate # Linux
.venv\Scripts\activate # Windows
pip install -r requirements-gcp.txt
```
