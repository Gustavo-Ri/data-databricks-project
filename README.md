# Pipeline de Dados — Cotações do Ibovespa (Databricks)

Projeto de portfólio de engenharia de dados: pipeline completo de ingestão, tratamento e modelagem de cotações diárias de 15 ações do Ibovespa, com arquitetura de medalhas (bronze → silver → gold) no Databricks, governança via Unity Catalog, carga incremental e orquestração automática diária com Lakeflow Jobs.

## Arquitetura

```text
yfinance (Yahoo Finance)
        |  extração incremental (somente datas ainda não carregadas)
        v
   BRONZE  (append)   -> mercado_financeiro.bronze.ibvespa_acoes
        |  tratamento e padronização
        v
   SILVER  (overwrite) -> mercado_financeiro.silver.ibvespa_acoes
        |  modelagem dimensional
        v
   GOLD    (overwrite) -> dim_ticker, dim_calendario, fato de cotações
                        -> enriquecimento com indicadores técnicos
```

## Notebooks

Os notebooks estão no formato *source* do Databricks (`.py`) e podem ser importados em qualquer workspace via **Workspace > Import**.

| Notebook | Camada | Descrição |
| --- | --- | --- |
| `01_bronze_ibovespa` | Bronze | Extração das cotações no Yahoo Finance (`yfinance`) e gravação incremental (append) na bronze. O `data_inicio` é calculado a partir do `MAX(Date)` já salvo, então cada execução baixa apenas os dias faltantes; se não há pregão novo, o notebook encerra sem escrever nada (evita duplicidade). |
| `02_silver_ibovespa` | Silver | Leitura da bronze, limpeza do sufixo `.SA` do ticker, renomeação das colunas para o padrão do negócio (`Data`, `Codigo`, `Abertura`, `Maximo`, `Minimo`, `Fechamento`, `Fechamento_Ajustado`) e gravação na silver. |
| `03_gold_ibovespa` | Gold | Modelagem dimensional: `dim_ticker` (tickers únicos), `dim_calendario` (ano, trimestre, mês, dia, dia da semana e nome do mês em pt-BR) e tabela de cotações consolidada com data tipada. |
| `04_gold_fato_cotacoes_indicadores` | Gold | Enriquecimento da fato com indicadores técnicos usando window functions: `Fechamento_Anterior` (LAG), `Retorno_Diario`, `Média_Movel_20` e `Média_Movel_50`. |

## Modelo de dados (Unity Catalog)

Catálogo `mercado_financeiro` com schemas por camada:

* `bronze.ibvespa_acoes` — dados brutos conforme a fonte: `Date`, `Ticker`, `Open`, `High`, `Low`, `Close`, `Adj_Close`, `Volume`, `data_ingestao`, `fonte` (Yahoo Finance), `indice` (IBOV)
* `silver.ibvespa_acoes` — dados tratados: `Data`, `Codigo`, `Abertura`, `Maximo`, `Minimo`, `Fechamento`, `Fechamento_Ajustado`, `Volume`, metadados de ingestão
* `gold.dim_ticker` — dimensão de ações do índice
* `gold.dim_calendario` — dimensão temporal com atributos em português
* `gold.ibvespa_acoes` — cotações consolidadas para consumo
* `gold.fato_cotacoes` — fato enriquecida com retorno diário e médias móveis

## Orquestração

O job **ibovespa** (Lakeflow Jobs) roda **todos os dias às 19:00** (America/Sao_Paulo), duas horas após o fechamento da B3 (17h), na sequência:

1. `01_bronze_ibovespa` — carga incremental do dia
2. `02_silver_ibovespa` — depende do bronze
3. `03_gold_ibovespa` — depende do silver

### Carga incremental (bronze)

```python
# data_inicio = maior data já carregada na bronze + 1 dia
ultima_data = (
    spark.table("mercado_financeiro.bronze.ibvespa_acoes")
    .agg(F.max("Date").alias("ultima_data"))
    .first()["ultima_data"]
)
data_inicio = (ultima_data + timedelta(days=1)).strftime("%Y-%m-%d")
```

Somado a isso, um guarda impede reprocessamento no mesmo dia:

```python
# se não há dados novos, encerra sem escrever nada
if df_pandas.empty:
    dbutils.notebook.exit("Sem dados novos - nada a carregar")
```

## Tecnologias

* **Databricks** — notebooks, Lakeflow Jobs e compute serverless
* **PySpark** — processamento distribuído e window functions
* **Delta Lake** — tabelas nas três camadas
* **Unity Catalog** — governança e linhagem de dados
* **yfinance** — fonte das cotações
* **Git/GitHub** — versionamento do código

## Como reproduzir

1. Criar o catálogo `mercado_financeiro` e os schemas `bronze`, `silver` e `gold` no Unity Catalog.
2. Importar os notebooks deste repositório (formato source) para um folder do workspace.
3. Executar na ordem `01` → `02` → `03` (a primeira execução da bronze carrega o histórico desde `2026-01-01`).
4. Executar `04_gold_fato_cotacoes_indicadores` para gerar a fato com indicadores.
5. Criar um job com as três primeiras tarefas encadeadas e agendar conforme a necessidade (ex.: diário às 19h).

## Autor

Gustavo Ribeiro — projeto de portfólio em engenharia de dados.