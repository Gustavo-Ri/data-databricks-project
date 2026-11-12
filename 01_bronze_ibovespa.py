# Databricks notebook source
# MAGIC %pip install yfinance
# MAGIC
# MAGIC

# COMMAND ----------

#importar bibliotecas
import yfinance as yf
import pandas as pd

from pyspark.sql import functions as F

# COMMAND ----------

#ativos indice ibovespa

tickers = [
    "ABEV3.SA",
    "ALOS3.SA",
    "ASAI3.SA",
    "B3SA3.SA",
    "BBAS3.SA",
    "BBDC3.SA",
    "BBDC4.SA",
    "BBSE3.SA",
    "BPAC11.SA",
    "ITSA4.SA",
    "ITUB4.SA",
    "PETR3.SA",
    "PETR4.SA",
    "PRIO3.SA",
    "VALE3.SA"
]

# COMMAND ----------

# DBTITLE 1,Cell 4
# parametros temporais - carga incremental
# data_inicio = maior data ja carregada na bronze + 1 dia (primeira execucao usa a data padrao)

from datetime import date, timedelta

from pyspark.sql import functions as F

data_inicio_padrao = "2026-01-01"
tabela_bronze = "mercado_financeiro.bronze.ibvespa_acoes"

try:
    ultima_data = (
        spark.table(tabela_bronze)
        .agg(F.max("Date").alias("ultima_data"))
        .first()["ultima_data"]
    )
    data_inicio = (ultima_data + timedelta(days=1)).strftime("%Y-%m-%d")
except Exception:
    data_inicio = data_inicio_padrao

data_fim = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")

# COMMAND ----------

#validar

print(data_inicio)
print(data_fim)

# COMMAND ----------

# DBTITLE 1,Cell 6
#extraindo apenas os dados novos

df_pandas = yf.download(tickers = tickers,
                        start = data_inicio,
                        end = data_fim,
                        group_by = "ticker",
                        auto_adjust= False)

#se nao ha dados novos (ex: job rodado 2x no mesmo dia), encerra sem escrever nada
if df_pandas.empty:
    dbutils.notebook.exit("Sem dados novos - nada a carregar")


# COMMAND ----------

#visualizando a extração

df_pandas.tail()

# COMMAND ----------

#visualizando a estrutura 

df_pandas.columns


# COMMAND ----------

#transformando o multindex em tabela normal 

df_long =(
    df_pandas
    .stack(level=0, future_stack = True)
    .rename_axis(index = ['Date', 'Ticker'])
    .reset_index()
)
df_long.head()

# COMMAND ----------

#criando o dataframe spark

df_ibov = spark.createDataFrame(df_long)
df_ibov.show(5)

# COMMAND ----------

# DBTITLE 1,metadados

#metadados  - saber da onde o dado veio

df_bronze = (
    df_ibov
    .withColumn(
        "data_ingestão",
        F.current_timestamp()
    )
    .withColumn(
       "fonte",
        F.lit("Yahoo Finance") 
    )
    .withColumn(
        "indice",
        F.lit("IBOV")
    )
)
df_bronze.show(5)

# COMMAND ----------

# DBTITLE 1,Cell 11
#salvar na camada bronze

(
    df_bronze
    .withColumnRenamed("Adj Close", "Adj_Close")
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "mercado_financeiro.bronze.ibvespa_acoes"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE mercado_financeiro.bronze.ibvespa_acoes

# COMMAND ----------

# DBTITLE 1,select bbas3
# MAGIC %sql
# MAGIC
# MAGIC SELECT DATE,
# MAGIC        Adj_Close,
# MAGIC        Ticker
# MAGIC FROM mercado_financeiro.bronze.ibvespa_acoes
# MAGIC WHERE Ticker = 'BBAS3.SA'

# COMMAND ----------

# MAGIC %sql
# MAGIC --- conhecendo a tabela
# MAGIC
# MAGIC SELECT Date,
# MAGIC        Adj_Close,
# MAGIC        Ticker,
# MAGIC     REPLACE(Ticker, '.SA', '')
# MAGIC FROM mercado_financeiro.bronze.ibvespa_acoes
# MAGIC ORDER BY Date DESC;
# MAGIC