from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, regexp_replace
from datetime import datetime
import os

# spark
spark = SparkSession.builder.appName("VendasDiarias").getOrCreate()

# data nome arquivo
data_execucao = datetime.now().strftime("%Y%m%d")
output_dir = f"../output/vendas_diarias_{data_execucao}"

# carrega arquivos csv
telefonia = spark.read.csv("../files/base_telefonia.csv", header=True, inferSchema=True)
pessoas = spark.read.csv("../files/base_pessoas.csv", header=True, inferSchema=True)

# join vendas-pessoas
vendas = telefonia.join(pessoas, on="Username", how="left")

# coluna sem espaco 
vendas = vendas.withColumn("Lider_da_Equipe", regexp_replace(col("Líder da Equipe"), " ", "_"))

# group by data e lideranca
vendas_diarias = vendas.groupBy("inicio_ligacao", "Líder da Equipe") \
    .agg(sum(col("Valor venda")).alias("total_vendas"))

# gera parquet
vendas_diarias.coalesce(1).write \
    .partitionBy("Lider_da_Equipe") \
    .mode("overwrite") \
    .parquet(output_dir)

print(f"Tabela de vendas diárias gerada com sucesso.")

spark.stop()