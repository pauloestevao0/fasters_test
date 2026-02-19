from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, unix_timestamp
from datetime import datetime
import os

# spark
spark = SparkSession.builder.appName("AnaliseExploratoria").getOrCreate()

# data nome arquivo
data_execucao = datetime.now().strftime("%Y%m%d")
output_file = f"../output/analise_exploratoria_{data_execucao}.txt"

# carrega arquivos csv
telefonia = spark.read.csv("../files/base_telefonia.csv", header=True, inferSchema=True)
pessoas = spark.read.csv("../files/base_pessoas.csv", header=True, inferSchema=True)
avaliacoes = spark.read.csv("../files/base_avaliacoes.csv", header=True, inferSchema=True)

# join nome-pessoa
vendas = telefonia.join(pessoas, on="Username", how="left")

# duracao da ligacao
vendas = vendas.withColumn(
    "DuracaoLigacao",
    unix_timestamp("fim_ligação") - unix_timestamp("inicio_ligacao")
)

with open(output_file, "w", encoding="utf-8") as f:
    # top 5 vendedores
    top_vendedores = vendas.groupBy("Nome") \
        .agg(sum(col("Valor venda")).alias("total_vendas")) \
        .orderBy(col("total_vendas").desc()) \
        .limit(5)
    print("Top 5 vendedores por valor total de vendas:")
    top_vendedores.show()
    f.write("Top 5 vendedores por valor total de vendas:\n")
    for row in top_vendedores.collect():
        f.write(f"{row['Nome']}: {row['total_vendas']}\n")

    # ticket medio
    ticket_medio = vendas.groupBy("Nome") \
        .agg(avg(col("Valor venda")).alias("ticket_medio"))
    print("\nTicket médio por vendedor:")
    ticket_medio.show()
    f.write("\nTicket médio por vendedor:\n")
    for row in ticket_medio.collect():
        f.write(f"{row['Nome']}: {row['ticket_medio']:.2f}\n")

    # tempo medio ligacoes
    tempo_medio = vendas.agg(avg("DuracaoLigacao").alias("tempo_medio"))
    print("\nTempo médio das ligações:")
    tempo_medio.show()
    f.write(f"\nTempo médio das ligações: {tempo_medio.collect()[0]['tempo_medio']:.2f}\n")

    # nota media
    nota_media = avaliacoes.agg(avg("Nota").alias("nota_media"))
    print("\nNota média geral:")
    nota_media.show()
    f.write(f"\nNota média geral: {nota_media.collect()[0]['nota_media']:.2f}\n")

    # pior
    avaliacoes_com_pessoas = avaliacoes.join(pessoas, on="Username", how="left")
    pior_vendedor = avaliacoes_com_pessoas.groupBy("Nome") \
        .agg(avg("Nota").alias("media_nota")) \
        .orderBy(col("media_nota").asc()) \
        .limit(1)
    print("\nVendedor com pior média de avaliação:")
    pior_vendedor.show()
    f.write("\nVendedor com pior média de avaliação:\n")
    for row in pior_vendedor.collect():
        f.write(f"{row['Nome']}: {row['media_nota']:.2f}\n")

spark.stop()