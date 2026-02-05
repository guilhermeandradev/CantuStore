# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Análise de Carrinhos Abandonados
# MAGIC ## Parte 2 - Prova de Análise de Dados com PySpark
# MAGIC ---
# MAGIC **Objetivo:** Analisar dados de carrinhos abandonados para entender padrões e gerar insights.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Carregamento dos Dados

# COMMAND ----------

# Imports necessários
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, 
    year, month, dayofmonth, date_format,
    row_number, dense_rank, lag,
    when, coalesce, concat, lit, concat_ws,
    collect_list, explode, array, struct,
    first, last, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os

# SparkSession (já existe no Databricks, mas útil para testes locais)
# spark = SparkSession.builder.appName("CarrinhosAbandonados").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Carregar os dados
# MAGIC **IMPORTANTE:** No Databricks, faça upload dos arquivos para o DBFS primeiro.
# MAGIC 
# MAGIC Opções de upload:
# MAGIC 1. Menu lateral > Data > Create Table > Upload File
# MAGIC 2. Ou use: `dbutils.fs.cp("file:/path/local", "dbfs:/FileStore/cantustore/")`

# COMMAND ----------

# Definir caminho base dos dados
# ALTERE ESTE CAMINHO conforme onde você fez upload no Databricks
BASE_PATH = "/FileStore/cantustore/"

# Se estiver rodando localmente (para teste), use:
# BASE_PATH = "./Parte2_AnaliseDados/"

# COMMAND ----------

# Carregar tabelas Parquet
df_carts = spark.read.parquet(f"{BASE_PATH}tb_carts")
df_cartentries = spark.read.parquet(f"{BASE_PATH}tb_cartentries")
df_addresses = spark.read.parquet(f"{BASE_PATH}tb_addresses")
df_paymentinfos = spark.read.parquet(f"{BASE_PATH}tb_paymentinfos")

# Carregar tabelas CSV
df_users = spark.read.csv(f"{BASE_PATH}tb_users.csv", header=True, inferSchema=True)
df_regions = spark.read.csv(f"{BASE_PATH}tb_regions.csv", header=True, inferSchema=True)
df_paymentmodes = spark.read.csv(f"{BASE_PATH}tb_paymentmodes.csv", header=True, inferSchema=True)
df_cmssitelp = spark.read.csv(f"{BASE_PATH}tb_cmssitelp.csv", header=True, inferSchema=True)

print("Dados carregados com sucesso!")

# COMMAND ----------

# Verificar schemas
print("="*60)
print("SCHEMA DAS TABELAS")
print("="*60)

print("\n--- tb_carts ---")
df_carts.printSchema()

print("\n--- tb_cartentries ---")
df_cartentries.printSchema()

# COMMAND ----------

# Estatísticas básicas
print("="*60)
print("CONTAGEM DE REGISTROS")
print("="*60)
print(f"tb_carts: {df_carts.count():,} registros")
print(f"tb_cartentries: {df_cartentries.count():,} registros")
print(f"tb_addresses: {df_addresses.count():,} registros")
print(f"tb_users: {df_users.count():,} registros")
print(f"tb_regions: {df_regions.count():,} registros")
print(f"tb_paymentmodes: {df_paymentmodes.count():,} registros")
print(f"tb_paymentinfos: {df_paymentinfos.count():,} registros")
print(f"tb_cmssitelp: {df_cmssitelp.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Preparar dados - JOIN principal
# MAGIC Criar DataFrame unificado com carrinhos e seus itens

# COMMAND ----------

# JOIN carrinhos com itens
df_carts_items = df_carts.alias("c").join(
    df_cartentries.alias("ce"),
    col("c.PK") == col("ce.p_order"),
    "inner"
).select(
    col("c.PK").alias("cart_pk"),
    col("c.createdTS").alias("cart_created"),
    col("c.p_totalprice").alias("cart_totalprice"),
    col("c.p_user").alias("cart_user"),
    col("c.p_paymentaddress").alias("cart_paymentaddress"),
    col("c.p_paymentinfo").alias("cart_paymentinfo"),
    col("c.p_paymentmode").alias("cart_paymentmode"),
    col("c.p_site").alias("cart_site"),
    col("ce.PK").alias("entry_pk"),
    col("ce.p_product").alias("product"),
    col("ce.p_quantity").alias("quantity"),
    col("ce.p_totalprice").alias("entry_totalprice")
)

# Adicionar colunas de data
df_carts_items = df_carts_items.withColumn(
    "ano", year(col("cart_created"))
).withColumn(
    "mes", month(col("cart_created"))
).withColumn(
    "ano_mes", date_format(col("cart_created"), "yyyy-MM")
).withColumn(
    "data", date_format(col("cart_created"), "yyyy-MM-dd")
)

# Cache para performance
df_carts_items.cache()

print(f"Total de registros (carts x entries): {df_carts_items.count():,}")
df_carts_items.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Análises Solicitadas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Quais os produtos que mais tiveram carrinhos abandonados?

# COMMAND ----------

# Produtos com mais carrinhos abandonados
df_produtos_abandonados = df_carts_items.groupBy("product").agg(
    count("cart_pk").alias("qtd_carrinhos"),
    sum("quantity").alias("qtd_itens"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_total")
).orderBy(col("qtd_carrinhos").desc())

print("="*60)
print("TOP 20 PRODUTOS COM MAIS CARRINHOS ABANDONADOS")
print("="*60)
df_produtos_abandonados.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Quais as duplas de produtos em conjunto que mais tiveram carrinhos abandonados?

# COMMAND ----------

# Criar pares de produtos no mesmo carrinho
# Self-join para encontrar pares

df_pares = df_carts_items.alias("a").join(
    df_carts_items.alias("b"),
    (col("a.cart_pk") == col("b.cart_pk")) & (col("a.product") < col("b.product")),
    "inner"
).select(
    col("a.cart_pk"),
    col("a.product").alias("produto_1"),
    col("b.product").alias("produto_2")
).distinct()

# Contar pares
df_duplas_abandonadas = df_pares.groupBy("produto_1", "produto_2").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy(col("qtd_carrinhos").desc())

print("="*60)
print("TOP 20 DUPLAS DE PRODUTOS MAIS ABANDONADAS JUNTAS")
print("="*60)
df_duplas_abandonadas.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Quais produtos tiveram um aumento de abandono?

# COMMAND ----------

# Análise de tendência de abandono por produto (mês a mês)
df_tendencia = df_carts_items.groupBy("product", "ano_mes").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy("product", "ano_mes")

# Calcular variação mês a mês usando Window
window_spec = Window.partitionBy("product").orderBy("ano_mes")

df_tendencia_var = df_tendencia.withColumn(
    "qtd_mes_anterior", lag("qtd_carrinhos", 1).over(window_spec)
).withColumn(
    "variacao", col("qtd_carrinhos") - coalesce(col("qtd_mes_anterior"), lit(0))
).withColumn(
    "variacao_pct", 
    when(col("qtd_mes_anterior").isNotNull() & (col("qtd_mes_anterior") > 0),
         spark_round((col("variacao") / col("qtd_mes_anterior")) * 100, 2)
    ).otherwise(None)
)

# Produtos com aumento consistente (últimos meses com variação positiva)
# Identificar produtos onde a última variação foi positiva
window_last = Window.partitionBy("product").orderBy(col("ano_mes").desc())

df_ultimo_mes = df_tendencia_var.withColumn(
    "rn", row_number().over(window_last)
).filter(col("rn") == 1).filter(col("variacao") > 0)

print("="*60)
print("PRODUTOS COM AUMENTO DE ABANDONO (último período)")
print("="*60)
df_ultimo_mes.select(
    "product", "ano_mes", "qtd_carrinhos", "qtd_mes_anterior", "variacao", "variacao_pct"
).orderBy(col("variacao").desc()).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Quais os produtos novos e a quantidade de carrinhos no seu primeiro mês de lançamento?

# COMMAND ----------

# Identificar primeiro mês de cada produto
window_primeiro = Window.partitionBy("product").orderBy("ano_mes")

df_primeiro_mes = df_carts_items.groupBy("product", "ano_mes").agg(
    count("cart_pk").alias("qtd_carrinhos"),
    sum("quantity").alias("qtd_itens")
).withColumn(
    "rn", row_number().over(window_primeiro)
).filter(col("rn") == 1)

print("="*60)
print("PRODUTOS NOVOS - PRIMEIRO MÊS DE LANÇAMENTO")
print("="*60)
df_primeiro_mes.select(
    "product", 
    col("ano_mes").alias("primeiro_mes"),
    "qtd_carrinhos",
    "qtd_itens"
).orderBy(col("qtd_carrinhos").desc()).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Quais estados tiveram mais abandonos?

# COMMAND ----------

# JOIN com addresses e regions para obter estados
df_carts_estados = df_carts.alias("c").join(
    df_addresses.alias("a"),
    col("c.p_paymentaddress") == col("a.PK"),
    "left"
).join(
    df_regions.alias("r"),
    col("a.p_region") == col("r.PK"),
    "left"
).select(
    col("c.PK").alias("cart_pk"),
    col("r.p_isocodeshort").alias("estado")
).filter(col("estado").isNotNull())

# Contar abandonos por estado
df_abandonos_estado = df_carts_estados.groupBy("estado").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy(col("qtd_carrinhos").desc())

print("="*60)
print("ABANDONOS POR ESTADO")
print("="*60)
df_abandonos_estado.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Relatórios

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Relatório por Produto - Mês a Mês

# COMMAND ----------

# Relatório mensal por produto
df_relatorio_produto_mes = df_carts_items.groupBy("product", "ano_mes").agg(
    count("cart_pk").alias("qtd_carrinhos_abandonados"),
    sum("quantity").alias("qtd_itens_abandonados"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_nao_faturado")
).orderBy("product", "ano_mes")

print("="*60)
print("RELATÓRIO POR PRODUTO - MÊS A MÊS")
print("="*60)
df_relatorio_produto_mes.show(50, truncate=False)

# Salvar como tabela/arquivo
# df_relatorio_produto_mes.write.mode("overwrite").csv(f"{BASE_PATH}relatorio_produto_mes", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Relatório por Data

# COMMAND ----------

# Relatório diário
df_relatorio_data = df_carts_items.groupBy("data").agg(
    count("cart_pk").alias("qtd_carrinhos_abandonados"),
    sum("quantity").alias("qtd_itens_abandonados"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_nao_faturado")
).orderBy("data")

print("="*60)
print("RELATÓRIO POR DATA")
print("="*60)
df_relatorio_data.show(50, truncate=False)

# Salvar como tabela/arquivo
# df_relatorio_data.write.mode("overwrite").csv(f"{BASE_PATH}relatorio_data", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exportação TXT - Top 50 Maiores Carrinhos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Preparar dados para exportação

# COMMAND ----------

# Top 50 carrinhos por p_totalprice
df_top50_carts = df_carts.orderBy(col("p_totalprice").desc()).limit(50)

# JOIN completo com todas as informações necessárias
df_export = df_top50_carts.alias("c").join(
    df_users.alias("u"),
    col("c.p_user") == col("u.PK"),
    "left"
).join(
    df_paymentmodes.alias("pm"),
    col("c.p_paymentmode") == col("pm.PK"),
    "left"
).join(
    df_paymentinfos.alias("pi"),
    col("c.p_paymentinfo") == col("pi.PK"),
    "left"
).join(
    df_cmssitelp.alias("cs"),
    col("c.p_site") == col("cs.ITEMPK"),
    "left"
).join(
    df_addresses.alias("a"),
    col("c.p_paymentaddress") == col("a.PK"),
    "left"
).select(
    col("c.PK").alias("cart_pk"),
    col("c.createdTS").alias("cart_createdTS"),
    col("c.p_totalprice").alias("cart_totalprice"),
    col("u.p_uid").alias("user_uid"),
    col("pm.p_code").alias("paymentmode_code"),
    col("pi.p_installments").alias("paymentinfo_installments"),
    col("cs.p_name").alias("site_name"),
    col("a.p_postalcode").alias("address_postalcode")
)

# Agregar entries por carrinho
df_entries_agg = df_cartentries.groupBy("p_order").agg(
    sum("p_quantity").alias("sum_quantity"),
    count("PK").alias("count_entries"),
    collect_list(
        struct(
            col("p_product"),
            col("p_quantity"),
            col("p_totalprice")
        )
    ).alias("entries")
)

# JOIN final
df_final_export = df_export.join(
    df_entries_agg,
    df_export.cart_pk == df_entries_agg.p_order,
    "left"
)

df_final_export.cache()
df_final_export.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Gerar arquivo TXT no formato solicitado

# COMMAND ----------

# Coletar dados para gerar o TXT
dados = df_final_export.collect()

# Gerar conteúdo do TXT
linhas_txt = []

for row in dados:
    # Linha do header do carrinho
    header = f"{row['cart_pk']}|{row['cart_createdTS']}|{row['cart_totalprice']}|{row['user_uid']}|{row['paymentmode_code']}|{row['paymentinfo_installments']}|{row['site_name']}|{row['address_postalcode']}|{row['sum_quantity']}|{row['count_entries']}"
    linhas_txt.append(header)
    
    # Linhas dos entries
    if row['entries']:
        for entry in row['entries']:
            entry_line = f"{entry['p_product']}|{entry['p_quantity']}|{entry['p_totalprice']}|"
            linhas_txt.append(entry_line)

# Mostrar preview
print("="*60)
print("PREVIEW DO ARQUIVO TXT (primeiras 30 linhas)")
print("="*60)
for i, linha in enumerate(linhas_txt[:30]):
    print(linha)

# COMMAND ----------

# Salvar arquivo TXT
# No Databricks, use dbutils para salvar no DBFS

txt_content = "\n".join(linhas_txt)

# Opção 1: Salvar localmente (para testes)
# with open("/tmp/top50_carrinhos.txt", "w") as f:
#     f.write(txt_content)

# Opção 2: Salvar no DBFS (Databricks)
# dbutils.fs.put(f"{BASE_PATH}top50_carrinhos.txt", txt_content, overwrite=True)

print(f"\nTotal de linhas no arquivo: {len(linhas_txt)}")
print("Arquivo pronto para exportação!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo Final

# COMMAND ----------

print("="*70)
print("RESUMO DAS ANÁLISES - CARRINHOS ABANDONADOS CANTUSTORE")
print("="*70)
print("""
ANÁLISES REALIZADAS:
1. ✅ Produtos com mais carrinhos abandonados
2. ✅ Duplas de produtos mais abandonadas juntas
3. ✅ Produtos com aumento de abandono
4. ✅ Produtos novos no primeiro mês de lançamento
5. ✅ Estados com mais abandonos

RELATÓRIOS GERADOS:
- ✅ Relatório por Produto/Mês (carrinhos, itens, valor não faturado)
- ✅ Relatório por Data (carrinhos, itens, valor não faturado)

EXPORTAÇÃO:
- ✅ Arquivo TXT com top 50 carrinhos por valor
""")
