# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Análise de Produtos Novos
# MAGIC ## Notebook 05 - Quais os produtos novos e a quantidade de carrinhos no seu primeiro mês de lançamento?

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise: Produtos Novos no Primeiro Mês

# COMMAND ----------

# Identificar o primeiro mês de cada produto
window_primeiro = Window.partitionBy("product").orderBy("ano_mes")

df_primeiro_mes = df_carts_items.groupBy("product", "ano_mes").agg(
    count("cart_pk").alias("qtd_carrinhos"),
    sum("quantity").alias("qtd_itens"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_total")
).withColumn(
    "rn", row_number().over(window_primeiro)
).filter(col("rn") == 1).drop("rn")

print(f"✓ Identificados {df_primeiro_mes.count():,} produtos únicos")

# COMMAND ----------

print("="*80)
print("TOP 50 PRODUTOS NOVOS - DESEMPENHO NO PRIMEIRO MÊS DE LANÇAMENTO")
print("="*80)
df_primeiro_mes.select(
    "product", 
    col("ano_mes").alias("primeiro_mes_lancamento"),
    "qtd_carrinhos",
    "qtd_itens",
    "valor_total"
).orderBy(col("qtd_carrinhos").desc()).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise por Período de Lançamento

# COMMAND ----------

# Agrupar produtos por período de lançamento
df_lancamentos_periodo = df_primeiro_mes.groupBy("ano_mes").agg(
    count("product").alias("qtd_produtos_lancados"),
    sum("qtd_carrinhos").alias("total_carrinhos"),
    spark_round(avg("qtd_carrinhos"), 2).alias("media_carrinhos_produto")
).orderBy("ano_mes")

print("="*80)
print("PRODUTOS LANÇADOS POR PERÍODO")
print("="*80)
df_lancamentos_periodo.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produtos Lançados Recentemente (Últimos 6 Meses)

# COMMAND ----------

# Identificar os 6 meses mais recentes
df_max_mes = df_carts_items.agg(max("ano_mes")).collect()[0][0]
print(f"Mês mais recente nos dados: {df_max_mes}")

# Produtos lançados nos últimos 6 meses
df_recentes = df_primeiro_mes.orderBy(col("ano_mes").desc()).limit(100)

print("="*80)
print("PRODUTOS LANÇADOS RECENTEMENTE (Top 100 mais recentes)")
print("="*80)
df_recentes.select(
    "product",
    col("ano_mes").alias("mes_lancamento"),
    "qtd_carrinhos",
    "qtd_itens",
    "valor_total"
).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Performance no Lançamento

# COMMAND ----------

# Classificar produtos por performance no lançamento
df_classificacao = df_primeiro_mes.withColumn(
    "classificacao_carrinhos",
    when(col("qtd_carrinhos") >= 100, "Excelente")
    .when(col("qtd_carrinhos") >= 50, "Bom")
    .when(col("qtd_carrinhos") >= 20, "Regular")
    .otherwise("Baixo")
)

# Contar por classificação
df_por_classificacao = df_classificacao.groupBy("classificacao_carrinhos").agg(
    count("product").alias("qtd_produtos")
).orderBy(col("qtd_produtos").desc())

print("="*80)
print("DISTRIBUIÇÃO DE PRODUTOS POR PERFORMANCE NO LANÇAMENTO")
print("="*80)
df_por_classificacao.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produtos com Melhor Início

# COMMAND ----------

# Top produtos com melhor performance no primeiro mês
df_melhores_lancamentos = df_primeiro_mes.orderBy(col("qtd_carrinhos").desc()).limit(20)

print("="*80)
print("TOP 20 PRODUTOS COM MELHOR PERFORMANCE NO LANÇAMENTO")
print("="*80)
df_melhores_lancamentos.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evolução Após o Lançamento

# COMMAND ----------

# Para os top 10 produtos, ver evolução nos 3 primeiros meses
top_10_produtos = df_primeiro_mes.orderBy(col("qtd_carrinhos").desc()).limit(10).select("product").rdd.flatMap(lambda x: x).collect()

df_evolucao = df_carts_items.filter(col("product").isin(top_10_produtos)).groupBy("product", "ano_mes").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy("product", "ano_mes")

# Para cada produto, pegar os 3 primeiros meses
window_3_meses = Window.partitionBy("product").orderBy("ano_mes")
df_evolucao_3m = df_evolucao.withColumn(
    "rn", row_number().over(window_3_meses)
).filter(col("rn") <= 3)

print("="*80)
print("EVOLUÇÃO DOS TOP 10 PRODUTOS NOS 3 PRIMEIROS MESES")
print("="*80)
df_evolucao_3m.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Produtos por ano de lançamento
# MAGIC WITH primeiro_mes AS (
# MAGIC     SELECT 
# MAGIC         product,
# MAGIC         MIN(ano_mes) as primeiro_mes,
# MAGIC         YEAR(MIN(cart_created)) as ano_lancamento
# MAGIC     FROM carts_items
# MAGIC     GROUP BY product
# MAGIC )
# MAGIC SELECT 
# MAGIC     ano_lancamento,
# MAGIC     COUNT(DISTINCT product) as qtd_produtos_lancados
# MAGIC FROM primeiro_mes
# MAGIC GROUP BY ano_lancamento
# MAGIC ORDER BY ano_lancamento

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Resultados

# COMMAND ----------

# Salvar produtos novos
output_path = f"{OUTPUT_PATH}produtos_novos_primeiro_mes"
df_primeiro_mes.write.mode("overwrite").csv(output_path, header=True)

# Salvar lançamentos por período
output_path_periodo = f"{OUTPUT_PATH}lancamentos_por_periodo"
df_lancamentos_periodo.write.mode("overwrite").csv(output_path_periodo, header=True)

# Salvar produtos recentes
output_path_recentes = f"{OUTPUT_PATH}produtos_lancados_recentemente"
df_recentes.write.mode("overwrite").csv(output_path_recentes, header=True)

print(f"✓ Resultados salvos em: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

total_produtos_unicos = df_primeiro_mes.count()
media_carrinhos_lancamento = df_primeiro_mes.agg(avg("qtd_carrinhos")).collect()[0][0]

print(f"""
================================================================================
ANÁLISE COMPLETA
================================================================================

Questão: Quais os produtos novos e a quantidade de carrinhos no seu primeiro 
mês de lançamento?

Resposta:
- Total de produtos únicos identificados: {total_produtos_unicos:,}
- Média de carrinhos no primeiro mês: {media_carrinhos_lancamento:.2f}
- Veja as tabelas acima para detalhes por produto

Insights adicionais:
- Análise de performance no lançamento (classificação)
- Evolução dos top produtos nos primeiros 3 meses
- Distribuição de lançamentos por período

Próximo notebook: 06_analise_estados.py
================================================================================
""")
