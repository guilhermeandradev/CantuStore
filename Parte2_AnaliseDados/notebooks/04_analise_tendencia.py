# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Análise de Tendência de Abandono
# MAGIC ## Notebook 04 - Quais produtos tiveram um aumento de abandono?

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise: Produtos com Aumento de Abandono

# COMMAND ----------

# Agregar por produto e mês
df_tendencia = df_carts_items.groupBy("product", "ano_mes").agg(
    countDistinct("cart_pk").alias("qtd_carrinhos"),
    sum("quantity").alias("qtd_itens")
).orderBy("product", "ano_mes")

print(f"✓ Dados agregados por produto/mês: {df_tendencia.count():,} registros")

# COMMAND ----------

# Calcular variação mês a mês usando Window Function
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

print("✓ Variações calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produtos com Aumento no Último Período

# COMMAND ----------

# Identificar produtos onde a última variação foi positiva
window_last = Window.partitionBy("product").orderBy(col("ano_mes").desc())

df_ultimo_mes = df_tendencia_var.withColumn(
    "rn", row_number().over(window_last)
).filter(col("rn") == 1).filter(col("variacao") > 0)

print("="*80)
print("TOP 50 PRODUTOS COM AUMENTO DE ABANDONO (último período)")
print("="*80)
df_ultimo_mes.select(
    "product", 
    "ano_mes", 
    "qtd_carrinhos", 
    "qtd_mes_anterior", 
    "variacao", 
    "variacao_pct"
).orderBy(col("variacao").desc()).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produtos com Aumento Consistente

# COMMAND ----------

# Identificar produtos com aumento nos últimos 3 meses
window_last3 = Window.partitionBy("product").orderBy(col("ano_mes").desc())

df_last3_months = df_tendencia_var.withColumn(
    "rn", row_number().over(window_last3)
).filter(col("rn") <= 3)

# Produtos com variação positiva em todos os 3 últimos meses
df_aumento_consistente = df_last3_months.filter(col("variacao") > 0).groupBy("product").agg(
    count("*").alias("meses_com_aumento"),
    avg("variacao_pct").alias("variacao_media_pct"),
    sum("variacao").alias("variacao_total")
).filter(col("meses_com_aumento") == 3).orderBy(col("variacao_total").desc())

print("="*80)
print("PRODUTOS COM AUMENTO CONSISTENTE (3 meses consecutivos)")
print("="*80)
df_aumento_consistente.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Sazonalidade

# COMMAND ----------

# Agregar por mês (todos os produtos)
df_sazonalidade = df_carts_items.withColumn("mes_nome", date_format(col("cart_created"), "MM")).groupBy("mes_nome").agg(
    count("cart_pk").alias("qtd_carrinhos_total")
).orderBy("mes_nome")

print("="*80)
print("SAZONALIDADE - CARRINHOS ABANDONADOS POR MÊS")
print("="*80)
df_sazonalidade.show(12)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização SQL - Tendência Temporal

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     ano_mes,
# MAGIC     COUNT(DISTINCT cart_pk) as total_carrinhos,
# MAGIC     COUNT(DISTINCT product) as produtos_distintos,
# MAGIC     SUM(quantity) as total_itens
# MAGIC FROM carts_items
# MAGIC GROUP BY ano_mes
# MAGIC ORDER BY ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Produtos com Maior Crescimento Percentual

# COMMAND ----------

# Produtos com maior % de crescimento (que tiveram pelo menos 10 carrinhos no mês anterior)
df_maior_crescimento = df_ultimo_mes.filter(col("qtd_mes_anterior") >= 10).orderBy(col("variacao_pct").desc())

print("="*80)
print("TOP 30 PRODUTOS COM MAIOR CRESCIMENTO PERCENTUAL")
print("(filtro: >= 10 carrinhos no mês anterior)")
print("="*80)
df_maior_crescimento.select(
    "product",
    "ano_mes",
    "qtd_mes_anterior",
    "qtd_carrinhos",
    "variacao",
    "variacao_pct"
).show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Resultados

# COMMAND ----------

# Salvar produtos com aumento
output_path = f"{OUTPUT_PATH}produtos_aumento_abandono"
df_ultimo_mes.write.mode("overwrite").csv(output_path, header=True)

# Salvar produtos com aumento consistente
output_path_consistente = f"{OUTPUT_PATH}produtos_aumento_consistente"
df_aumento_consistente.write.mode("overwrite").csv(output_path_consistente, header=True)

# Salvar tendência completa
output_path_tendencia = f"{OUTPUT_PATH}tendencia_abandono_completa"
df_tendencia_var.write.mode("overwrite").csv(output_path_tendencia, header=True)

print(f"✓ Resultados salvos em: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("""
================================================================================
ANÁLISE COMPLETA
================================================================================

Questão: Quais produtos tiveram um aumento de abandono?

Respostas:
1. Produtos com aumento no último período: veja tabela acima
2. Produtos com aumento consistente (3 meses): veja segunda tabela
3. Análise de sazonalidade por mês do ano

Insights:
- Identificados produtos com crescimento percentual expressivo
- Análise de tendência temporal completa disponível nos CSVs salvos

Próximo notebook: 05_analise_produtos_novos.py
================================================================================
""")
