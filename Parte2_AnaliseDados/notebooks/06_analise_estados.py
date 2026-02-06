# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Análise de Abandonos por Estado
# MAGIC ## Notebook 06 - Quais estados tiveram mais abandonos?

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise: Estados com Mais Abandonos

# COMMAND ----------

# JOIN carts com addresses e regions para obter estados
df_carts_estados = df_carts.alias("c").join(
    df_addresses.alias("a"),
    col("c.p_paymentaddress") == col("a.PK"),
    "left"
).join(
    df_regions.alias("r"),
    col("a.p_region").cast("long") == col("r.PK"),  # Cast necessário: p_region é Double, PK é Long
    "left"
).select(
    col("c.PK").alias("cart_pk"),
    (col("c.p_totalprice") / 100).alias("cart_totalprice"),  # Converter de centavos para reais
    col("c.createdTS").alias("cart_created"),
    col("r.p_isocodeshort").alias("estado"),
    col("a.p_postalcode").alias("cep")
).filter(col("estado").isNotNull())

print(f"✓ Carrinhos com informação de estado: {df_carts_estados.count():,}")

# COMMAND ----------

# Agregar por estado
df_abandonos_estado = df_carts_estados.groupBy("estado").agg(
    count("cart_pk").alias("qtd_carrinhos"),
    spark_round(sum("cart_totalprice"), 2).alias("valor_total"),
    spark_round(avg("cart_totalprice"), 2).alias("ticket_medio")
).orderBy(col("qtd_carrinhos").desc())

print("="*80)
print("ABANDONOS POR ESTADO")
print("="*80)
df_abandonos_estado.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise Temporal por Estado

# COMMAND ----------

# Abandonos por estado e ano/mês
df_estado_temporal = df_carts_estados.withColumn(
    "ano_mes", date_format(col("cart_created"), "yyyy-MM")
).groupBy("estado", "ano_mes").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy("estado", "ano_mes")

print("✓ Dados temporais por estado calculados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 5 Estados - Evolução Temporal

# COMMAND ----------

# Top 5 estados
# Convertido para list comprehension (Serverless não suporta RDD)
top_5_estados = [row.estado for row in df_abandonos_estado.limit(5).select("estado").collect()]

df_top5_evolucao = df_estado_temporal.filter(col("estado").isin(top_5_estados)).orderBy("ano_mes", "estado")

print("="*80)
print("EVOLUÇÃO TEMPORAL - TOP 5 ESTADOS")
print("="*80)
df_top5_evolucao.show(100, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Concentração

# COMMAND ----------

# Calcular % de participação de cada estado
total_carrinhos_geral = df_abandonos_estado.agg(sum("qtd_carrinhos")).collect()[0][0]

df_participacao = df_abandonos_estado.withColumn(
    "participacao_pct",
    spark_round((col("qtd_carrinhos") / total_carrinhos_geral) * 100, 2)
).withColumn(
    "participacao_acumulada",
    spark_round(sum("participacao_pct").over(Window.orderBy(col("qtd_carrinhos").desc())), 2)
)

print("="*80)
print("PARTICIPAÇÃO E CONCENTRAÇÃO POR ESTADO")
print("="*80)
df_participacao.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estados com Maior Ticket Médio

# COMMAND ----------

# Estados ordenados por ticket médio (com pelo menos 100 carrinhos)
df_maior_ticket = df_abandonos_estado.filter(col("qtd_carrinhos") >= 100).orderBy(col("ticket_medio").desc())

print("="*80)
print("ESTADOS COM MAIOR TICKET MÉDIO (min. 100 carrinhos)")
print("="*80)
df_maior_ticket.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise Regional

# COMMAND ----------

# Criar macro-regiões
df_regioes = df_abandonos_estado.withColumn(
    "regiao",
    when(col("estado").isin(["SP", "RJ", "MG", "ES"]), "Sudeste")
    .when(col("estado").isin(["PR", "SC", "RS"]), "Sul")
    .when(col("estado").isin(["GO", "MT", "MS", "DF"]), "Centro-Oeste")
    .when(col("estado").isin(["BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MA"]), "Nordeste")
    .when(col("estado").isin(["AM", "RR", "AP", "PA", "TO", "RO", "AC"]), "Norte")
    .otherwise("Outros")
)

df_por_regiao = df_regioes.groupBy("regiao").agg(
    sum("qtd_carrinhos").alias("qtd_carrinhos"),
    spark_round(sum("valor_total"), 2).alias("valor_total"),
    spark_round(avg("ticket_medio"), 2).alias("ticket_medio_regiao")
).orderBy(col("qtd_carrinhos").desc())

print("="*80)
print("ABANDONOS POR REGIÃO")
print("="*80)
df_por_regiao.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Estados com mais abandonos (preços convertidos de centavos para reais)
# MAGIC SELECT 
# MAGIC     r.p_isocodeshort as estado,
# MAGIC     COUNT(DISTINCT c.PK) as qtd_carrinhos,
# MAGIC     ROUND(SUM(c.p_totalprice / 100), 2) as valor_total,
# MAGIC     ROUND(AVG(c.p_totalprice / 100), 2) as ticket_medio
# MAGIC FROM carts c
# MAGIC LEFT JOIN addresses a ON c.p_paymentaddress = a.PK
# MAGIC LEFT JOIN regions r ON CAST(a.p_region AS BIGINT) = r.PK
# MAGIC WHERE r.p_isocodeshort IS NOT NULL
# MAGIC GROUP BY r.p_isocodeshort
# MAGIC ORDER BY qtd_carrinhos DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de CEPs

# COMMAND ----------

# Top CEPs com mais abandonos (dentro dos top estados)
df_top_ceps = df_carts_estados.filter(col("estado").isin(top_5_estados)).filter(col("cep").isNotNull()).groupBy("estado", "cep").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy(col("qtd_carrinhos").desc())

print("="*80)
print("TOP 30 CEPs COM MAIS ABANDONOS (Top 5 Estados)")
print("="*80)
df_top_ceps.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Resultados

# COMMAND ----------

# Salvar abandonos por estado
output_path_estados = f"{OUTPUT_PATH}abandonos_por_estado"
df_abandonos_estado.write.mode("overwrite").csv(output_path_estados, header=True)

# Salvar evolução temporal
output_path_temporal = f"{OUTPUT_PATH}abandonos_estado_temporal"
df_estado_temporal.write.mode("overwrite").csv(output_path_temporal, header=True)

# Salvar por região
output_path_regiao = f"{OUTPUT_PATH}abandonos_por_regiao"
df_por_regiao.write.mode("overwrite").csv(output_path_regiao, header=True)

# Salvar participação
output_path_participacao = f"{OUTPUT_PATH}participacao_por_estado"
df_participacao.write.mode("overwrite").csv(output_path_participacao, header=True)

print(f"✓ Resultados salvos em: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

top_estado = df_abandonos_estado.first()
total_estados = df_abandonos_estado.count()

print(f"""
================================================================================
ANÁLISE COMPLETA
================================================================================

Questão: Quais estados tiveram mais abandonos?

Resposta:
- Estado com mais abandonos: {top_estado['estado']} ({top_estado['qtd_carrinhos']:,} carrinhos)
- Total de estados analisados: {total_estados}
- Veja tabela completa acima

Insights adicionais:
- Análise de concentração (% participação)
- Estados com maior ticket médio
- Distribuição por macro-regiões
- Evolução temporal dos top estados
- Top CEPs com mais abandonos

Próximo notebook: 07_relatorio_produto_mes.py
================================================================================
""")
