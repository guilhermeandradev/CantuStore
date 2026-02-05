# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Análise de Duplas de Produtos
# MAGIC ## Notebook 03 - Quais as duplas de produtos em conjunto que mais tiveram carrinhos abandonados?

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise: Duplas de Produtos Mais Abandonadas

# COMMAND ----------

# Self-join para encontrar pares de produtos no mesmo carrinho
df_pares = df_carts_items.alias("a").join(
    df_carts_items.alias("b"),
    (col("a.cart_pk") == col("b.cart_pk")) & 
    (col("a.product") < col("b.product")),  # Evitar duplicatas (A,B) e (B,A)
    "inner"
).select(
    col("a.cart_pk"),
    col("a.product").alias("produto_1"),
    col("b.product").alias("produto_2")
).distinct()

print("✓ Pares de produtos identificados")
print(f"Total de pares únicos: {df_pares.count():,}")

# COMMAND ----------

# Contar ocorrências de cada par
df_duplas_abandonadas = df_pares.groupBy("produto_1", "produto_2").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy(col("qtd_carrinhos").desc())

print("="*70)
print("TOP 50 DUPLAS DE PRODUTOS MAIS ABANDONADAS JUNTAS")
print("="*70)
df_duplas_abandonadas.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas das Duplas

# COMMAND ----------

# Top 20 duplas
df_top20_duplas = df_duplas_abandonadas.limit(20)

total_carrinhos_duplas = df_top20_duplas.agg(sum("qtd_carrinhos")).collect()[0][0]

print("="*70)
print("ESTATÍSTICAS DAS TOP 20 DUPLAS")
print("="*70)
print(f"Total de carrinhos com essas duplas: {total_carrinhos_duplas:,}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Triplas (Bônus)

# COMMAND ----------

# Identificar triplas de produtos (opcional)
print("Analisando triplas de produtos...")

# Self-join triplo
df_triplas = df_carts_items.alias("a").join(
    df_carts_items.alias("b"),
    (col("a.cart_pk") == col("b.cart_pk")) & 
    (col("a.product") < col("b.product")),
    "inner"
).join(
    df_carts_items.alias("c"),
    (col("a.cart_pk") == col("c.cart_pk")) & 
    (col("b.product") < col("c.product")),
    "inner"
).select(
    col("a.cart_pk"),
    col("a.product").alias("produto_1"),
    col("b.product").alias("produto_2"),
    col("c.product").alias("produto_3")
).distinct()

df_triplas_count = df_triplas.groupBy("produto_1", "produto_2", "produto_3").agg(
    count("cart_pk").alias("qtd_carrinhos")
).orderBy(col("qtd_carrinhos").desc())

print("="*70)
print("TOP 20 TRIPLAS DE PRODUTOS MAIS ABANDONADAS JUNTAS")
print("="*70)
df_triplas_count.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplas mais comuns usando SQL
# MAGIC SELECT 
# MAGIC     a.product as produto_1,
# MAGIC     b.product as produto_2,
# MAGIC     COUNT(DISTINCT a.cart_pk) as qtd_carrinhos
# MAGIC FROM carts_items a
# MAGIC INNER JOIN carts_items b 
# MAGIC     ON a.cart_pk = b.cart_pk 
# MAGIC     AND a.product < b.product
# MAGIC GROUP BY a.product, b.product
# MAGIC ORDER BY qtd_carrinhos DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Resultados

# COMMAND ----------

# Salvar duplas
output_path_duplas = f"{OUTPUT_PATH}duplas_produtos_abandonados"
df_duplas_abandonadas.write.mode("overwrite").csv(output_path_duplas, header=True)

# Salvar triplas
output_path_triplas = f"{OUTPUT_PATH}triplas_produtos_abandonados"
df_triplas_count.write.mode("overwrite").csv(output_path_triplas, header=True)

print(f"✓ Duplas salvas em: {output_path_duplas}")
print(f"✓ Triplas salvas em: {output_path_triplas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("""
================================================================================
ANÁLISE COMPLETA
================================================================================

Questão: Quais as duplas de produtos em conjunto que mais tiveram carrinhos 
abandonados?

Resposta: Veja a tabela acima com as top 50 duplas.

Insight adicional: Também identificamos triplas de produtos frequentemente 
abandonadas juntas.

Próximo notebook: 04_analise_tendencia.py
================================================================================
""")
