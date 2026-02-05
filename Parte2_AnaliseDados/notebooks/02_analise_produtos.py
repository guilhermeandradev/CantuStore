# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Análise de Produtos Mais Abandonados
# MAGIC ## Notebook 02 - Quais os produtos que mais tiveram carrinhos abandonados?

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise: Produtos com Mais Carrinhos Abandonados

# COMMAND ----------

# Agregar por produto
df_produtos_abandonados = df_carts_items.groupBy("product").agg(
    count("cart_pk").alias("qtd_carrinhos"),
    sum("quantity").alias("qtd_itens"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_total")
).orderBy(col("qtd_carrinhos").desc())

print("="*70)
print("TOP 50 PRODUTOS COM MAIS CARRINHOS ABANDONADOS")
print("="*70)
df_produtos_abandonados.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas Gerais

# COMMAND ----------

# Estatísticas dos top 10
print("="*70)
print("ESTATÍSTICAS DOS TOP 10 PRODUTOS")
print("="*70)

df_top10 = df_produtos_abandonados.limit(10)

total_carrinhos = df_top10.agg(sum("qtd_carrinhos")).collect()[0][0]
total_itens = df_top10.agg(sum("qtd_itens")).collect()[0][0]
total_valor = df_top10.agg(sum("valor_total")).collect()[0][0]

print(f"Total de carrinhos (Top 10): {total_carrinhos:,}")
print(f"Total de itens (Top 10): {total_itens:,}")
print(f"Valor total (Top 10): R$ {total_valor:,.2f}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização (usando SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     product,
# MAGIC     COUNT(cart_pk) as qtd_carrinhos,
# MAGIC     SUM(quantity) as qtd_itens,
# MAGIC     ROUND(SUM(entry_totalprice), 2) as valor_total
# MAGIC FROM carts_items
# MAGIC GROUP BY product
# MAGIC ORDER BY qtd_carrinhos DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Resultado

# COMMAND ----------

# Salvar resultado completo
output_path = f"{OUTPUT_PATH}produtos_mais_abandonados"
df_produtos_abandonados.write.mode("overwrite").csv(output_path, header=True)

print(f"✓ Resultado salvo em: {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("""
================================================================================
ANÁLISE COMPLETA
================================================================================

Questão: Quais os produtos que mais tiveram carrinhos abandonados?

Resposta: Veja a tabela acima com os top 50 produtos.

O resultado completo foi salvo em CSV para análise detalhada.

Próximo notebook: 03_analise_duplas.py
================================================================================
""")
