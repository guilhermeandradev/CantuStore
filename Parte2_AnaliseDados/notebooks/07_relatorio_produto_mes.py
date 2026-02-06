# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Relatório por Produto/Mês
# MAGIC ## Notebook 07 - Relatório Mensal de Produtos Abandonados

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório: Produto x Mês
# MAGIC 
# MAGIC Para cada produto, mês a mês, informando:
# MAGIC - Quantidade de carrinhos abandonados
# MAGIC - Quantidade de itens abandonados
# MAGIC - Valor não faturado

# COMMAND ----------

# Agregar por produto e mês
df_relatorio_produto_mes = df_carts_items.groupBy("product", "ano_mes").agg(
    countDistinct("cart_pk").alias("qtd_carrinhos_abandonados"),
    sum("quantity").alias("qtd_itens_abandonados"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_nao_faturado")
).orderBy("product", "ano_mes")

print(f"✓ Relatório gerado: {df_relatorio_produto_mes.count():,} registros")

# COMMAND ----------

# Preview do relatório
print("="*80)
print("RELATÓRIO POR PRODUTO - MÊS A MÊS (Preview)")
print("="*80)
df_relatorio_produto_mes.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas do Relatório

# COMMAND ----------

# Totais gerais
totais = df_relatorio_produto_mes.agg(
    sum("qtd_carrinhos_abandonados").alias("total_carrinhos"),
    sum("qtd_itens_abandonados").alias("total_itens"),
    spark_round(sum("valor_nao_faturado"), 2).alias("total_valor")
).collect()[0]

print("="*80)
print("TOTAIS GERAIS DO RELATÓRIO")
print("="*80)
print(f"Total de carrinhos abandonados: {totais['total_carrinhos']:,}")
print(f"Total de itens abandonados: {totais['total_itens']:,}")
print(f"Valor total não faturado: R$ {totais['total_valor']:,.2f}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top Produtos por Valor Não Faturado

# COMMAND ----------

# Agregar por produto (total histórico)
df_top_produtos_valor = df_relatorio_produto_mes.groupBy("product").agg(
    sum("qtd_carrinhos_abandonados").alias("total_carrinhos"),
    sum("qtd_itens_abandonados").alias("total_itens"),
    spark_round(sum("valor_nao_faturado"), 2).alias("total_valor_nao_faturado")
).orderBy(col("total_valor_nao_faturado").desc())

print("="*80)
print("TOP 30 PRODUTOS POR VALOR NÃO FATURADO (histórico)")
print("="*80)
df_top_produtos_valor.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise Mensal Consolidada

# COMMAND ----------

# Totais por mês (todos os produtos)
df_consolidado_mes = df_relatorio_produto_mes.groupBy("ano_mes").agg(
    sum("qtd_carrinhos_abandonados").alias("total_carrinhos"),
    sum("qtd_itens_abandonados").alias("total_itens"),
    spark_round(sum("valor_nao_faturado"), 2).alias("total_valor"),
    count("product").alias("qtd_produtos_distintos")
).orderBy("ano_mes")

print("="*80)
print("CONSOLIDADO MENSAL - TODOS OS PRODUTOS")
print("="*80)
df_consolidado_mes.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produtos com Maior Variação Mensal

# COMMAND ----------

# Calcular variação mês a mês por produto
window_mes = Window.partitionBy("product").orderBy("ano_mes")

df_variacao_mensal = df_relatorio_produto_mes.withColumn(
    "valor_mes_anterior", lag("valor_nao_faturado", 1).over(window_mes)
).withColumn(
    "variacao_valor", 
    col("valor_nao_faturado") - coalesce(col("valor_mes_anterior"), lit(0))
).withColumn(
    "variacao_pct",
    when(col("valor_mes_anterior").isNotNull() & (col("valor_mes_anterior") > 0),
         spark_round((col("variacao_valor") / col("valor_mes_anterior")) * 100, 2)
    ).otherwise(None)
)

# Produtos com maior variação positiva no último mês
window_ultimo = Window.partitionBy("product").orderBy(col("ano_mes").desc())
df_maior_variacao = df_variacao_mensal.withColumn(
    "rn", row_number().over(window_ultimo)
).filter(col("rn") == 1).filter(col("variacao_valor") > 0).orderBy(col("variacao_valor").desc())

print("="*80)
print("TOP 30 PRODUTOS COM MAIOR AUMENTO DE VALOR (último mês)")
print("="*80)
df_maior_variacao.select(
    "product",
    "ano_mes",
    "valor_mes_anterior",
    "valor_nao_faturado",
    "variacao_valor",
    "variacao_pct"
).show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Relatório mensal por produto
# MAGIC SELECT 
# MAGIC     product,
# MAGIC     ano_mes,
# MAGIC     COUNT(DISTINCT cart_pk) as qtd_carrinhos_abandonados,
# MAGIC     SUM(quantity) as qtd_itens_abandonados,
# MAGIC     ROUND(SUM(entry_totalprice), 2) as valor_nao_faturado
# MAGIC FROM carts_items
# MAGIC GROUP BY product, ano_mes
# MAGIC ORDER BY ano_mes DESC, valor_nao_faturado DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Relatório Completo

# COMMAND ----------

# Salvar relatório principal
output_path_principal = f"{OUTPUT_PATH}relatorio_produto_mes"
df_relatorio_produto_mes.write.mode("overwrite").csv(output_path_principal, header=True)

# Salvar consolidado mensal
output_path_consolidado = f"{OUTPUT_PATH}relatorio_consolidado_mensal"
df_consolidado_mes.write.mode("overwrite").csv(output_path_consolidado, header=True)

# Salvar top produtos por valor
output_path_top_valor = f"{OUTPUT_PATH}top_produtos_valor_nao_faturado"
df_top_produtos_valor.write.mode("overwrite").csv(output_path_top_valor, header=True)

print(f"✓ Relatórios salvos em: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

total_registros = df_relatorio_produto_mes.count()
total_produtos = df_relatorio_produto_mes.select("product").distinct().count()
total_meses = df_relatorio_produto_mes.select("ano_mes").distinct().count()

print(f"""
================================================================================
RELATÓRIO POR PRODUTO/MÊS GERADO
================================================================================

Estrutura do relatório:
- Total de registros: {total_registros:,}
- Total de produtos únicos: {total_produtos:,}
- Total de meses analisados: {total_meses}

Colunas:
1. product: ID do produto
2. ano_mes: Período (YYYY-MM)
3. qtd_carrinhos_abandonados: Quantidade de carrinhos
4. qtd_itens_abandonados: Quantidade total de itens
5. valor_nao_faturado: Valor total não faturado (R$)

Arquivos salvos:
- relatorio_produto_mes.csv
- relatorio_consolidado_mensal.csv
- top_produtos_valor_nao_faturado.csv

Próximo notebook: 08_relatorio_data.py
================================================================================
""")
