# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Relatório por Data
# MAGIC ## Notebook 08 - Relatório Diário de Carrinhos Abandonados

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório: Por Data
# MAGIC
# MAGIC Para cada data, informando:
# MAGIC - Quantidade de carrinhos abandonados
# MAGIC - Quantidade de itens abandonados
# MAGIC - Valor não faturado

# COMMAND ----------

# Agregar por data
df_relatorio_data = df_carts_items.groupBy("data").agg(
    countDistinct("cart_pk").alias("qtd_carrinhos_abandonados"),
    sum("quantity").alias("qtd_itens_abandonados"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_nao_faturado")
).orderBy("data")

print(f"✓ Relatório gerado: {df_relatorio_data.count():,} dias com dados")

# COMMAND ----------

# Preview do relatório
print("="*80)
print("RELATÓRIO POR DATA (Preview - Últimos 50 dias)")
print("="*80)
df_relatorio_data.orderBy(col("data").desc()).show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas do Relatório

# COMMAND ----------

# Totais gerais
totais = df_relatorio_data.agg(
    sum("qtd_carrinhos_abandonados").alias("total_carrinhos"),
    sum("qtd_itens_abandonados").alias("total_itens"),
    spark_round(sum("valor_nao_faturado"), 2).alias("total_valor")
).collect()[0]

# Médias diárias
medias = df_relatorio_data.agg(
    spark_round(avg("qtd_carrinhos_abandonados"), 2).alias("media_carrinhos"),
    spark_round(avg("qtd_itens_abandonados"), 2).alias("media_itens"),
    spark_round(avg("valor_nao_faturado"), 2).alias("media_valor")
).collect()[0]

print("="*80)
print("TOTAIS E MÉDIAS DO RELATÓRIO")
print("="*80)
print(f"Total de carrinhos abandonados: {totais['total_carrinhos']:,}")
print(f"Total de itens abandonados: {totais['total_itens']:,}")
print(f"Valor total não faturado: R$ {totais['total_valor']:,.2f}")
print()
print(f"Média de carrinhos por dia: {medias['media_carrinhos']:,.2f}")
print(f"Média de itens por dia: {medias['media_itens']:,.2f}")
print(f"Média de valor por dia: R$ {medias['media_valor']:,.2f}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dias com Maior Volume de Abandonos

# COMMAND ----------

# Top 30 dias com mais carrinhos
df_top_dias_carrinhos = df_relatorio_data.orderBy(col("qtd_carrinhos_abandonados").desc())

print("="*80)
print("TOP 30 DIAS COM MAIS CARRINHOS ABANDONADOS")
print("="*80)
df_top_dias_carrinhos.show(30, truncate=False)

# COMMAND ----------

# Top 30 dias por valor
df_top_dias_valor = df_relatorio_data.orderBy(col("valor_nao_faturado").desc())

print("="*80)
print("TOP 30 DIAS COM MAIOR VALOR NÃO FATURADO")
print("="*80)
df_top_dias_valor.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise por Dia da Semana

# COMMAND ----------

# Adicionar dia da semana
from pyspark.sql.functions import dayofweek, date_format as df_format

df_dia_semana = df_carts_items.withColumn(
    "dia_semana", dayofweek(col("cart_created"))
).withColumn(
    "dia_semana_nome", 
    when(col("dia_semana") == 1, "Domingo")
    .when(col("dia_semana") == 2, "Segunda")
    .when(col("dia_semana") == 3, "Terça")
    .when(col("dia_semana") == 4, "Quarta")
    .when(col("dia_semana") == 5, "Quinta")
    .when(col("dia_semana") == 6, "Sexta")
    .otherwise("Sábado")
)

df_por_dia_semana = df_dia_semana.groupBy("dia_semana", "dia_semana_nome").agg(
    countDistinct("cart_pk").alias("qtd_carrinhos"),
    spark_round(avg(col("entry_totalprice")), 2).alias("valor_medio")
).orderBy("dia_semana")

print("="*80)
print("ABANDONOS POR DIA DA SEMANA")
print("="*80)
df_por_dia_semana.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tendência Temporal (Média Móvel de 7 dias)

# COMMAND ----------

# Calcular média móvel de 7 dias
window_7d = Window.orderBy("data").rowsBetween(-6, 0)

df_media_movel = df_relatorio_data.withColumn(
    "media_movel_7d_carrinhos",
    spark_round(avg("qtd_carrinhos_abandonados").over(window_7d), 2)
).withColumn(
    "media_movel_7d_valor",
    spark_round(avg("valor_nao_faturado").over(window_7d), 2)
)

print("="*80)
print("TENDÊNCIA - MÉDIA MÓVEL 7 DIAS (Últimos 30 dias)")
print("="*80)
df_media_movel.orderBy(col("data").desc()).show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificar Outliers

# COMMAND ----------

# Calcular percentis e identificar outliers
from pyspark.sql.functions import expr

percentis = df_relatorio_data.approxQuantile("qtd_carrinhos_abandonados", [0.25, 0.75], 0.01)
q1 = percentis[0]
q3 = percentis[1]
iqr = q3 - q1
limite_superior = q3 + (1.5 * iqr)
limite_inferior = q1 - (1.5 * iqr) if q1 - (1.5 * iqr) > 0 else 0 

df_outliers = df_relatorio_data.filter(
    (col("qtd_carrinhos_abandonados") > limite_superior) |
    (col("qtd_carrinhos_abandonados") < limite_inferior)
).orderBy(col("qtd_carrinhos_abandonados").desc())

print("="*80)
print(f"OUTLIERS IDENTIFICADOS (IQR: {limite_inferior:.0f} - {limite_superior:.0f})")
print("="*80)
df_outliers.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Relatório diário
# MAGIC SELECT 
# MAGIC     data,
# MAGIC     COUNT(DISTINCT cart_pk) as qtd_carrinhos_abandonados,
# MAGIC     SUM(quantity) as qtd_itens_abandonados,
# MAGIC     ROUND(SUM(entry_totalprice), 2) as valor_nao_faturado
# MAGIC FROM carts_items
# MAGIC GROUP BY data
# MAGIC ORDER BY data DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Padrões Mensais

# COMMAND ----------

# Comparar início vs final do mês
df_padrao_mensal = df_carts_items.withColumn(
    "dia_mes", dayofmonth(col("cart_created"))
).withColumn(
    "periodo_mes",
    when(col("dia_mes") <= 10, "Início (1-10)")
    .when(col("dia_mes") <= 20, "Meio (11-20)")
    .otherwise("Final (21-31)")
).groupBy("periodo_mes").agg(
    countDistinct("cart_pk").alias("qtd_carrinhos"),
    spark_round(avg(col("entry_totalprice")), 2).alias("valor_medio")
).orderBy("periodo_mes")

print("="*80)
print("PADRÃO DE ABANDONOS POR PERÍODO DO MÊS")
print("="*80)
df_padrao_mensal.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Relatório Completo

# COMMAND ----------

# Salvar relatório principal
output_path_principal = f"{OUTPUT_PATH}relatorio_por_data"
df_relatorio_data.write.mode("overwrite").csv(output_path_principal, header=True)

# Salvar média móvel
output_path_media_movel = f"{OUTPUT_PATH}relatorio_data_media_movel"
df_media_movel.write.mode("overwrite").csv(output_path_media_movel, header=True)

# Salvar por dia da semana
output_path_dia_semana = f"{OUTPUT_PATH}relatorio_por_dia_semana"
df_por_dia_semana.write.mode("overwrite").csv(output_path_dia_semana, header=True)

# Salvar outliers
output_path_outliers = f"{OUTPUT_PATH}dias_outliers"
df_outliers.write.mode("overwrite").csv(output_path_outliers, header=True)

print(f"✓ Relatórios salvos em: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

total_dias = df_relatorio_data.count()
primeiro_dia = df_relatorio_data.agg(min("data")).collect()[0][0]
ultimo_dia = df_relatorio_data.agg(max("data")).collect()[0][0]

print(f"""
================================================================================
RELATÓRIO POR DATA GERADO
================================================================================

Período analisado:
- Primeiro dia: {primeiro_dia}
- Último dia: {ultimo_dia}
- Total de dias com dados: {total_dias:,}

Colunas do relatório:
1. data: Data (YYYY-MM-DD)
2. qtd_carrinhos_abandonados: Quantidade de carrinhos
3. qtd_itens_abandonados: Quantidade total de itens
4. valor_nao_faturado: Valor total não faturado (R$)

Insights:
- Análise por dia da semana
- Identificação de outliers
- Tendência com média móvel de 7 dias
- Padrão por período do mês

Arquivos salvos:
- relatorio_por_data.csv
- relatorio_data_media_movel.csv
- relatorio_por_dia_semana.csv
- dias_outliers.csv

Próximo notebook: 09_exportacao_txt.py
================================================================================
""")
