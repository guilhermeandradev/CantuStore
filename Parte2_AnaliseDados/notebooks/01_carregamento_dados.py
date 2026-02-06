# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Carregamento de Dados
# MAGIC ## Notebook 01 - Carregar e Validar Dados
# MAGIC 
# MAGIC Este notebook carrega todas as tabelas e cria views temporárias para análise.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento das Tabelas Parquet

# COMMAND ----------

print("Carregando tabelas Parquet...")

# Carregar tb_carts
df_carts = spark.read.parquet(PATHS["carts"])
df_carts.createOrReplaceTempView("carts")
print(f"✓ tb_carts: {df_carts.count():,} registros")

# Carregar tb_cartentries
df_cartentries = spark.read.parquet(PATHS["cartentries"])
df_cartentries.createOrReplaceTempView("cartentries")
print(f"✓ tb_cartentries: {df_cartentries.count():,} registros")

# Carregar tb_addresses
df_addresses = spark.read.parquet(PATHS["addresses"])
df_addresses.createOrReplaceTempView("addresses")
print(f"✓ tb_addresses: {df_addresses.count():,} registros")

# Carregar tb_paymentinfos
df_paymentinfos = spark.read.parquet(PATHS["paymentinfos"])
df_paymentinfos.createOrReplaceTempView("paymentinfos")
print(f"✓ tb_paymentinfos: {df_paymentinfos.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento das Tabelas CSV

# COMMAND ----------

print("Carregando tabelas CSV...")

# Carregar tb_users (delimiter: pipe)
df_users = spark.read.csv(PATHS["users"], header=True, inferSchema=True, sep="|")
df_users.createOrReplaceTempView("users")
print(f"✓ tb_users: {df_users.count():,} registros")

# Carregar tb_regions (delimiter: pipe)
df_regions = spark.read.csv(PATHS["regions"], header=True, inferSchema=True, sep="|")
df_regions.createOrReplaceTempView("regions")
print(f"✓ tb_regions: {df_regions.count():,} registros")

# Carregar tb_paymentmodes (delimiter: pipe)
df_paymentmodes = spark.read.csv(PATHS["paymentmodes"], header=True, inferSchema=True, sep="|")
df_paymentmodes.createOrReplaceTempView("paymentmodes")
print(f"✓ tb_paymentmodes: {df_paymentmodes.count():,} registros")

# Carregar tb_cmssitelp (delimiter: pipe)
df_cmssitelp = spark.read.csv(PATHS["cmssitelp"], header=True, inferSchema=True, sep="|")
df_cmssitelp.createOrReplaceTempView("cmssitelp")
print(f"✓ tb_cmssitelp: {df_cmssitelp.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos Schemas

# COMMAND ----------

mostrar_schema(df_carts, "tb_carts")
mostrar_schema(df_cartentries, "tb_cartentries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview dos Dados

# COMMAND ----------

preview_data(df_carts, "tb_carts", 3)
preview_data(df_cartentries, "tb_cartentries", 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza e Preparação de tb_carts

# COMMAND ----------

print("="*80)
print("LIMPEZA DE DADOS: tb_carts")
print("="*80)

# 1. DEDUPLICAÇÃO: Remover PKs duplicados
print("\n1. Deduplicação de tb_carts...")
from pyspark.sql.window import Window

window_dedup = Window.partitionBy("PK").orderBy("createdTS")

df_carts_original = df_carts.count()
df_carts_dedup = df_carts.withColumn(
    "rn",
    row_number().over(window_dedup)
).filter(col("rn") == 1).drop("rn")

df_carts_dedup_count = df_carts_dedup.count()
duplicatas_removidas = df_carts_original - df_carts_dedup_count

print(f"  Carrinhos originais: {df_carts_original:,}")
print(f"  Carrinhos únicos: {df_carts_dedup_count:,}")
print(f"  Duplicatas removidas: {duplicatas_removidas:,}")

# 2. FILTRO: Apenas carrinhos ABANDONADOS
print("\n2. Filtro de carrinhos abandonados...")
print("  Critérios:")
print("    - p_paymentinfo IS NULL (nunca iniciou pagamento)")
print("    - p_totalprice > 0 (tem produtos)")

df_carts_abandonados = df_carts_dedup.filter(
    (col("p_paymentinfo").isNull()) & (col("p_totalprice") > 0)
)

carrinhos_abandonados = df_carts_abandonados.count()
print(f"  Carrinhos abandonados: {carrinhos_abandonados:,}")

# Atualizar df_carts para usar apenas abandonados
df_carts = df_carts_abandonados
df_carts.createOrReplaceTempView("carts")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar DataFrame Principal (Carts Abandonados + Entries)

# COMMAND ----------

print("Criando DataFrame principal...")

# JOIN carrinhos abandonados com itens + adicionar colunas de data
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
).withColumn(
    "ano", year(col("cart_created"))
).withColumn(
    "mes", month(col("cart_created"))
).withColumn(
    "ano_mes", date_format(col("cart_created"), "yyyy-MM")
).withColumn(
    "data", date_format(col("cart_created"), "yyyy-MM-dd")
)

# NOTA: Os valores já estão no formato correto (reais com 2 decimais)
# NÃO é necessário dividir por 100

print(f"✓ df_carts_items criado: {df_carts_items.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remoção de Outliers

# COMMAND ----------

print("="*80)
print("REMOÇÃO DE OUTLIERS")
print("="*80)

# Calcular total por carrinho
df_totais_por_cart = df_carts_items.groupBy("cart_pk").agg(
    spark_round(sum("entry_totalprice"), 2).alias("cart_total")
)

# Identificar outliers (> R$ 50.000)
LIMITE_OUTLIER = 50000

outliers_count = df_totais_por_cart.filter(col("cart_total") > LIMITE_OUTLIER).count()
outliers_valor = df_totais_por_cart.filter(col("cart_total") > LIMITE_OUTLIER).agg(
    spark_round(sum("cart_total"), 2)
).collect()[0][0]

print(f"\nOutliers identificados (carrinhos > R$ {LIMITE_OUTLIER:,.2f}):")
print(f"  Quantidade: {outliers_count:,} carrinhos")
print(f"  Valor total: R$ {outliers_valor:,.2f}")

# Filtrar carrinhos SEM outliers
df_carts_limpo = df_totais_por_cart.filter(col("cart_total") <= LIMITE_OUTLIER)

# JOIN para manter apenas entries de carrinhos limpos
df_carts_items = df_carts_items.join(
    df_carts_limpo.select("cart_pk"),
    "cart_pk",
    "inner"
)

# Criar view temporária
df_carts_items.createOrReplaceTempView("carts_items")

# Cache não é necessário no Serverless - otimização automática
# df_carts_items.cache()

print(f"\n✓ DataFrame final (sem outliers): {df_carts_items.count():,} registros")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas Finais

# COMMAND ----------

# Calcular estatísticas do dataset final
stats = df_carts_items.agg(
    countDistinct("cart_pk").alias("carrinhos_unicos"),
    spark_round(sum("entry_totalprice"), 2).alias("valor_total"),
    sum("quantity").alias("itens_total")
).collect()[0]

carrinhos = stats['carrinhos_unicos']
valor_total = stats['valor_total']
itens_total = stats['itens_total']

ticket_medio = valor_total / carrinhos
itens_por_carrinho = itens_total / carrinhos
preco_por_item = valor_total / itens_total

print("="*80)
print("ESTATÍSTICAS DO DATASET FINAL")
print("="*80)
print(f"\nCarrinhos abandonados: {carrinhos:,}")
print(f"Total de itens: {itens_total:,.0f}")
print(f"Valor total não faturado: R$ {valor_total:,.2f}")
print(f"\nTicket médio: R$ {ticket_medio:,.2f}")
print(f"Itens por carrinho: {itens_por_carrinho:.2f}")
print(f"Preço médio por item: R$ {preco_por_item:,.2f}")
print("="*80)

# Preview dos dados
print("\nPreview do DataFrame:")
df_carts_items.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print(f"""
================================================================================
DADOS CARREGADOS E PROCESSADOS COM SUCESSO
================================================================================

FILTROS APLICADOS:
1. ✓ Deduplicação de tb_carts ({duplicatas_removidas:,} duplicatas removidas)
2. ✓ Apenas carrinhos ABANDONADOS (p_paymentinfo IS NULL + p_totalprice > 0)
3. ✓ Outliers removidos ({outliers_count:,} carrinhos > R$ {LIMITE_OUTLIER:,.2f})

DATASET FINAL:
- Carrinhos abandonados: {carrinhos:,}
- Itens abandonados: {itens_total:,.0f}
- Valor não faturado: R$ {valor_total:,.2f}
- Ticket médio: R$ {ticket_medio:,.2f}

Views temporárias criadas:
- carts (apenas abandonados, sem duplicatas)
- cartentries
- addresses
- paymentinfos
- users
- regions
- paymentmodes
- cmssitelp
- carts_items (JOIN principal com filtros aplicados)

DataFrame principal: df_carts_items

Próximo passo: Execute os notebooks de análise (02 a 09)
================================================================================
""")
