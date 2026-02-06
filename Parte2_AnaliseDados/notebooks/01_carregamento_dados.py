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
# MAGIC ## Criar DataFrame Principal (Carts + Entries)

# COMMAND ----------

# JOIN carrinhos com itens + adicionar colunas de data
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

# IMPORTANTE: Converter preços de centavos para reais (dividir por 100)
df_carts_items = df_carts_items.withColumn(
    "cart_totalprice",
    col("cart_totalprice") / 100
).withColumn(
    "entry_totalprice",
    col("entry_totalprice") / 100
)

# Criar view temporária
df_carts_items.createOrReplaceTempView("carts_items")

# Cache não é necessário no Serverless - otimização automática
# df_carts_items.cache()

print(f"✓ df_carts_items criado: {df_carts_items.count():,} registros")
df_carts_items.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("""
================================================================================
DADOS CARREGADOS COM SUCESSO
================================================================================

Views temporárias criadas:
- carts
- cartentries
- addresses
- paymentinfos
- users
- regions
- paymentmodes
- cmssitelp
- carts_items (JOIN principal com colunas de data)

DataFrame principal em cache: df_carts_items

Próximo passo: Execute os notebooks de análise (02 a 06)
================================================================================
""")
