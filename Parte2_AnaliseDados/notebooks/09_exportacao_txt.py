# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Exportação TXT
# MAGIC ## Notebook 09 - Exportar Top 50 Carrinhos no Formato Especificado

# COMMAND ----------

# MAGIC %run ./01_carregamento_dados

# COMMAND ----------

# MAGIC %md
# MAGIC ## Formato do Arquivo TXT
# MAGIC 
# MAGIC ```
# MAGIC carts.PK|carts.createdTS|carts.p_totalprice|user.p_uid|paymentmodes.p_code|paymentinfos.p_installments|cmssitelp.p_name|addresses.p_postalcode|sum(cartentries.p_quantity)|count(cartentries.PK)
# MAGIC cartentries.p_product|cartentries.p_quantity|cartentries.p_totalprice|
# MAGIC cartentries.p_product|cartentries.p_quantity|cartentries.p_totalprice|
# MAGIC ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecionar Top 50 Carrinhos

# COMMAND ----------

# Top 50 carrinhos por p_totalprice
df_top50_carts = df_carts.withColumn(
    "p_totalprice_reais", 
    col("p_totalprice")
).orderBy(col("p_totalprice").desc()).limit(50)

print(f"✓ Top 50 carrinhos selecionados")
print("="*80)
df_top50_carts.select("PK", "createdTS", "p_totalprice_reais").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOIN com Todas as Tabelas Auxiliares

# COMMAND ----------

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
    (col("c.p_totalprice_reais")).alias("cart_totalprice"),  # Usar preço já convertido
    coalesce(col("u.p_uid"), lit("")).alias("user_uid"),
    coalesce(col("pm.p_code"), lit("")).alias("paymentmode_code"),
    coalesce(col("pi.p_installments"), lit(0)).alias("paymentinfo_installments"),
    coalesce(col("cs.p_name"), lit("")).alias("site_name"),
    coalesce(col("a.p_postalcode"), lit("")).alias("address_postalcode")
)

print("✓ JOINs realizados")
df_export.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregar Entries por Carrinho

# COMMAND ----------

# Agregar entries: soma de quantidade e contagem
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

print("✓ Entries agregados")

# COMMAND ----------

# JOIN final
df_final_export = df_export.join(
    df_entries_agg,
    df_export.cart_pk == df_entries_agg.p_order,
    "left"
).select(
    "cart_pk",
    "cart_createdTS",
    "cart_totalprice",
    "user_uid",
    "paymentmode_code",
    "paymentinfo_installments",
    "site_name",
    "address_postalcode",
    coalesce("sum_quantity", lit(0)).alias("sum_quantity"),
    coalesce("count_entries", lit(0)).alias("count_entries"),
    "entries"
)

# Ordenar por valor (maior primeiro)
df_final_export = df_final_export.orderBy(col("cart_totalprice").desc())

# Cache não é necessário no Serverless - otimização automática
# df_final_export.cache()
print(f"✓ DataFrame final preparado: {df_final_export.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerar Arquivo TXT

# COMMAND ----------

# Coletar dados (já está limitado a 50)
dados = df_final_export.collect()

print(f"✓ Dados coletados: {len(dados)} carrinhos")

# COMMAND ----------

# Gerar conteúdo do TXT no formato especificado
linhas_txt = []

for row in dados:
    # Linha do header do carrinho
    # Formato: PK|createdTS|totalprice|uid|paymentmode|installments|site|postalcode|sum_quantity|count_entries
    header = "|".join([
        str(row['cart_pk']),
        str(row['cart_createdTS']),
        str(row['cart_totalprice']),
        str(row['user_uid']) if row['user_uid'] else "",
        str(row['paymentmode_code']) if row['paymentmode_code'] else "",
        str(row['paymentinfo_installments']) if row['paymentinfo_installments'] else "",
        str(row['site_name']) if row['site_name'] else "",
        str(row['address_postalcode']) if row['address_postalcode'] else "",
        str(row['sum_quantity']),
        str(row['count_entries'])
    ])
    linhas_txt.append(header)
    
    # Linhas dos entries
    if row['entries']:
        for entry in row['entries']:
            # Formato: product|quantity|totalprice|
            entry_line = "|".join([
                str(entry['p_product']),
                str(entry['p_quantity']),
                str(entry['p_totalprice']),
                ""  # Pipe final
            ])
            linhas_txt.append(entry_line)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview do Arquivo

# COMMAND ----------

print("="*80)
print("PREVIEW DO ARQUIVO TXT (primeiras 50 linhas)")
print("="*80)
for i, linha in enumerate(linhas_txt[:50]):
    print(f"{i+1:3d}: {linha}")
print("="*80)
print(f"Total de linhas no arquivo: {len(linhas_txt)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Arquivo TXT

# COMMAND ----------

# Juntar todas as linhas
txt_content = "\n".join(linhas_txt)

# Salvar no DBFS
output_path_txt = f"{OUTPUT_PATH}top50_carrinhos.txt"

# Usar dbutils para salvar
dbutils.fs.put(output_path_txt, txt_content, overwrite=True)

print(f"✓ Arquivo TXT salvo em: {output_path_txt}")
print(f"✓ Total de linhas: {len(linhas_txt)}")
print(f"✓ Total de carrinhos: {len(dados)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar Arquivo Salvo

# COMMAND ----------

# Verificar se o arquivo foi criado
try:
    file_info = dbutils.fs.ls(OUTPUT_PATH)
    txt_files = [f for f in file_info if f.name.endswith('.txt')]
    
    print("="*80)
    print("ARQUIVOS TXT NO DIRETÓRIO DE OUTPUT")
    print("="*80)
    for f in txt_files:
        print(f"Nome: {f.name}")
        print(f"Path: {f.path}")
        print(f"Tamanho: {f.size:,} bytes")
        print("-"*80)
except Exception as e:
    print(f"Erro ao listar arquivos: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas do Arquivo

# COMMAND ----------

# Calcular estatísticas
total_entries = sum([row['count_entries'] for row in dados])
total_quantity = sum([row['sum_quantity'] for row in dados])
total_value = sum([row['cart_totalprice'] for row in dados if row['cart_totalprice']])

print("="*80)
print("ESTATÍSTICAS DO ARQUIVO TXT")
print("="*80)
print(f"Total de carrinhos: {len(dados)}")
print(f"Total de entries: {total_entries:,}")
print(f"Total de itens (quantidade): {total_quantity:,}")
print(f"Valor total: R$ {total_value:,.2f}")
print(f"Ticket médio: R$ {total_value/len(dados):,.2f}")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download do Arquivo

# COMMAND ----------

# Para baixar o arquivo no Databricks Community Edition
# Use o seguinte link (substitua o path):
# https://community.cloud.databricks.com/files/cantustore/resultados/top50_carrinhos.txt

print("""
================================================================================
PARA BAIXAR O ARQUIVO
================================================================================

1. Via UI do Databricks:
   - Navegue para: Catalog > workspace > default > cantustore_data > resultados
   - Clique em "top50_carrinhos.txt"
   - Clique em "Download"

2. Via dbutils:
   Executar em outra célula:
   display(dbutils.fs.head("OUTPUT_PATH/top50_carrinhos.txt", 1000))

3. Via URL (Community Edition):
   https://community.cloud.databricks.com/files/cantustore/resultados/top50_carrinhos.txt

================================================================================
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print(f"""
================================================================================
EXPORTAÇÃO TXT CONCLUÍDA
================================================================================

Arquivo gerado: top50_carrinhos.txt
Localização: {output_path_txt}

Formato:
- Linha 1 (por carrinho): Header com informações do carrinho
- Linhas seguintes: Entries do carrinho (produto|quantidade|valor|)

Conteúdo:
- {len(dados)} carrinhos (top 50 por valor)
- {total_entries:,} entries (itens)
- {len(linhas_txt):,} linhas no arquivo

Valor total dos carrinhos: R$ {total_value:,.2f}

================================================================================
TODOS OS NOTEBOOKS EXECUTADOS COM SUCESSO!
================================================================================
""")
