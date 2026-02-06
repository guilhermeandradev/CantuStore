# Databricks notebook source
# MAGIC %md
# MAGIC # CANTUSTORE - Setup e Configuração
# MAGIC ## Notebook 00 - Configuração Inicial
# MAGIC 
# MAGIC Este notebook configura o ambiente e define variáveis globais para o projeto.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports Globais

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, max, min, 
    year, month, dayofmonth, date_format,
    row_number, dense_rank, lag, lead,
    when, coalesce, concat, lit, concat_ws,
    collect_list, explode, array, struct,
    first, last, round as spark_round,
    to_date, to_timestamp, datediff, add_months
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd

print("✓ Imports realizados com sucesso")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração de Paths

# COMMAND ----------

# Definir caminho base dos dados no Unity Catalog Volume
# Caminho configurado para o volume criado no Databricks
BASE_PATH = "/Volumes/workspace/default/cantustore_data/"

# Paths específicos para cada tabela
PATHS = {
    "carts": f"{BASE_PATH}tb_carts",
    "cartentries": f"{BASE_PATH}tb_cartentries",
    "addresses": f"{BASE_PATH}tb_addresses",
    "paymentinfos": f"{BASE_PATH}tb_paymentinfos",
    "users": f"{BASE_PATH}tb_users.csv",
    "regions": f"{BASE_PATH}tb_regions.csv",
    "paymentmodes": f"{BASE_PATH}tb_paymentmodes.csv",
    "cmssitelp": f"{BASE_PATH}tb_cmssitelp.csv"
}

# Path para resultados
OUTPUT_PATH = f"{BASE_PATH}resultados/"

print(f"✓ Base path configurado: {BASE_PATH}")
print(f"✓ Output path: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções Utilitárias

# COMMAND ----------

def verificar_dados():
    """Verifica se os arquivos de dados existem no DBFS"""
    print("="*60)
    print("VERIFICANDO EXISTÊNCIA DOS ARQUIVOS")
    print("="*60)
    
    try:
        for nome, path in PATHS.items():
            try:
                files = dbutils.fs.ls(path.rsplit('/', 1)[0] if '.' in path else path)
                print(f"✓ {nome}: OK")
            except:
                print(f"✗ {nome}: NÃO ENCONTRADO - {path}")
        print("="*60)
    except Exception as e:
        print(f"Erro ao verificar arquivos: {e}")

def mostrar_schema(df, nome):
    """Mostra o schema de um DataFrame de forma formatada"""
    print("="*60)
    print(f"SCHEMA: {nome}")
    print("="*60)
    df.printSchema()
    print()

def estatisticas_basicas(df, nome):
    """Mostra estatísticas básicas de um DataFrame"""
    print("="*60)
    print(f"ESTATÍSTICAS: {nome}")
    print("="*60)
    print(f"Total de registros: {df.count():,}")
    print(f"Total de colunas: {len(df.columns)}")
    print()

def preview_data(df, nome, n=5):
    """Mostra preview dos dados"""
    print("="*60)
    print(f"PREVIEW: {nome}")
    print("="*60)
    df.show(n, truncate=False)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste de Conexão

# COMMAND ----------

# Verificar se os dados estão acessíveis
verificar_dados()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações do Spark

# COMMAND ----------

# Configurações de otimização
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("✓ Configurações do Spark aplicadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("""
================================================================================
SETUP COMPLETO
================================================================================

Ambiente configurado com sucesso!

Próximos passos:
1. Execute o notebook 01_carregamento_dados.py
2. Execute os notebooks de análise (02 a 06)
3. Execute os notebooks de relatórios (07 a 08)
4. Execute o notebook de exportação (09)

================================================================================
""")
