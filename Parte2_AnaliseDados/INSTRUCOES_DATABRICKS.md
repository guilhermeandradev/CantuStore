# Instruções para Rodar no Databricks

## Passo 1: Acessar o Databricks Community Edition

1. Acesse: https://community.cloud.databricks.com/
2. Faça login ou crie uma conta gratuita

## Passo 2: Fazer Upload dos Dados

### Opção A: Via Interface (Recomendado)

1. No menu lateral, clique em **"Data"**
2. Clique em **"Create Table"**
3. Selecione **"Upload File"**
4. Faça upload dos arquivos:
   - `tb_carts/tb_carts.parquet`
   - `tb_cartentries/tb_cartentries.parquet`
   - `tb_addresses/tb_addresses.parquet`
   - `tb_paymentinfos/tb_paymentinfos.parquet`
   - `tb_users.csv`
   - `tb_regions.csv`
   - `tb_paymentmodes.csv`
   - `tb_cmssitelp.csv`

5. Os arquivos serão salvos em `/FileStore/tables/`

### Opção B: Via Notebook (Upload programático)

```python
# Execute este código em uma célula do notebook
import os

# Se você tem os arquivos locais no driver
# dbutils.fs.cp("file:/local/path", "dbfs:/FileStore/cantustore/")
```

## Passo 3: Criar o Notebook

1. No menu lateral, clique em **"Workspace"**
2. Clique com botão direito na sua pasta
3. Selecione **"Create" > "Notebook"**
4. Nome: `Carrinhos_Abandonados`
5. Linguagem: **Python**
6. Cluster: Selecione ou crie um cluster

## Passo 4: Copiar o Código

1. Abra o arquivo `notebook_carrinhos_abandonados.py`
2. Copie cada seção entre `# COMMAND ----------` para uma célula separada
3. Ou importe o arquivo `.py` diretamente:
   - File > Import > Selecione o arquivo

## Passo 5: Ajustar o Caminho dos Dados

Na célula de configuração, ajuste o `BASE_PATH`:

```python
# Se você fez upload para /FileStore/tables/
BASE_PATH = "/FileStore/tables/"

# Ou se criou uma pasta específica
BASE_PATH = "/FileStore/cantustore/"
```

## Passo 6: Executar

1. Inicie um cluster (se não estiver rodando)
2. Execute célula por célula (Shift+Enter)
3. Ou execute tudo: **Run All**

## Estrutura do Notebook

| Seção | Descrição |
|-------|-----------|
| 1 | Setup e Carregamento dos Dados |
| 2.1 | Produtos com mais carrinhos abandonados |
| 2.2 | Duplas de produtos mais abandonadas |
| 2.3 | Produtos com aumento de abandono |
| 2.4 | Produtos novos no primeiro mês |
| 2.5 | Estados com mais abandonos |
| 3.1 | Relatório mensal por produto |
| 3.2 | Relatório diário |
| 4 | Exportação TXT - Top 50 carrinhos |

## Exportar Resultados

### Salvar como CSV:
```python
df_resultado.write.mode("overwrite").csv("/FileStore/resultados/nome_arquivo", header=True)
```

### Baixar do DBFS:
```python
# Listar arquivos
display(dbutils.fs.ls("/FileStore/resultados/"))

# O arquivo estará disponível em:
# https://community.cloud.databricks.com/files/resultados/nome_arquivo
```

## Dicas

1. **Cluster pequeno é suficiente** - Community Edition tem limitações
2. **Cache os DataFrames grandes** - Use `.cache()` após filtros pesados
3. **Use .show(n)** - Para ver apenas N linhas em vez de todos os dados
4. **Salve resultados intermediários** - Evita reprocessamento

## Problemas Comuns

### Erro: "Table not found"
- Verifique se o caminho `BASE_PATH` está correto
- Use `dbutils.fs.ls("/FileStore/")` para listar arquivos

### Erro: "Cluster terminated"
- O cluster do Community Edition desliga após inatividade
- Reinicie o cluster e execute novamente

### Dados muito grandes
- Use `.limit(1000)` para testar com subset
- Depois remova o limite para análise completa
