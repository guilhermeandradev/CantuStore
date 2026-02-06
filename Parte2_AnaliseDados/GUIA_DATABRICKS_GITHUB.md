# Guia Completo: Databricks + GitHub

Este guia detalha o processo completo de integração entre GitHub e Databricks para o projeto de análise de carrinhos abandonados.

---

## Fase 1: Preparar Repositório GitHub

### 1.1 Criar Repositório

1. Acesse: https://github.com
2. Clique em **"New repository"**
3. Configure:
   - **Name:** `cantustore-analise-carrinhos`
   - **Description:** "Análise de carrinhos abandonados com PySpark e Databricks"
   - **Visibility:** Private ou Public
   - ✅ Initialize with README
4. Clique em **"Create repository"**

### 1.2 Clonar Repositório Localmente

```bash
# Clone o repositório
git clone https://github.com/SEU_USUARIO/CantuStore.git
cd cantustore-analise-carrinhos

# Copie os arquivos do projeto
# (copie a estrutura de pastas CantuStore para dentro do repositório)
```

### 1.3 Fazer Primeiro Commit

```bash
# Adicionar arquivos
git add .

# Commit
git commit -m "Initial commit: Estrutura do projeto e notebooks PySpark"

# Push para o GitHub
git push origin main
```

---

## Fase 2: Configurar Databricks

### 2.1 Criar Conta no Databricks Community Edition

1. Acesse: https://community.cloud.databricks.com/
2. Clique em **"Sign up"**
3. Preencha:
   - Email
   - Nome
   - Senha
4. Confirme o email
5. Faça login

### 2.2 Criar Cluster

1. No menu lateral, clique em **"Compute"**
2. Clique em **"Create Cluster"**
3. Configure:
   - **Cluster name:** `CantuStore-Cluster`
   - **Runtime:** Selecione a versão mais recente (ex: 13.3 LTS)
   - **Node type:** Padrão (Community Edition tem opção limitada)
   - **Terminate after:** 120 minutes (padrão)
4. Clique em **"Create Cluster"**
5. Aguarde o cluster iniciar (status verde)

---

## Fase 3: Conectar GitHub ao Databricks

### 3.1 Gerar Personal Access Token no GitHub

1. No GitHub, vá para: **Settings** (seu perfil)
2. No menu lateral esquerdo, role até **"Developer settings"**
3. Clique em **"Personal access tokens"** > **"Tokens (classic)"**
4. Clique em **"Generate new token"** > **"Generate new token (classic)"**
5. Configure:
   - **Note:** `Databricks CantuStore`
   - **Expiration:** 90 days (ou custom)
   - **Scopes:** 
     - ✅ `repo` (full control of private repositories)
6. Clique em **"Generate token"**
7. **IMPORTANTE:** Copie o token (só aparece uma vez!)
   - Formato: `ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

### 3.2 Configurar Repos no Databricks

1. No Databricks, clique em **"Workspace"** no menu lateral
2. Clique na seta ao lado do seu nome de usuário
3. Selecione **"User Settings"**
4. Vá para a aba **"Git Integration"**
5. Em **"Git provider"**, selecione **"GitHub"**
6. Clique em **"Save"**

### 3.3 Clonar Repositório no Databricks

1. No menu lateral, clique em **"Workspace"**
2. Clique na seta ao lado do seu usuário
3. Selecione **"Create" > "Repo"**
4. Configure:
   - **Git provider:** GitHub
   - **Git repository URL:** `https://github.com/SEU_USUARIO/cantustore-analise-carrinhos.git`
   - **Git credential:** Cole o Personal Access Token
   - **Branch:** `main` (ou `master`)
5. Clique em **"Create Repo"**

Aguarde a clonagem. Seus notebooks agora aparecem em:
```
Workspace > Repos > seu_usuario > cantustore-analise-carrinhos
```

---

## Fase 4: Upload dos Dados para DBFS

### 4.1 Opção A - Upload via UI (Recomendado)

1. No menu lateral, clique em **"Data"**
2. Clique em **"Create Table"**
3. Selecione **"Upload File"**
4. **Arraste ou selecione** os arquivos:

**Arquivos Parquet (pastas completas):**
- `tb_carts/tb_carts.parquet`
- `tb_cartentries/tb_cartentries.parquet`
- `tb_addresses/tb_addresses.parquet`
- `tb_paymentinfos/tb_paymentinfos.parquet`

**Arquivos CSV:**
- `tb_users.csv`
- `tb_regions.csv`
- `tb_paymentmodes.csv`
- `tb_cmssitelp.csv`

5. Os arquivos serão salvos em: `/FileStore/tables/`

**IMPORTANTE:** Anote os caminhos exatos onde os arquivos foram salvos!

### 4.2 Opção B - Upload via Notebook

Crie um notebook temporário com o código:

```python
# Databricks notebook source

# Listar arquivos locais (se disponíveis no driver)
import os

# Fazer upload programático
# Este método só funciona se você tem acesso ao driver node

# Exemplo de cópia
dbutils.fs.cp("file:/Workspace/Repos/seu_usuario/cantustore-analise-carrinhos/Parte2_AnaliseDados/data/tb_users.csv", 
              "dbfs:/FileStore/cantustore/tb_users.csv")

# Repetir para cada arquivo
```

### 4.3 Verificar Upload

```python
# Em um notebook, execute:
dbutils.fs.ls("/FileStore/tables/")
# ou
dbutils.fs.ls("/FileStore/cantustore/")
```

### 4.4 Organizar Estrutura de Dados

Estrutura ideal no DBFS:
```
/FileStore/cantustore/
├── tb_carts/
│   └── tb_carts.parquet
├── tb_cartentries/
│   └── tb_cartentries.parquet
├── tb_addresses/
│   └── tb_addresses.parquet
├── tb_paymentinfos/
│   └── tb_paymentinfos.parquet
├── tb_users.csv
├── tb_regions.csv
├── tb_paymentmodes.csv
└── tb_cmssitelp.csv
```

---

## Fase 5: Executar os Notebooks

### 5.1 Ajustar Paths

No notebook **00_setup.py**, ajuste o `BASE_PATH`:

```python
# Se você fez upload para /FileStore/tables/
BASE_PATH = "/FileStore/tables/"

# Ou se criou uma pasta específica
BASE_PATH = "/FileStore/cantustore/"
```

### 5.2 Ordem de Execução

Execute os notebooks nesta ordem:

1. **00_setup.py** - Verificar configuração
2. **01_carregamento_dados.py** - Carregar dados
3. **02_analise_produtos.py**
4. **03_analise_duplas.py**
5. **04_analise_tendencia.py**
6. **05_analise_produtos_novos.py**
7. **06_analise_estados.py**
8. **07_relatorio_produto_mes.py**
9. **08_relatorio_data.py**
10. **09_exportacao_txt.py**

### 5.3 Dicas de Execução

- Execute célula por célula: **Shift + Enter**
- Execute tudo: **Run All** (menu superior)
- Sempre verifique se o cluster está rodando (verde)
- Se houver erro, leia a mensagem e verifique paths

---

## Fase 6: Sincronizar Mudanças

### 6.1 Fazer Commit de Mudanças pelo Databricks

1. Navegue até **Workspace > Repos > seu repo**
2. Clique no nome do repositório
3. Você verá opções de Git:
   - **Branch:** Ver/mudar branch
   - **Pull:** Trazer mudanças do GitHub
   - **Commit & Push:** Enviar mudanças

4. Para fazer commit:
   - Edite arquivos no Databricks
   - Clique em **"Git..."** (botão superior)
   - Selecione **"Commit & Push"**
   - Adicione mensagem de commit
   - Clique em **"Commit"**

### 6.2 Atualizar do GitHub

Se fizer mudanças localmente e subir para GitHub:

1. No Databricks, vá ao seu repo
2. Clique em **"Git..."**
3. Selecione **"Pull"**
4. Confirme

---

## Fase 7: Baixar Resultados

### 7.1 Baixar Arquivos do DBFS

**Opção A - Via UI:**
1. Data > DBFS > FileStore > cantustore > resultados
2. Clique no arquivo
3. Clique em **"Download"**

**Opção B - Via Notebook:**
```python
# Visualizar conteúdo
display(dbutils.fs.head("/FileStore/cantustore/resultados/top50_carrinhos.txt", 1000))

# Listar arquivos
display(dbutils.fs.ls("/FileStore/cantustore/resultados/"))
```

**Opção C - Via URL (Community Edition):**
```
https://community.cloud.databricks.com/files/cantustore/resultados/top50_carrinhos.txt
```

---

## Problemas Comuns e Soluções

### Erro: "Table not found"

**Causa:** Path dos dados incorreto

**Solução:**
```python
# Verificar onde os dados estão
dbutils.fs.ls("/FileStore/")
dbutils.fs.ls("/FileStore/tables/")

# Ajustar BASE_PATH no notebook 00_setup.py
```

### Erro: "Cluster terminated"

**Causa:** Cluster do Community Edition desliga após inatividade

**Solução:**
- Reinicie o cluster: Compute > Seu cluster > Start
- Execute novamente os notebooks

### Erro: "Authentication failed"

**Causa:** Token do GitHub expirou ou incorreto

**Solução:**
- Gere novo Personal Access Token no GitHub
- Atualize em: User Settings > Git Integration

### Erro: "Out of memory"

**Causa:** Dados muito grandes para Community Edition

**Solução:**
```python
# Use sampling para testes
df_sample = df_carts_items.sample(0.1)  # 10% dos dados
```

---

## Dicas de Produtividade

1. **Use Cache:** Para DataFrames grandes
   ```python
   df.cache()
   ```

2. **Limite Dados em Testes:**
   ```python
   df.limit(1000).show()
   ```

3. **Monitore Performance:**
   - Veja detalhes de execução clicando na seta ao lado de cada célula
   - Spark UI: Cluster > Spark UI

4. **Atalhos Úteis:**
   - `Shift + Enter`: Executar célula
   - `Esc + B`: Nova célula abaixo
   - `Esc + A`: Nova célula acima
   - `Esc + D, D`: Deletar célula

---

## Próximos Passos

1. ✅ Repositório GitHub configurado
2. ✅ Databricks conectado ao GitHub
3. ✅ Dados carregados no DBFS
4. ✅ Notebooks executados
5. ✅ Resultados gerados e baixados

**Projeto completo e funcionando!** 🎉

---

## Recursos Adicionais

- [Databricks Community Edition Guide](https://docs.databricks.com/getting-started/community-edition.html)
- [PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/)
- [GitHub Personal Access Tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
