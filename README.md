# CANTUSTORE - Análise de Carrinhos Abandonados

Projeto completo de análise de dados utilizando **Databricks** e **PySpark** para identificar padrões de carrinhos abandonados em e-commerce.

---

## 📋 Estrutura do Projeto

```
CantuStore/
├── Parte1_SQL/                      # Questões SQL
│   ├── 1.1_campeonato.sql           # Classificação de campeonato
│   ├── 1.2_comissoes.sql            # Análise de comissões
│   └── 1.3_colaboradores.sql        # Hierarquia de colaboradores
│
├── Parte2_AnaliseDados/             # Análise de Dados com PySpark
│   ├── data/                        # Dados (não versionados - upload separado)
│   │   ├── tb_carts/                # Diretório Parquet
│   │   ├── tb_cartentries/          # Diretório Parquet
│   │   ├── tb_addresses/            # Diretório Parquet
│   │   ├── tb_paymentinfos/         # Diretório Parquet
│   │   ├── tb_users.csv
│   │   ├── tb_regions.csv
│   │   ├── tb_paymentmodes.csv
│   │   └── tb_cmssitelp.csv
│   │
│   ├── notebooks/                   # Notebooks PySpark (ordem de execução)
│   │   ├── 00_setup.py              # Configuração inicial
│   │   ├── 01_carregamento_dados.py # Carregamento + filtros de abandono
│   │   ├── 02_analise_produtos.py   # Produtos mais abandonados
│   │   ├── 03_analise_duplas.py     # Duplas/trios de produtos
│   │   ├── 04_analise_tendencia.py  # Tendência temporal de abandono
│   │   ├── 05_analise_produtos_novos.py  # Performance de produtos novos
│   │   ├── 06_analise_estados.py    # Análise geográfica (estados/regiões)
│   │   ├── 07_relatorio_produto_mes.py   # Relatório mensal detalhado
│   │   ├── 08_relatorio_data.py     # Relatório diário + tendências
│   │   └── 09_exportacao_txt.py     # Exportação formato TXT
│   │
│   ├── FILTROS_CARRINHOS_ABANDONADOS.md  # Documentação dos filtros
│   ├── RESUMO_NOTEBOOKS.md          # Resumo de cada notebook
│   └── GUIA_DATABRICKS_GITHUB.md    # Guia completo Databricks
│
├── .gitignore                       # Arquivos ignorados
└── README.md                        # Este arquivo
```

---

## 🎯 Objetivos do Projeto

### Parte 1: SQL (3 Questões)

1. ✅ **Campeonato**: Calcular classificação de times por pontuação (vitória, empate, derrota)
2. ✅ **Comissões**: Identificar vendedores que receberam >= R$ 1.024 em até 3 transferências
3. ✅ **Colaboradores**: Encontrar chefe indireto mais baixo na hierarquia que ganha >= 2x o salário do funcionário

### Parte 2: Análise de Dados (PySpark + Databricks)

**5 Análises Exploratórias:**
1. ✅ Produtos com mais carrinhos abandonados
2. ✅ Duplas/trios de produtos frequentemente abandonados juntos
3. ✅ Produtos com aumento de abandono ao longo do tempo
4. ✅ Produtos novos e sua performance no primeiro mês
5. ✅ Estados com maior concentração de abandonos

**2 Relatórios:**
1. ✅ Relatório mensal por produto (carrinhos, itens, valor não faturado)
2. ✅ Relatório diário (carrinhos, itens, valor não faturado + tendências)

**1 Exportação:**
1. ✅ Arquivo TXT com top 50 carrinhos no formato especificado

---

## 🚀 Como Executar Este Projeto (Guia para Avaliação)

### 📌 Pré-requisitos

- Conta no [Databricks Community Edition](https://community.cloud.databricks.com/) (gratuito)
- Acesso aos arquivos de dados (fornecidos separadamente)

---

### **PASSO 1: Configurar Databricks e Clonar Repositório**

#### 1.1 - Acessar Databricks
1. Acesse: https://community.cloud.databricks.com/
2. Faça login ou crie uma conta gratuita

#### 1.2 - Criar Cluster
1. No menu lateral, clique em **Compute**
2. Clique em **Create Compute**
3. Configure:
   - **Cluster name**: `CantuStore-Cluster` (ou nome de sua preferência)
   - **Cluster mode**: Single Node (padrão Community Edition)
   - **Databricks runtime version**: **13.3 LTS** ou superior
   - **Node type**: Deixar padrão (Community Edition tem apenas uma opção)
4. Clique em **Create Compute**
5. Aguarde o cluster ficar com status **"Running"** (ícone verde)

#### 1.3 - Clonar Repositório do GitHub
1. No menu lateral, clique em **Workspace**
2. Clique em **Repos** (ou **Workspace** → **Repos**)
3. Clique em **Add Repo** e clique em "Criar Pasta Git"
4. Preencha:
   - **Git repository URL**: `https://github.com/guilhermeandradev/CantuStore`
   - **Git provider**: GitHub
   - **Repository name**: `CantuStore` (ou deixar auto-preencher)
5. Clique em **Create Repo**
6. Aguarde a clonagem (alguns segundos)

> ✅ **Resultado**: Você verá a estrutura completa do projeto em `Workspace → Repos → CantuStore`

---

### **PASSO 2: Upload dos Dados**

Os arquivos de dados **não estão no GitHub** (`.gitignore`). Você precisa fazer upload manual.

#### 2.1 - Criar Volume para os Dados
1. No menu lateral, clique em **Catalog**
2. Navegue até: **workspace** (ou **main**) → **default**
3. Clique nos 3 pontinhos (...) ao lado de **default** → **Create** → **Volume**
4. Preencha:
   - **Name**: `cantustore_data`
   - **Schema**: `default`
   - **Catalog**: `workspace` (ou o que estiver selecionado)
5. Clique em **Create**

> 📍 **Caminho criado**: `/Volumes/workspace/default/cantustore_data/`

#### 2.2 - Fazer Upload dos Arquivos
1. Clique no Volume **cantustore_data** que você acabou de criar
2. Clique em **Upload Files** (botão no canto superior direito)
3. Faça upload dos seguintes arquivos (fornecidos separadamente):

**Diretórios Parquet** (fazer upload de cada diretório):
- `tb_carts/` (contém arquivos .parquet)
- `tb_cartentries/` (contém arquivos .parquet)
- `tb_addresses/` (contém arquivos .parquet)
- `tb_paymentinfos/` (contém arquivos .parquet)

**Arquivos CSV** (fazer upload individual):
- `tb_users.csv`
- `tb_regions.csv`
- `tb_paymentmodes.csv`
- `tb_cmssitelp.csv`

> 💡 **Dica**: No Databricks, você pode arrastar e soltar os arquivos diretamente na interface de upload.

#### 2.3 - Verificar Upload
Após o upload, você deve ver no Volume:
```
cantustore_data/
├── tb_carts/
├── tb_cartentries/
├── tb_addresses/
├── tb_paymentinfos/
├── tb_users.csv
├── tb_regions.csv
├── tb_paymentmodes.csv
└── tb_cmssitelp.csv
```

---

### **PASSO 3: Ajustar Configuração (Se Necessário)**

#### 3.1 - Verificar Caminho dos Dados
1. Navegue até: **Workspace** → **Repos** → **CantuStore** → **Parte2_AnaliseDados** → **notebooks**
2. Abra o notebook **`00_setup.py`**
3. Localize a linha 23 (aproximadamente):
   ```python
   BASE_PATH = "/Volumes/workspace/default/cantustore_data/"
   ```
4. **Se você usou outro Catalog ou Schema**, ajuste o caminho:
   - Exemplo: `/Volumes/main/default/cantustore_data/`
   - Exemplo: `/Volumes/workspace/seu_schema/cantustore_data/`

5. **Se o caminho estiver correto**, não é necessário alterar nada

> ⚠️ **Importante**: Certifique-se de que o caminho termina com `/`

---

### **PASSO 4: Executar os Notebooks (ORDEM OBRIGATÓRIA)**

#### 4.1 - Anexar Cluster aos Notebooks
Antes de executar, certifique-se de que o cluster está anexado:
1. Abra qualquer notebook
2. No topo do notebook, você verá **"Detached"** ou o nome de um cluster
3. Se estiver **"Detached"**, clique e selecione **CantuStore-Cluster**
4. Aguarde a conexão (alguns segundos)

#### 4.2 - Executar na Ordem
Execute os notebooks **UM POR VEZ**, na ordem abaixo:

| Ordem | Notebook | Descrição | Comando |
|-------|----------|-----------|---------|
| 1 | `00_setup.py` | Configuração inicial (imports, paths, funções) | **Run All** |
| 2 | `01_carregamento_dados.py` | Carrega dados + aplica filtros de abandono | **Run All** |
| 3 | `02_analise_produtos.py` | Top produtos mais abandonados | **Run All** |
| 4 | `03_analise_duplas.py` | Duplas/trios de produtos abandonados | **Run All** |
| 5 | `04_analise_tendencia.py` | Tendência temporal de abandono | **Run All** |
| 6 | `05_analise_produtos_novos.py` | Performance de produtos novos | **Run All** |
| 7 | `06_analise_estados.py` | Análise geográfica (estados) | **Run All** |
| 8 | `07_relatorio_produto_mes.py` | Relatório mensal por produto | **Run All** |
| 9 | `08_relatorio_data.py` | Relatório diário + tendências | **Run All** |
| 10 | `09_exportacao_txt.py` | Exportação formato TXT (top 50) | **Run All** |

#### 4.3 - Como Executar "Run All"
1. Abra o notebook
2. No menu superior, clique em **Run All** (ou pressione `Ctrl + Shift + Enter`)
3. Aguarde a execução completa (você verá os resultados aparecerem)
4. Passe para o próximo notebook

> ⏱️ **Tempo estimado**: 
> - Notebooks 00-01: ~2-3 minutos cada
> - Notebooks 02-09: ~1-2 minutos cada
> - **Total**: ~15-20 minutos para executar todos

---

### **PASSO 5: Validar Resultados**

#### 5.1 - Verificar Carregamento de Dados (Notebook 01)
Após executar `01_carregamento_dados.py`, role até o final. Você deve ver:
```
================================================================================
ESTATÍSTICAS DO DATASET FINAL
================================================================================

Carrinhos abandonados: [número]
Total de itens: [número]
Valor total não faturado: R$ [valor]

Ticket médio: R$ [valor]
Itens por carrinho: [número]
Preço médio por item: R$ [valor]
```

✅ **Se você vê esta mensagem**: Dados carregados e filtrados corretamente!

#### 5.2 - Verificar Análises (Notebooks 02-09)
Cada notebook gera:
- Tabelas e gráficos com os resultados
- Estatísticas e insights
- Arquivos CSV salvos no caminho de output

#### 5.3 - Localizar Arquivos Gerados
Os resultados são salvos em:
```
/Volumes/workspace/default/cantustore_data/resultados/
```

Para visualizar:
1. **Catalog** → **workspace** → **default** → **cantustore_data** → **resultados**
2. Ou navegue pelo código dos notebooks para ver os outputs inline

---

## 🔍 Filtros de Dados Aplicados

O projeto aplica **filtros automáticos** no notebook 01 para garantir análise precisa:

### **1. Deduplicação**
- Remove **11.134 PKs duplicados** em `tb_carts`
- Mantém apenas o primeiro registro de cada carrinho

### **2. Filtro de Abandono**
- **p_paymentinfo IS NULL**: Carrinho nunca iniciou pagamento
- **p_totalprice > 0**: Carrinho tem produtos adicionados
- **Resultado**: Apenas carrinhos REALMENTE abandonados

### **3. Remoção de Outliers**
- Remove carrinhos com valor total > R$ 50.000
- Elimina **4.267 outliers** (carrinhos de teste/erro)

### **📊 Dataset Final (Após Filtros)**

```
Período: 2019-12-16 a 2022-07-26 (2,61 anos / 953 dias)

Carrinhos abandonados: 905.180
Total de itens abandonados: 2.769.758
Valor total não faturado: R$ 6.267.369.294,36

Ticket médio: R$ 6.923,89
Itens por carrinho: 3,06 pneus
Preço médio por item: R$ 2.262,79

Abandonos por dia: 950 carrinhos
Valor não faturado por dia: R$ 6.576.463,06

✅ Todos os valores validados para e-commerce de pneus premium
```

> 📖 **Documentação completa dos filtros**: [FILTROS_CARRINHOS_ABANDONADOS.md](Parte2_AnaliseDados/FILTROS_CARRINHOS_ABANDONADOS.md)

---

## 🛠️ Tecnologias Utilizadas

- **Databricks**: Plataforma de análise de dados em nuvem
- **PySpark**: Processamento distribuído de grandes volumes de dados
- **SQL**: Queries e análises relacionais
- **Python**: Lógica de negócio e transformações
- **GitHub**: Versionamento e colaboração

---

## 📊 Estrutura das Análises

### **Análises Exploratórias (Notebooks 02-06)**
| Análise | Objetivo | Output |
|---------|----------|--------|
| **Produtos** | Identificar produtos com mais abandonos | Top 50 produtos + estatísticas |
| **Duplas** | Produtos frequentemente abandonados juntos | Top duplas e trios |
| **Tendência** | Crescimento/queda de abandono ao longo do tempo | Produtos com mudança de padrão |
| **Produtos Novos** | Performance de lançamentos | Abandono no primeiro mês |
| **Estados** | Concentração geográfica | Ranking por estado + região |

### **Relatórios (Notebooks 07-08)**
| Relatório | Granularidade | Colunas |
|-----------|---------------|---------|
| **Mensal** | Produto + Mês | qtd_carrinhos, qtd_itens, valor_nao_faturado |
| **Diário** | Data | qtd_carrinhos, qtd_itens, valor_nao_faturado |

### **Exportação (Notebook 09)**
- Formato: TXT
- Conteúdo: Top 50 carrinhos por valor
- Layout: Especificado conforme requisitos

---

## 📈 Insights de Negócio Esperados

### **Principais Descobertas Possíveis:**
1. **Produtos Críticos**: Identificação de produtos com alto volume de abandono
2. **Complementaridade**: Duplas/trios abandonados juntos → oportunidades de bundle
3. **Padrões Temporais**: Sazonalidade, dia da semana, período do mês
4. **Distribuição Geográfica**: Estados/regiões com maior abandono
5. **Performance de Novos Produtos**: Taxa de abandono em lançamentos

### **Recomendações de Negócio:**
- Implementar remarketing para produtos com alto abandono
- Criar ofertas de bundle para produtos abandonados juntos
- Investigar causas de aumento de abandono em produtos específicos
- Otimizar checkout nos estados com mais abandonos
- Ajustar estratégias de frete/pagamento por região

---

## 📚 Documentação Adicional

- **[FILTROS_CARRINHOS_ABANDONADOS.md](Parte2_AnaliseDados/FILTROS_CARRINHOS_ABANDONADOS.md)**: Explicação detalhada dos filtros e validações
- **[RESUMO_NOTEBOOKS.md](Parte2_AnaliseDados/RESUMO_NOTEBOOKS.md)**: Resumo do objetivo de cada notebook
- **[GUIA_DATABRICKS_GITHUB.md](Parte2_AnaliseDados/GUIA_DATABRICKS_GITHUB.md)**: Guia completo de integração Databricks + GitHub

---

## ❓ Troubleshooting

### **Problema: "Path does not exist"**
**Causa**: Caminho dos dados incorreto  
**Solução**: Verificar `BASE_PATH` no `00_setup.py` (linha 23) e ajustar conforme o caminho do seu Volume

### **Problema: "Unable to attach cluster"**
**Causa**: Cluster não está rodando  
**Solução**: 
1. Vá em **Compute**
2. Verifique se o cluster está **"Running"**
3. Se não, clique em **Start**

### **Problema: "countDistinct not defined"**
**Causa**: Notebook 00 não foi executado  
**Solução**: Execute `00_setup.py` primeiro (imports e configurações)

### **Problema: "DataFrame not found"**
**Causa**: Notebook 01 não foi executado  
**Solução**: Execute `01_carregamento_dados.py` antes dos outros

### **Problema: Valores parecem incorretos**
**Causa**: Executou notebooks fora de ordem ou não executou 01  
**Solução**: 
1. **Clear State & Outputs** em todos os notebooks
2. **Restart Cluster**
3. Re-executar na ordem (00 → 01 → 02... → 09)

---

## 🔗 Links Úteis

- [Databricks Community Edition](https://community.cloud.databricks.com/) - Plataforma gratuita
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/) - Documentação oficial PySpark
- [Databricks Documentation](https://docs.databricks.com/) - Guias e tutoriais Databricks

---

## 📄 Relatório Técnico

Para uma visão executiva completa do projeto, consulte:

📋 **[RELATORIO_TECNICO_CANTUSTORE.md](RELATORIO_TECNICO_CANTUSTORE.md)**

O relatório contém:
- Explicação detalhada de todas as soluções SQL
- Arquitetura completa da análise de dados
- Insights e recomendações de negócio
- Métricas de qualidade e desafios superados

---

**CantuStore - Plataforma de tecnologia e logística em pneus**  
*Se o assunto é pneu, você resolve aqui.*
