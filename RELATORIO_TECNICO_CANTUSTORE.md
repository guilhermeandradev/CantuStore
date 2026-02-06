# Relatório Técnico - Desafio CantuStore
## Análise de Carrinhos Abandonados

---

**Elaborado por:** Engenharia de Dados  
**Data:** 06 de Fevereiro de 2026  
**Versão:** 1.0

---

## 📋 Sumário Executivo

Este relatório apresenta as respostas completas para o desafio técnico proposto pela CantuStore, dividido em duas partes:

- **Parte 1:** 3 questões SQL (classificação, agregações, hierarquia)
- **Parte 2:** Análise de carrinhos abandonados com PySpark/Databricks

---

## 🔗 Recursos do Projeto

### **Repositório GitHub**
🔗 https://github.com/guilhermeandradev/CantuStore

**Conteúdo:**
- `Parte1_SQL/` - Soluções SQL completas
- `Parte2_AnaliseDados/notebooks/` - 10 notebooks PySpark
- Documentação técnica completa

### **Databricks Workspace**
🔗 https://dbc-42c3ab84-833a.cloud.databricks.com/browse/folders/4203178988216001?o=7474656927229489

**Acesso aos notebooks funcionais + resultados executados**

### **Dados de Origem**
📦 SharePoint: [Link fornecido pela CantuStore]

---

## 📊 PARTE 1 - Questões SQL

### **Questão 1.1 - Classificação de Campeonato**

#### **Enunciado:**
Calcule o número total de pontos que cada equipe marcou após todas as partidas. As regras são:
- Vitória (mais gols que o adversário): 3 pontos
- Empate (mesmo número de gols): 1 ponto
- Derrota (menos gols que o adversário): 0 pontos

Retorne uma classificação de todas as equipes ordenada por pontos (DESC) e, em caso de empate, por time_id (ASC).

#### **Resposta:**

```sql
WITH pontos_mandante AS (
    SELECT
        mandante_time AS time_id,
        CASE
            WHEN mandante_gols > visitante_gols THEN 3
            WHEN mandante_gols = visitante_gols THEN 1
            ELSE 0
        END AS pontos
    FROM jogos
),
pontos_visitante AS (
    SELECT
        visitante_time AS time_id,
        CASE
            WHEN visitante_gols > mandante_gols THEN 3
            WHEN visitante_gols = mandante_gols THEN 1
            ELSE 0
        END AS pontos
    FROM jogos
),
pontos_totais AS (
    SELECT time_id, SUM(pontos) AS num_pontos
    FROM (
        SELECT time_id, pontos FROM pontos_mandante
        UNION ALL
        SELECT time_id, pontos FROM pontos_visitante
    ) AS todos_pontos
    GROUP BY time_id
)
SELECT
    t.time_id,
    t.time_nome,
    COALESCE(pt.num_pontos, 0) AS num_pontos
FROM times t
LEFT JOIN pontos_totais pt ON t.time_id = pt.time_id
ORDER BY num_pontos DESC, t.time_id;
```

📄 **Arquivo:** `Parte1_SQL/1.1_campeonato.sql`

---

### **Questão 1.2 - Análise de Comissões**

#### **Enunciado:**
Retorne a lista de vendedores que receberam pelo menos R$ 1.024 em até três transferências. 

Em outras palavras: se existirem três ou menos comissões cuja soma seja >= R$ 1.024, o vendedor deve ser listado. Ordene por nome do vendedor (ASC).

#### **Resposta:**

```sql
WITH comissoes_ordenadas AS (
    SELECT
        vendedor,
        valor,
        ROW_NUMBER() OVER (PARTITION BY vendedor ORDER BY valor DESC) AS rn
    FROM comissoes
),
top3_comissoes AS (
    SELECT
        vendedor,
        SUM(valor) AS soma_top3
    FROM comissoes_ordenadas
    WHERE rn <= 3
    GROUP BY vendedor
)
SELECT DISTINCT vendedor
FROM top3_comissoes
WHERE soma_top3 >= 1024
ORDER BY vendedor;
```

📄 **Arquivo:** `Parte1_SQL/1.2_comissoes.sql`

---

### **Questão 1.3 - Hierarquia de Colaboradores**

#### **Enunciado:**
Para cada funcionário, encontre o chefe indireto de classificação **mais baixa na hierarquia** (aquele com mais chefes indiretos) que ganha pelo menos o dobro do salário do funcionário.

Se nenhum chefe atender a condição, retorne NULL. Ordene por id do funcionário (ASC).

#### **Resposta:**

```sql
WITH RECURSIVE
-- Mapear todos os chefes indiretos
chefes_indiretos AS (
    SELECT
        c.id AS funcionario_id,
        c.lider_id AS chefe_id,
        c.salario AS funcionario_salario
    FROM colaboradores c
    WHERE c.lider_id IS NOT NULL

    UNION ALL

    SELECT
        ci.funcionario_id,
        c.lider_id AS chefe_id,
        ci.funcionario_salario
    FROM chefes_indiretos ci
    INNER JOIN colaboradores c ON ci.chefe_id = c.id
    WHERE c.lider_id IS NOT NULL
),
-- Contar quantos chefes indiretos cada pessoa tem (profundidade)
contagem_chefes AS (
    SELECT
        funcionario_id,
        COUNT(*) AS num_chefes_indiretos
    FROM chefes_indiretos
    GROUP BY funcionario_id
),
-- Filtrar chefes que ganham >= 2x o salário
chefes_validos AS (
    SELECT
        ci.funcionario_id,
        ci.chefe_id,
        c.salario AS chefe_salario,
        COALESCE(cc.num_chefes_indiretos, 0) AS chefe_num_indiretos
    FROM chefes_indiretos ci
    INNER JOIN colaboradores c ON ci.chefe_id = c.id
    LEFT JOIN contagem_chefes cc ON ci.chefe_id = cc.funcionario_id
    WHERE c.salario >= ci.funcionario_salario * 2
),
-- Selecionar o chefe com MAIS chefes indiretos (mais baixo)
chefes_mais_baixos AS (
    SELECT
        funcionario_id,
        chefe_id,
        ROW_NUMBER() OVER (
            PARTITION BY funcionario_id
            ORDER BY chefe_num_indiretos DESC, chefe_id ASC
        ) AS rn
    FROM chefes_validos
)
SELECT
    c.id AS id,
    cmb.chefe_id AS chefe_id
FROM colaboradores c
LEFT JOIN chefes_mais_baixos cmb
    ON c.id = cmb.funcionario_id AND cmb.rn = 1
ORDER BY c.id;
```

📄 **Arquivo:** `Parte1_SQL/1.3_colaboradores.sql`

---

## 📊 PARTE 2 - Análise de Carrinhos Abandonados

### **Contexto**

Análise de carrinhos abandonados em e-commerce de pneus utilizando **Databricks** e **PySpark**.

**Período dos dados:** 2019-12-16 a 2022-07-26 (2,61 anos)

---

### **Filtros Aplicados (Garantia de Qualidade)**

Antes de todas as análises, aplicamos 3 filtros críticos:

1. **Deduplicação:** 11.134 PKs duplicados removidos
2. **Filtro de Abandono:** Apenas carrinhos com `p_paymentinfo IS NULL` e `p_totalprice > 0`
3. **Remoção de Outliers:** 4.267 carrinhos > R$ 50.000 removidos

**Resultado:** 
- **905.180 carrinhos abandonados** (dataset limpo e validado)
- **R$ 6.267.369.294,36** em valor não faturado
- **Ticket médio:** R$ 6.923,89

📄 **Documentação:** `Parte2_AnaliseDados/FILTROS_CARRINHOS_ABANDONADOS.md`

---

### **Questão 2.1 - Produtos com Mais Carrinhos Abandonados**

#### **Enunciado:**
Quais os produtos que mais tiveram carrinhos abandonados?

#### **Resposta:**
✅ Análise realizada com agregação por produto:
- Quantidade de carrinhos únicos abandonados (`countDistinct`)
- Quantidade total de itens
- Valor total não faturado

**Resultado:** Top 50 produtos identificados e ranqueados

📄 **Notebook:** `02_analise_produtos.py`

---

### **Questão 2.2 - Duplas de Produtos Abandonados Juntos**

#### **Enunciado:**
Quais as duplas de produtos em conjunto que mais tiveram carrinhos abandonados?

#### **Resposta:**
✅ Análise realizada com:
- Identificação de produtos no mesmo carrinho
- Geração de combinações (duplas e trios)
- Ranking por frequência de co-ocorrência

**Resultado:** Top duplas e trios identificados

📄 **Notebook:** `03_analise_duplas.py`

---

### **Questão 2.3 - Produtos com Aumento de Abandono**

#### **Enunciado:**
Quais produtos tiveram um aumento de abandono?

#### **Resposta:**
✅ Análise temporal realizada:
- Agregação mensal por produto
- Cálculo de variação percentual mês a mês
- Identificação de produtos com tendência crescente

**Resultado:** Produtos com crescimento de abandono detectados

📄 **Notebook:** `04_analise_tendencia.py`

---

### **Questão 2.4 - Produtos Novos no Primeiro Mês**

#### **Enunciado:**
Quais os produtos novos e a quantidade de carrinhos no seu primeiro mês de lançamento?

#### **Resposta:**
✅ Análise de lançamentos realizada:
- Identificação do primeiro mês de cada produto
- Contagem de abandonos no mês de lançamento
- Ranking de performance inicial

**Resultado:** Lista de produtos novos com métricas do 1º mês

📄 **Notebook:** `05_analise_produtos_novos.py`

---

### **Questão 2.5 - Estados com Mais Abandonos**

#### **Enunciado:**
Quais estados tiveram mais abandonos?

#### **Resposta:**
✅ Análise geográfica realizada:
- JOIN com tabelas de endereços e regiões
- Agregação por estado (UF)
- Cálculo de ticket médio por estado
- Agrupamento por região (Sul, Sudeste, etc)

**Resultado:** Ranking de estados + análise regional

📄 **Notebook:** `06_analise_estados.py`

---

### **Questão 2.6 - Relatório Mensal por Produto**

#### **Enunciado:**
Gere um relatório dos produtos, mês a mês, informando:
- Quantidade de carrinhos abandonados
- Quantidade de itens abandonados
- Valor não faturado

#### **Resposta:**
✅ Relatório gerado com granularidade produto + mês

**Formato:** CSV com colunas `product | ano_mes | qtd_carrinhos | qtd_itens | valor_nao_faturado`

**Output:** `/resultados/relatorio_por_produto_mes.csv`

📄 **Notebook:** `07_relatorio_produto_mes.py`

---

### **Questão 2.7 - Relatório Diário**

#### **Enunciado:**
Gere um relatório por data informando:
- Quantidade de carrinhos abandonados
- Quantidade de itens abandonados
- Valor não faturado

#### **Resposta:**
✅ Relatório gerado com granularidade diária

**Formato:** CSV com colunas `data | qtd_carrinhos | qtd_itens | valor_nao_faturado`

**Análises Adicionais:**
- Média móvel de 7 dias
- Identificação de outliers (dias atípicos)
- Padrões por dia da semana

**Output:** `/resultados/relatorio_por_data.csv`

📄 **Notebook:** `08_relatorio_data.py`

---

### **Questão 2.8 - Exportação TXT (Top 50 Carrinhos)**

#### **Enunciado:**
Exporte um arquivo .txt com os 50 carrinhos com os maiores `carts.p_totalprice` no layout especificado:

```
carts.PK|carts.createdTS|carts.p_totalprice|user.p_uid|payment_modes.p_code|...
cartentries.p_product|cartentries.p_quantity|cartentries.p_totalprice
cartentries.p_product|cartentries.p_quantity|cartentries.p_totalprice
[próximo carrinho]
```

#### **Resposta:**
✅ Exportação realizada com:
- Top 50 carrinhos por valor (ordenados DESC)
- JOIN com 6 tabelas para obter todos os campos
- Formatação customizada conforme layout especificado
- Delimiter: pipe (|)

**Output:** `/resultados/top50_carrinhos.txt`

📄 **Notebook:** `09_exportacao_txt.py`

---

## 📊 Resultados Finais

### **Estatísticas do Projeto**

| Métrica | Valor |
|---------|-------|
| **Carrinhos abandonados** | 905.180 |
| **Itens abandonados** | 2.769.758 |
| **Valor não faturado** | R$ 6.267.369.294,36 |
| **Ticket médio** | R$ 6.923,89 |
| **Preço médio por pneu** | R$ 2.262,79 |
| **Itens por carrinho** | 3,06 pneus |
| **Abandonos por dia** | 950 carrinhos |

### **Cobertura de Requisitos**

| Requisito | Status | Evidência |
|-----------|--------|-----------|
| SQL 1.1 - Campeonato | ✅ | `1.1_campeonato.sql` |
| SQL 1.2 - Comissões | ✅ | `1.2_comissoes.sql` |
| SQL 1.3 - Colaboradores | ✅ | `1.3_colaboradores.sql` |
| Análise - Top Produtos | ✅ | `02_analise_produtos.py` |
| Análise - Duplas | ✅ | `03_analise_duplas.py` |
| Análise - Tendência | ✅ | `04_analise_tendencia.py` |
| Análise - Produtos Novos | ✅ | `05_analise_produtos_novos.py` |
| Análise - Estados | ✅ | `06_analise_estados.py` |
| Relatório Mensal | ✅ | `07_relatorio_produto_mes.py` |
| Relatório Diário | ✅ | `08_relatorio_data.py` |
| Exportação TXT | ✅ | `09_exportacao_txt.py` |

**Cobertura Total: 11/11 requisitos (100%)** ✅

---

## 🛠️ Stack Tecnológico

- **Databricks Community Edition** - Plataforma de processamento
- **Apache Spark 3.4 + PySpark** - Engine distribuído
- **SQL** - Queries analíticas
- **Python 3.10** - Lógica de negócio
- **Git/GitHub** - Versionamento

---

## 📚 Documentação

### **Repositório GitHub**
🔗 https://github.com/guilhermeandradev/CantuStore

**Arquivos importantes:**
- `README.md` - Guia completo de execução
- `FILTROS_CARRINHOS_ABANDONADOS.md` - Documentação dos filtros
- `RESUMO_NOTEBOOKS.md` - Resumo de cada análise
- `GUIA_DATABRICKS_GITHUB.md` - Integração Databricks + GitHub

### **Como Executar**

1. Clone o repositório no Databricks
2. Faça upload dos dados no Volume
3. Execute os notebooks na ordem (00 → 01 → 02... → 09)
4. Tempo total: ~15-20 minutos

Instruções detalhadas no **README.md**

---

## ✅ Conclusão

Todos os requisitos do desafio foram atendidos:
- ✅ 3 questões SQL resolvidas com técnicas avançadas (CTEs recursivas, window functions)
- ✅ 8 análises exploratórias de carrinhos abandonados
- ✅ 2 relatórios gerenciais (CSV)
- ✅ 1 exportação customizada (TXT)
- ✅ Código 100% funcional e documentado
- ✅ Dataset validado com filtros de qualidade

**Resultados:** 905.180 carrinhos abandonados identificados, R$ 6,27 bilhões em oportunidade de recuperação.

---

**Elaborado por Engenharia de Dados**  
**CantuStore - Plataforma de tecnologia e logística em pneus**

*Versão 1.0 - Fevereiro 2026*
