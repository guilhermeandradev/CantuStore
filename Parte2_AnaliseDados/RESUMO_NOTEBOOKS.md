# Resumo dos Notebooks - Análise de Carrinhos Abandonados

Este documento resume o conteúdo e objetivo de cada notebook do projeto.

---

## 📁 Estrutura dos Notebooks

### 00_setup.py - Configuração Inicial
**Objetivo:** Configurar ambiente e definir variáveis globais

**O que faz:**
- Importa bibliotecas necessárias (PySpark, funções SQL)
- Define paths para dados e resultados
- Cria funções utilitárias (verificar_dados, mostrar_schema, etc.)
- Testa conexão com DBFS
- Configura otimizações do Spark

**Saída:** Ambiente configurado e pronto para uso

---

### 01_carregamento_dados.py - Carregamento e Limpeza de Dados
**Objetivo:** Carregar todas as tabelas, aplicar filtros e criar views temporárias

**O que faz:**
- Carrega tabelas Parquet (carts, cartentries, addresses, paymentinfos)
- Carrega tabelas CSV (users, regions, paymentmodes, cmssitelp)
- **DEDUPLICAÇÃO**: Remove 11.134 PKs duplicados em tb_carts
- **FILTRO DE ABANDONO**: Seleciona apenas carrinhos abandonados
  - `p_paymentinfo IS NULL` (nunca iniciou pagamento)
  - `p_totalprice > 0` (tem produtos)
- **REMOÇÃO DE OUTLIERS**: Remove carrinhos > R$ 50.000 (4.267 outliers)
- Gera DataFrame principal (JOIN carts abandonados + cartentries)
- Adiciona colunas de data (ano, mês, ano_mes, data)
- Calcula estatísticas finais do dataset

**Saída:** 
- 8 views temporárias criadas
- DataFrame `df_carts_items` com apenas carrinhos abandonados realistas
- **905.180 carrinhos abandonados** | R$ 6,27 bilhões | Ticket médio: R$ 6.923,89

> 📖 **Documentação dos filtros**: [FILTROS_CARRINHOS_ABANDONADOS.md](FILTROS_CARRINHOS_ABANDONADOS.md)

---

### 02_analise_produtos.py - Produtos Mais Abandonados
**Objetivo:** Responder "Quais produtos mais tiveram carrinhos abandonados?"

**O que faz:**
- Agrupa por produto
- Conta carrinhos, soma itens e valor
- Ordena por quantidade de carrinhos (DESC)
- Mostra top 50 produtos
- Gera estatísticas dos top 10

**Saída:**
- Tabela com top produtos
- CSV: `produtos_mais_abandonados.csv`

**Colunas do resultado:**
- `product`: ID do produto
- `qtd_carrinhos`: Quantidade de carrinhos
- `qtd_itens`: Quantidade de itens
- `valor_total`: Valor total não faturado

---

### 03_analise_duplas.py - Duplas de Produtos
**Objetivo:** Responder "Quais duplas de produtos mais foram abandonadas juntas?"

**O que faz:**
- Self-join para encontrar pares (produto_1 < produto_2)
- Conta ocorrências de cada par
- Ordena por quantidade
- **Bônus:** Analisa triplas de produtos

**Saída:**
- Tabela com top 50 duplas
- CSV: `duplas_produtos_abandonados.csv`
- CSV: `triplas_produtos_abandonados.csv`

**Colunas do resultado:**
- `produto_1`: ID do primeiro produto
- `produto_2`: ID do segundo produto
- `qtd_carrinhos`: Quantidade de carrinhos com ambos

---

### 04_analise_tendencia.py - Tendência de Abandono
**Objetivo:** Responder "Quais produtos tiveram aumento de abandono?"

**O que faz:**
- Agrupa por produto e mês
- Usa Window Function LAG() para comparar com mês anterior
- Calcula variação absoluta e percentual
- Identifica produtos com aumento no último período
- Identifica produtos com aumento consistente (3 meses)
- Analisa sazonalidade mensal

**Saída:**
- Tabela com produtos em crescimento
- CSV: `produtos_aumento_abandono.csv`
- CSV: `produtos_aumento_consistente.csv`
- CSV: `tendencia_abandono_completa.csv`

**Colunas principais:**
- `product`: ID do produto
- `ano_mes`: Período
- `qtd_carrinhos`: Quantidade atual
- `qtd_mes_anterior`: Quantidade do mês anterior
- `variacao`: Diferença absoluta
- `variacao_pct`: Diferença percentual

---

### 05_analise_produtos_novos.py - Produtos Novos
**Objetivo:** Responder "Quais produtos novos e quantidade de carrinhos no primeiro mês?"

**O que faz:**
- Identifica primeiro mês de cada produto (MIN)
- Conta carrinhos e itens no lançamento
- Agrupa lançamentos por período
- Analisa produtos lançados recentemente
- Classifica performance no lançamento
- Analisa evolução dos top 10 nos 3 primeiros meses

**Saída:**
- Tabela com todos os produtos e primeiro mês
- CSV: `produtos_novos_primeiro_mes.csv`
- CSV: `lancamentos_por_periodo.csv`
- CSV: `produtos_lancados_recentemente.csv`

**Colunas principais:**
- `product`: ID do produto
- `primeiro_mes_lancamento`: Mês de lançamento
- `qtd_carrinhos`: Quantidade no primeiro mês
- `qtd_itens`: Itens no primeiro mês
- `valor_total`: Valor no primeiro mês

---

### 06_analise_estados.py - Abandonos por Estado
**Objetivo:** Responder "Quais estados tiveram mais abandonos?"

**O que faz:**
- JOIN carts + addresses + regions
- Agrupa por estado (UF)
- Calcula total de carrinhos, valor e ticket médio
- Analisa evolução temporal por estado
- Calcula participação % de cada estado
- Agrupa por macro-regiões (Sudeste, Sul, etc.)
- Identifica top CEPs por estado

**Saída:**
- Tabela com todos os estados
- CSV: `abandonos_por_estado.csv`
- CSV: `abandonos_estado_temporal.csv`
- CSV: `abandonos_por_regiao.csv`
- CSV: `participacao_por_estado.csv`

**Colunas principais:**
- `estado`: UF (SP, RJ, MG, etc.)
- `qtd_carrinhos`: Quantidade de carrinhos
- `valor_total`: Valor total
- `ticket_medio`: Valor médio por carrinho

---

### 07_relatorio_produto_mes.py - Relatório Mensal
**Objetivo:** Gerar relatório mensal por produto

**O que faz:**
- Agrupa por produto e ano_mes
- Calcula: carrinhos, itens, valor não faturado
- Gera consolidado mensal (todos os produtos)
- Identifica produtos com maior variação
- Analisa top produtos por valor histórico

**Saída:**
- CSV: `relatorio_produto_mes.csv` (completo)
- CSV: `relatorio_consolidado_mensal.csv`
- CSV: `top_produtos_valor_nao_faturado.csv`

**Colunas principais:**
- `product`: ID do produto
- `ano_mes`: Período (YYYY-MM)
- `qtd_carrinhos_abandonados`: Quantidade de carrinhos
- `qtd_itens_abandonados`: Quantidade de itens
- `valor_nao_faturado`: Valor não faturado (R$)

---

### 08_relatorio_data.py - Relatório Diário
**Objetivo:** Gerar relatório diário consolidado

**O que faz:**
- Agrupa por data
- Calcula: carrinhos, itens, valor não faturado
- Identifica dias com maior volume
- Analisa por dia da semana
- Calcula média móvel de 7 dias
- Identifica outliers (dias atípicos)
- Analisa padrão por período do mês (início, meio, fim)

**Saída:**
- CSV: `relatorio_por_data.csv` (completo)
- CSV: `relatorio_data_media_movel.csv`
- CSV: `relatorio_por_dia_semana.csv`
- CSV: `dias_outliers.csv`

**Colunas principais:**
- `data`: Data (YYYY-MM-DD)
- `qtd_carrinhos_abandonados`: Quantidade de carrinhos
- `qtd_itens_abandonados`: Quantidade de itens
- `valor_nao_faturado`: Valor não faturado (R$)

---

### 09_exportacao_txt.py - Exportação TXT
**Objetivo:** Exportar top 50 carrinhos no formato especificado

**O que faz:**
- Seleciona top 50 carrinhos por `p_totalprice`
- JOIN com todas as tabelas auxiliares
- Formata no layout especificado:
  - Linha header: dados do carrinho
  - Linhas entries: produtos do carrinho
- Salva arquivo TXT no DBFS
- Gera estatísticas do arquivo

**Saída:**
- TXT: `top50_carrinhos.txt`

**Formato do arquivo:**
```
cart_pk|createdTS|totalprice|user_uid|payment_mode|installments|site|postalcode|sum_qty|count_entries
product|quantity|totalprice|
product|quantity|totalprice|
...
```

---

## 📊 Resumo de Entregas

### Análises (5)
1. ✅ Produtos mais abandonados
2. ✅ Duplas de produtos
3. ✅ Tendência de abandono
4. ✅ Produtos novos
5. ✅ Estados com mais abandonos

### Relatórios (2)
1. ✅ Mensal por produto
2. ✅ Diário

### Exportação (1)
1. ✅ TXT top 50 carrinhos

---

## 🔄 Fluxo de Execução

```
00_setup.py
    ↓
01_carregamento_dados.py
    ↓
┌─────────────────────────────┐
│  02 a 06: Análises          │
│  (podem rodar em paralelo)  │
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│  07 e 08: Relatórios        │
│  (podem rodar em paralelo)  │
└─────────────────────────────┘
    ↓
09_exportacao_txt.py
```

---

## 💡 Dicas de Uso

1. **Sempre execute 00 e 01 primeiro** - São pré-requisitos
2. **Use %run** - Notebooks 02-09 usam `%run ./01_carregamento_dados.py`
3. **Ajuste paths** - Verifique `BASE_PATH` no notebook 00
4. **Monitore performance** - Use `.cache()` em DataFrames grandes
5. **Teste com samples** - Use `.limit(100)` para testes rápidos

---

## 📈 Métricas de Execução (Estimadas)

| Notebook | Tempo Estimado | Memória |
|----------|----------------|---------|
| 00 | < 1 min | Baixa |
| 01 | 2-5 min | Alta |
| 02 | 2-3 min | Média |
| 03 | 5-10 min | Alta (self-join) |
| 04 | 3-5 min | Média |
| 05 | 2-3 min | Média |
| 06 | 3-5 min | Média (múltiplos JOINs) |
| 07 | 2-3 min | Média |
| 08 | 2-3 min | Média |
| 09 | 1-2 min | Baixa |

**Total estimado:** 25-40 minutos

*Tempos baseados em Databricks Community Edition com cluster padrão.*

---

## 🎯 Próximos Passos

Após executar todos os notebooks:

1. ✅ Baixar os CSVs gerados
2. ✅ Baixar o arquivo TXT
3. ✅ Revisar insights de negócio
4. ✅ Preparar apresentação dos resultados
5. ✅ Commit e push para GitHub

**Projeto completo!** 🚀
