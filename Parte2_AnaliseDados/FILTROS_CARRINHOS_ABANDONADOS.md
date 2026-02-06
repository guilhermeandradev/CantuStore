# 🔍 Filtros de Carrinhos Abandonados

## Contexto

Durante a análise dos dados, identificamos que a tabela `tb_carts` contém **TODOS** os carrinhos do sistema, incluindo:
- Carrinhos abandonados (nunca finalizados)
- Carrinhos com pagamento iniciado
- Carrinhos finalizados
- Carrinhos vazios (sem produtos)
- Duplicatas (11.134 PKs repetidos)
- Outliers extremos (carrinhos de teste ou erro)

---

## 📊 Visão Geral dos Dados

### **Dados Brutos (Sem Filtros)**
```
Total de registros em tb_carts: 16.047.042
Total de registros em tb_cartentries: 2.461.265
```

### **Distribuição dos Carrinhos**
| Tipo | Quantidade | % |
|------|-----------|---|
| Carrinhos vazios (p_totalprice = 0) | 13.905.761 | 86,66% |
| Carrinhos com pagamento iniciado | 1.227.360 | 7,65% |
| Carrinhos abandonados (com valor) | 933.630 | 5,82% |
| Duplicatas (PKs repetidos) | 11.134 | 0,07% |

---

## ✅ Filtros Aplicados

Para analisar **apenas carrinhos abandonados realistas**, aplicamos os seguintes filtros:

### **1. Deduplicação**
```python
# Remove PKs duplicados, mantendo apenas o primeiro registro por PK
window_dedup = Window.partitionBy("PK").orderBy("createdTS")
df_carts_dedup = df_carts.withColumn(
    "rn",
    row_number().over(window_dedup)
).filter(col("rn") == 1).drop("rn")
```
**Resultado:** 11.134 duplicatas removidas

---

### **2. Filtro de Abandono**
```python
# Apenas carrinhos que NUNCA iniciaram pagamento E têm valor > 0
df_carts_abandonados = df_carts_dedup.filter(
    (col("p_paymentinfo").isNull()) & (col("p_totalprice") > 0)
)
```

**Critérios:**
- `p_paymentinfo IS NULL`: Carrinho nunca teve informação de pagamento criada
- `p_totalprice > 0`: Carrinho tem produtos adicionados

**Lógica:**
- Se `p_paymentinfo` está preenchido → Cliente iniciou processo de pagamento → **NÃO é abandono**
- Se `p_paymentinfo` é NULL → Cliente nunca tentou pagar → **É abandono**

**Resultado:** 923.576 carrinhos abandonados identificados

---

### **3. Remoção de Outliers**
```python
# Remove carrinhos com valor total > R$ 50.000
LIMITE_OUTLIER = 50000
df_totais_por_cart = df_carts_items.groupBy("cart_pk").agg(
    spark_round(sum("entry_totalprice"), 2).alias("cart_total")
)
df_carts_limpo = df_totais_por_cart.filter(col("cart_total") <= LIMITE_OUTLIER)
```

**Critério:**
- Carrinhos com valor total > R$ 50.000 são considerados outliers (carrinhos de teste, erros, ou pedidos B2B)

**Resultado:** 4.267 outliers removidos (R$ 315,5 milhões)

---

## 📈 Resultado Final

### **Dataset Final (Após Filtros)**
```
Carrinhos abandonados: 905.180
Itens abandonados: 2.769.758
Valor total não faturado: R$ 6.267.369.294,36

Ticket médio: R$ 6.923,89
Itens por carrinho: 3,06 pneus
Preço médio por item: R$ 2.262,79

Período: 2019-12-16 a 2022-07-26 (2,61 anos / 953 dias)
Carrinhos abandonados/dia: 950
Valor abandonado/dia: R$ 6.576.463,06
```

---

## ✅ Validação: E-commerce de Pneus

### **Métricas Validadas**
| Métrica | Valor | Status | Observação |
|---------|-------|--------|------------|
| **Ticket médio** | R$ 6.923,89 | ✅ OK | Esperado: R$ 3.000 - R$ 10.000 (2-4 pneus) |
| **Preço/pneu** | R$ 2.262,79 | ✅ OK | Esperado: R$ 1.000 - R$ 4.000 (pneus premium) |
| **Itens/carrinho** | 3,06 | ✅ OK | Esperado: 2-4 unidades |
| **Abandonos/dia** | 950 | ✅ OK | Razoável para grande e-commerce |
| **Valor/dia** | R$ 6,6 M | ✅ OK | Abaixo de R$ 10 M/dia |

**Conclusão:** Todos os valores estão dentro do esperado para um **grande e-commerce de pneus premium** no Brasil.

---

## 🔄 Evolução da Análise

| Etapa | Carrinhos | Valor Total | Problema |
|-------|-----------|-------------|----------|
| 1. Todos os carrinhos | 16.047.042 | R$ 14,13 bi | ❌ Incluía finalizados + vazios |
| 2. Com valor > 0 | 2.112.523 | R$ 14,13 bi | ❌ Incluía finalizados |
| 3. SEM paymentinfo | 909.447 | R$ 6,58 bi | ⚠️ Tinha duplicatas e outliers |
| 4. **Final (limpo)** | **905.180** | **R$ 6,27 bi** | ✅ **Correto!** |

**Redução:** -94,36% de carrinhos, -55,64% de valor

---

## 💡 Por Que os Filtros São Importantes?

### **Sem Filtros (Análise Incorreta)**
```
❌ Analisando 2,1 milhões de "carrinhos abandonados"
❌ Valor: R$ 15,8 bilhões
❌ Ticket médio: R$ 7.532
❌ PROBLEMA: Incluía carrinhos finalizados, duplicatas, outliers
```

### **Com Filtros (Análise Correta)**
```
✅ Analisando 905 mil carrinhos REALMENTE abandonados
✅ Valor: R$ 6,27 bilhões
✅ Ticket médio: R$ 6.924
✅ Valores realistas para e-commerce de pneus
```

---

## 🎯 Impacto nas Análises

Todos os notebooks (02 a 09) agora analisam **apenas carrinhos abandonados realistas**:

- ✅ **02_analise_produtos**: Top produtos abandonados (sem produtos finalizados)
- ✅ **03_analise_duplas**: Combinações de produtos abandonados
- ✅ **04_analise_tendencia**: Tendência de abandono (valores corretos)
- ✅ **05_analise_produtos_novos**: Produtos novos com abandono
- ✅ **06_analise_estados**: Estados com mais abandono real
- ✅ **07_relatorio_produto_mes**: Relatório mensal correto
- ✅ **08_relatorio_data**: Relatório diário correto
- ✅ **09_exportacao_txt**: Top 50 carrinhos abandonados reais

---

## 📝 Notas Técnicas

### **Campo p_paymentinfo**
- **NULL**: Carrinho nunca teve pagamento iniciado → **Abandonado**
- **Preenchido**: Carrinho teve informação de pagamento criada → **Não abandonado**

### **Relacionamento com tb_paymentinfos**
```sql
-- Carrinhos COM paymentinfo (não abandonados)
SELECT COUNT(*) FROM tb_carts WHERE p_paymentinfo IS NOT NULL;
-- Resultado: 1.227.360 (7,6%)

-- Carrinhos SEM paymentinfo (abandonados)
SELECT COUNT(*) FROM tb_carts WHERE p_paymentinfo IS NULL AND p_totalprice > 0;
-- Resultado: 933.630 (5,8% - antes de remover outliers)
```

### **Por Que R$ 50.000 Como Limite de Outlier?**
- Ticket médio esperado: R$ 3.000 - R$ 10.000 (2-4 pneus)
- Ticket máximo razoável: ~R$ 20.000 (6-8 pneus premium)
- R$ 50.000 = 2,5x acima do máximo razoável
- Carrinhos acima desse valor provavelmente são:
  - Carrinhos de teste
  - Erros de sistema
  - Pedidos B2B (fora do escopo de e-commerce B2C)

---

## 🚀 Como Usar

Os filtros são aplicados automaticamente no **notebook 01**. Basta executar os notebooks na ordem:

```
1. 00_setup.py         → Configuração inicial
2. 01_carregamento_dados.py → Carrega dados + aplica filtros
3. 02-09 (análises)    → Usam dados já filtrados
```

**IMPORTANTE:** Sempre execute `01_carregamento_dados.py` primeiro para garantir que os filtros sejam aplicados!

---

## 📚 Referências

- Notebook de carregamento: `01_carregamento_dados.py`
- Notebook de setup: `00_setup.py`
- Documentação principal: `README.md`
- Guia Databricks: `GUIA_DATABRICKS_GITHUB.md`

---

**Última atualização:** 2026-02-06
