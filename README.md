# CANTUSTORE - Análise de Carrinhos Abandonados

Projeto completo de análise de dados utilizando **Databricks** e **PySpark** para identificar padrões de carrinhos abandonados no e-commerce da CantuStore.

---

## 📋 Estrutura do Projeto

```
CantuStore/
├── Parte1_SQL/                      # Questões SQL da prova
│   ├── 1.1_campeonato.sql           # Classificação de campeonato
│   ├── 1.2_comissoes.sql            # Análise de comissões
│   └── 1.3_colaboradores.sql        # Hierarquia de colaboradores
│
├── Parte2_AnaliseDados/             # Análise de Dados com PySpark
│   ├── data/                        # Dados (não versionados)
│   │   ├── tb_carts/
│   │   ├── tb_cartentries/
│   │   ├── tb_addresses/
│   │   ├── tb_paymentinfos/
│   │   ├── tb_users.csv
│   │   ├── tb_regions.csv
│   │   ├── tb_paymentmodes.csv
│   │   └── tb_cmssitelp.csv
│   │
│   └── notebooks/                   # Notebooks PySpark
│       ├── 00_setup.py              # Configuração inicial
│       ├── 01_carregamento_dados.py # Carregamento e validação
│       ├── 02_analise_produtos.py   # Produtos mais abandonados
│       ├── 03_analise_duplas.py     # Duplas de produtos
│       ├── 04_analise_tendencia.py  # Tendência de abandono
│       ├── 05_analise_produtos_novos.py  # Produtos novos
│       ├── 06_analise_estados.py    # Abandonos por estado
│       ├── 07_relatorio_produto_mes.py   # Relatório mensal
│       ├── 08_relatorio_data.py     # Relatório diário
│       └── 09_exportacao_txt.py     # Exportação TXT
│
├── .gitignore                       # Arquivos ignorados
└── README.md                        # Este arquivo
```

---

## 🎯 Objetivos do Projeto

### Parte 1: SQL

Resolução de 3 questões usando SQL:
1. ✅ **Campeonato**: Classificação de times por pontuação
2. ✅ **Comissões**: Vendedores com >= R$ 1.024 em até 3 transferências
3. ✅ **Colaboradores**: Identificar chefe indireto mais baixo na hierarquia

### Parte 2: Análise de Dados (PySpark + Databricks)

Análise de carrinhos abandonados respondendo:

**5 Análises Exploratórias:**
1. ✅ Quais produtos mais tiveram carrinhos abandonados?
2. ✅ Quais duplas de produtos mais foram abandonadas juntas?
3. ✅ Quais produtos tiveram aumento de abandono?
4. ✅ Quais produtos novos e sua performance no primeiro mês?
5. ✅ Quais estados tiveram mais abandonos?

**2 Relatórios:**
1. ✅ Relatório mensal por produto (carrinhos, itens, valor não faturado)
2. ✅ Relatório diário (carrinhos, itens, valor não faturado)

**1 Exportação:**
1. ✅ Arquivo TXT com top 50 carrinhos no formato especificado

---

## 🚀 Como Executar

### Pré-requisitos

- Conta no [Databricks Community Edition](https://community.cloud.databricks.com/)
- Acesso ao GitHub (opcional, para versionamento)

### Passo 1: Configurar Databricks

1. **Criar Cluster:**
   - Acesse: Compute > Create Cluster
   - Runtime: 13.3 LTS ou superior
   - Configuração: Padrão (Community Edition)

2. **Conectar GitHub (Opcional):**
   - Workspace > Repos > Add Repo
   - Clonar este repositório

### Passo 2: Upload dos Dados

**Opção A - Via UI:**
1. Data > Create Table > Upload File
2. Fazer upload de todos os arquivos da pasta `Parte2_AnaliseDados/data/`

**Opção B - Via dbutils:**
```python
# Copiar arquivos locais para DBFS
dbutils.fs.cp("file:/local/path", "dbfs:/FileStore/cantustore/")
```

### Passo 3: Executar Notebooks

Execute os notebooks na ordem:

1. **00_setup.py** - Configuração inicial
2. **01_carregamento_dados.py** - Carregar dados e criar views
3. **02 a 06** - Análises exploratórias
4. **07 e 08** - Relatórios
5. **09** - Exportação TXT

---

## 📊 Resultados Obtidos

### Análises Principais

| Análise | Resultado |
|---------|-----------|
| **Produtos Mais Abandonados** | Top 50 produtos identificados |
| **Duplas de Produtos** | Pares frequentemente abandonados juntos |
| **Tendência de Abandono** | Produtos com crescimento identificados |
| **Produtos Novos** | Performance no primeiro mês analisada |
| **Estados** | Concentração geográfica de abandonos |

### Relatórios Gerados

- **Mensal por Produto**: CSV com métricas mensais detalhadas
- **Diário**: CSV com métricas diárias consolidadas
- **TXT Top 50**: Arquivo no formato especificado para análise

---

## 🛠️ Tecnologias Utilizadas

- **Databricks**: Plataforma de análise de dados
- **PySpark**: Processamento distribuído de dados
- **SQL**: Queries e análises relacionais
- **Python**: Lógica de negócio e transformações

---

## 📈 Insights de Negócio

### Principais Descobertas:

1. **Concentração de Abandonos**: Identificados produtos com alto volume de abandonos
2. **Produtos Complementares**: Duplas/triplas frequentemente abandonadas juntas sugerem oportunidades de bundle
3. **Tendência Temporal**: Produtos com crescimento de abandono requerem atenção
4. **Sazonalidade**: Padrões de abandono por dia da semana e período do mês
5. **Distribuição Geográfica**: Estados com maior concentração para ações regionalizadas

### Recomendações:

- Implementar remarketing para produtos com alto abandono
- Criar ofertas de bundle para produtos abandonados juntos
- Investigar causas do aumento de abandono em produtos específicos
- Otimizar processo de checkout nos estados com mais abandonos
- Ajustar estratégias de frete e pagamento por região

---

## 👥 Autor

Projeto desenvolvido como parte da prova técnica para a **CantuStore**.

---

## 📝 Licença

Este projeto foi desenvolvido para fins educacionais e de avaliação técnica.

---

## 🔗 Links Úteis

- [Databricks Community Edition](https://community.cloud.databricks.com/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Documentation](https://docs.databricks.com/)

---

## 📞 Suporte

Para dúvidas ou sugestões sobre o projeto, consulte a documentação dos notebooks ou entre em contato.
