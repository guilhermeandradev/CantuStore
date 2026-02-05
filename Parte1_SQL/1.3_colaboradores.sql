-- ============================================
-- CANTUSTORE - PROVA SQL
-- Questão 1.3 - Colaboradores e Hierarquia
-- ============================================


WITH RECURSIVE 
-- CTE 1: Encontrar todos os chefes indiretos de cada funcionário
chefes_indiretos AS (
    -- Caso base: chefes diretos
    SELECT 
        c.id AS funcionario_id,
        c.lider_id AS chefe_id,
        c.salario AS funcionario_salario
    FROM colaboradores c
    WHERE c.lider_id IS NOT NULL
    
    UNION ALL
    
    -- Caso recursivo: chefes dos chefes
    SELECT 
        ci.funcionario_id,
        c.lider_id AS chefe_id,
        ci.funcionario_salario
    FROM chefes_indiretos ci
    INNER JOIN colaboradores c ON ci.chefe_id = c.id
    WHERE c.lider_id IS NOT NULL
),
-- CTE 2: Contar quantos chefes indiretos cada pessoa tem (profundidade na hierarquia)
contagem_chefes AS (
    SELECT 
        funcionario_id,
        COUNT(*) AS num_chefes_indiretos
    FROM chefes_indiretos
    GROUP BY funcionario_id
),
-- CTE 3: Filtrar chefes que ganham >= 2x o salário do funcionário
chefes_validos AS (
    SELECT 
        ci.funcionario_id,
        ci.chefe_id,
        ci.funcionario_salario,
        c.salario AS chefe_salario,
        COALESCE(cc.num_chefes_indiretos, 0) AS chefe_num_indiretos
    FROM chefes_indiretos ci
    INNER JOIN colaboradores c ON ci.chefe_id = c.id
    LEFT JOIN contagem_chefes cc ON ci.chefe_id = cc.funcionario_id
    WHERE c.salario >= ci.funcionario_salario * 2
),
-- CTE 4: Selecionar o chefe mais baixo na hierarquia (com mais chefes indiretos)
chefes_mais_baixos AS (
    SELECT 
        funcionario_id,
        chefe_id,
        chefe_num_indiretos,
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
    ON c.id = cmb.funcionario_id 
    AND cmb.rn = 1
ORDER BY c.id;

-- ============================================
-- EXPLICAÇÃO:
-- ============================================
-- "Mais baixo na hierarquia" = tem MAIS chefes indiretos
-- 
-- Hierarquia:
-- Marcos (0 chefes indiretos) - TOPO
--   └─ Leonardo (1 chefe indireto: Marcos)
--       ├─ Bruno (2 chefes: Leonardo, Marcos)
--       │   ├─ Helen (3 chefes: Bruno, Leonardo, Marcos)
--       │   └─ Wilian (3 chefes: Bruno, Leonardo, Marcos)
--       └─ Mateus (2 chefes: Leonardo, Marcos)
--           └─ Cinthia (3 chefes: Mateus, Leonardo, Marcos)
-- 
-- Para Helen (salário 1500, precisa chefe >= 3000):
--   Chefes válidos: Bruno(3000), Leonardo(4500), Marcos(10000)
--   Bruno tem 2 chefes indiretos, Leonardo tem 1, Marcos tem 0
--   Mais baixo = Bruno (tem mais chefes) → Resultado: 50
-- ============================================
