-- CANTUSTORE - PROVA SQL
-- Questão 1.2 - Comissões


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


