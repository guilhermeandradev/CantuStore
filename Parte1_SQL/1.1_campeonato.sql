-- CANTUSTORE - PROVA SQL
-- Questão 1.1 - Campeonato


WITH pontos_mandante AS (
    SELECT 
        mandante_time AS time_id,
        CASE 
            WHEN mandante_gols > visitante_gols THEN 3  -- Vitória
            WHEN mandante_gols = visitante_gols THEN 1  -- Empate
            ELSE 0  -- Derrota
        END AS pontos
    FROM jogos
),
pontos_visitante AS (
    SELECT 
        visitante_time AS time_id,
        CASE 
            WHEN visitante_gols > mandante_gols THEN 3  -- Vitória
            WHEN visitante_gols = mandante_gols THEN 1  -- Empate
            ELSE 0  -- Derrota
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
