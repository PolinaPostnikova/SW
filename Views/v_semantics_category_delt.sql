DROP VIEW IF EXISTS `raiffeisen-owox.SEO_Ashmanov.v_semantics_category`;

CREATE VIEW `raiffeisen-owox.SEO_Ashmanov.v_semantics_category` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY date, project_id, search_engine, category_id ORDER BY created_at DESC) as state
    FROM `raiffeisen-owox.SEO_Ashmanov.semantics_category`
    WHERE queries_count IS NOT NULL
),
delta_adapter AS (
  SELECT *,
    LAG(avg_position) OVER(PARTITION BY project_id, search_engine, category_id ORDER BY date) - avg_position as avg_position_diff,
    round((LAG(avg_position) OVER(PARTITION BY project_id, search_engine, category_id ORDER BY date) - avg_position) / avg_position * 100) as avg_position_diff_percent
  FROM adapter
  WHERE state = 1
)

SELECT
    project_id,
    project_name,
    IF((project_description = '') IS NOT FALSE, project_name, project_description) as project_description,
    date,
    search_engine,
    region_id,
    region_name,
    is_mobile,
    category_id,
    category_name,
    queries_groups_count,
    queries_count,
    documents_count,
    avg_position,
    avg_position_diff,
    avg_position_diff_percent,
    frequency1,
    frequency2,
    frequency3,
    top3_count,
    top5_count,
    top10_count,
    top100_count,
    top3_percent,
    top5_percent,
    top10_percent,
    top100_percent,
    frequency1_top10_percent,
    frequency2_top10_percent,
    frequency1_top10_count,
    frequency2_top10_count,
    potential_traffic,
    potential_traffic_percent,
    worth,
    worth_max,
    worth_reserve,
    worth_percent
FROM delta_adapter
WHERE state = 1
ORDER BY date DESC, project_id, search_engine, category_id;
