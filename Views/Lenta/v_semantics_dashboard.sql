DROP VIEW IF EXISTS `seowork.lenta_views.v_semantics_dashboard`;

CREATE VIEW `seowork.lenta_views.v_semantics_dashboard` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER()
    OVER(PARTITION BY date, project_id, search_engine ORDER BY created_at DESC) as state
    FROM `seowork.lenta_source.semantics_dashboard`
    WHERE queries_count IS NOT NULL AND frequency2 > 0 AND queries_count > 0 AND frequency2_top10_percent > 1 AND potential_traffic_percent > 1 AND top3_percent > 1 AND top5_percent > 1
),

delta_adapter AS (
  SELECT *,
    round(LAG(top3_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - top3_percent) as top3_percent_diff,
    round((LAG(top3_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - top3_percent) / top3_percent * 100) as top3_percent_diff_percent,
    round(LAG(top5_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - top5_percent) as top5_percent_diff,
    round((LAG(top5_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - top5_percent) / top5_percent * 100) as top5_percent_diff_percent,
    round(LAG(top10_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - top10_percent) as top10_percent_diff,
    round((LAG(top10_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - top10_percent) / top10_percent * 100) as top10_percent_diff_percent,
    round(LAG(frequency2_top10_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - frequency2_top10_percent) as frequency2_top10_percent_diff,
    round((LAG(frequency2_top10_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - frequency2_top10_percent) / frequency2_top10_percent * 100) as frequency2_top10_percent_diff_percent,
    round(LAG(potential_traffic_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - potential_traffic_percent) as potential_traffic_percent_diff,
    round((LAG(potential_traffic_percent) OVER(PARTITION BY project_id, search_engine ORDER BY date) - potential_traffic_percent) / potential_traffic_percent * 100) as potential_traffic_percent_diff_percent 
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
    group_count,
    queries_groups_count, 
    queries_count, 
    documents_count, 
    categories_count,
    avg_position,
    frequency1,
    frequency2,
    frequency3,
    top3_count, 
    top5_count, 
    top10_count, 
    top100_count, 
    top3_percent,
    top3_percent_diff,
    top3_percent_diff_percent,
    top5_percent,
    top5_percent_diff,
    top5_percent_diff_percent,
    top10_percent,
    top10_percent_diff,
    top10_percent_diff_percent,
    top100_percent,
    frequency1_top10_percent,
    frequency2_top10_percent,
    frequency2_top10_percent_diff,
    frequency2_top10_percent_diff_percent,
    frequency1_top10_count,
    frequency2_top10_count,
    potential_traffic,
    potential_traffic_percent,
    potential_traffic_percent_diff,
    potential_traffic_percent_diff_percent
FROM delta_adapter
WHERE state = 1
ORDER BY date DESC, project_id, search_engine;
