DROP VIEW IF EXISTS `seowork.lenta_views.v_competitors_dashboard`;

CREATE VIEW `seowork.lenta_views.v_competitors_dashboard` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER()
    OVER(PARTITION BY date, project_id, search_engine, competitor ORDER BY created_at DESC) as state
    FROM `seowork.lenta_source.competitors_dashboard`
    WHERE frequency2 > 0 AND frequency2_percent > 1 AND count_percent > 1 AND potential_traffic_percent > 1 
),

delta_adapter AS (
  SELECT *,
    round(LAG(count_percent) OVER(PARTITION BY project_id, search_engine, competitor ORDER BY date) - count_percent) as count_percent_diff,
    round((LAG(count_percent) OVER(PARTITION BY project_id, search_engine, competitor ORDER BY date) - count_percent) / count_percent * 100) as count_percent_diff_percent,
    round(LAG(frequency2_percent) OVER(PARTITION BY project_id, search_engine, competitor ORDER BY date) - frequency2_percent) as frequency2_percent_diff,
    round((LAG(frequency2_percent) OVER(PARTITION BY project_id, search_engine, competitor ORDER BY date) - frequency2_percent) / frequency2_percent * 100) as frequency2_percent_diff_percent,
    round(LAG(potential_traffic_percent) OVER(PARTITION BY project_id, search_engine, competitor ORDER BY date) - potential_traffic_percent) as potential_traffic_percent_diff,
    round((LAG(potential_traffic_percent) OVER(PARTITION BY project_id, search_engine, competitor ORDER BY date) - potential_traffic_percent) / potential_traffic_percent * 100) as potential_traffic_percent_diff_percent 
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
    top, 
    competitor, 
    count, 
    finished, 
    count_percent, 
    count_percent_diff,
    count_percent_diff_percent,
    volume, 
    volume_percent
    frequency1,
    frequency2,
    frequency3,
    frequency1_percent, 
    frequency2_percent,
    frequency2_percent_diff,
    frequency2_percent_diff_percent,
    frequency3_percent, 
    potential_traffic, 
    potential_traffic_percent,
    potential_traffic_percent_diff,
    potential_traffic_percent_diff_percent
FROM delta_adapter
WHERE state = 1
ORDER BY date DESC, project_id, search_engine;
