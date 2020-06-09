DROP VIEW IF EXISTS `seowork.ozon_views.v_competitors_dashboard`;

CREATE VIEW `seowork.ozon_views.v_competitors_dashboard` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER()
    OVER(PARTITION BY date, project_id, search_engine, competitor, top ORDER BY created_at DESC) as state
    FROM `seowork.ozon_source.competitors_dashboard`
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
    -- top,
    competitor,

    count,
    finished,
    count_percent,
    -- volume,
    -- volume_percent,
    frequency1,
    frequency2,
    frequency3,
    frequency1_percent,
    frequency2_percent,
    frequency3_percent,
    potential_traffic,
    potential_traffic_percent
FROM adapter
WHERE state = 1
ORDER BY date DESC, project_id, search_engine, competitor;
