DROP VIEW IF EXISTS `seowork-banki.seowork_views.v_competition_category`;

CREATE VIEW `seowork-banki.seowork_views.v_competition_category` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER()
    OVER(PARTITION BY date, project_id, search_engine, competitor, category_id ORDER BY created_at DESC) as state
    FROM `seowork.banki_source.competition_category`
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
    category_id,
    category_name,
    position_average,
    count,  
    count_percent, 
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
ORDER BY date DESC, project_id, search_engine;
