DROP VIEW IF EXISTS `seowork.ozon_views.v_semantics_dashboard`;

CREATE VIEW `seowork.ozon_views.v_semantics_dashboard` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY date, project_id, search_engine ORDER BY created_at DESC) as state
    FROM `seowork.ozon_source.semantics_dashboard`
    WHERE queries_count IS NOT NULL
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
FROM adapter
WHERE state = 1
ORDER BY date DESC, project_id, search_engine;



-- -- критерий
-- SELECT project_id, date, search_engine, count(*) as `rows`  FROM `seowork.ozon_views.v_semantics_dashboard`
-- GROUP BY project_id, date, search_engine ORDER BY `rows` DESC, date DESC;
