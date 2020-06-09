DROP VIEW IF EXISTS `seowork.ozon_views.v_semantics_category`;

CREATE VIEW `seowork.ozon_views.v_semantics_category` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY date, project_id, search_engine, category_id ORDER BY created_at DESC) as state
    FROM `seowork.ozon_source.semantics_category`
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
    category_id,
    category_name,
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
ORDER BY date DESC, project_id, search_engine, category_id;



-- -- критерий
-- SELECT project_id, date, search_engine, category_id, count(*) as `rows`  FROM `seowork.ozon_views.v_semantics_category`
-- GROUP BY project_id, date, search_engine, category_id ORDER BY `rows` DESC, date DESC;
