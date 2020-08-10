DROP VIEW IF EXISTS `seowork.domclick_views.v_semantics_documents`;

CREATE VIEW `seowork.domclick_views.v_semantics_documents` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY date, project_id, search_engine, document_id ORDER BY created_at DESC) as state
    FROM `seowork.domclick_source.semantics_documents`
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
    document_id,
    document_name,
    category_id,
    category_name,
    queries_count,
    avg_position,
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
ORDER BY date DESC, project_id, search_engine, document_id;
