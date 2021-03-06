DROP VIEW IF EXISTS `seowork.eldorado_views.v_semantics_queries`;

CREATE VIEW `seowork.eldorado_views.v_semantics_queries` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER()
    OVER(PARTITION BY date, project_id, search_engine, query_id ORDER BY created_at DESC) as state
    FROM `seowork.eldorado_source.semantics_queries`
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
    query_id,
    query_name,
    document_id,
    document_name,
    real_document,
    rel_document,
    category_id,
    category_name,
    avg_position,
    frequency1,
    frequency2,
    frequency3,
    potential_traffic,
FROM adapter
WHERE adapter.state = 1
ORDER BY date DESC, project_id, search_engine;
