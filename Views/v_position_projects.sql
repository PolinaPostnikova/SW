DROP VIEW IF EXISTS `vseinstrumenti-seowork.vseinstrumenti_seowork_views.v_position_projects`;

CREATE VIEW `vseinstrumenti-seowork.vseinstrumenti_seowork_views.v_position_projects` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_at DESC, uploaded_at DESC) as state
    FROM `seowork.vseinstrumenti_source.position_projects`

)
SELECT
    id as position_project_id,
    project_id,
    project_name,
    IF((project_description = '') IS NOT FALSE, project_name, project_description) as project_description,
    project_url,
    -- project_is_active,
    host_id,
    host_name,
    region_id,
    region_name,
    position_search_engine,
    position_is_mobile,
    -- position_is_active,
    position_queries_count
FROM adapter
WHERE state = 1 AND project_is_active = 1 AND position_is_active = 1
ORDER BY project_id, host_id, region_id, position_search_engine;
