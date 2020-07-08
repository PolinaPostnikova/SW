DROP VIEW IF EXISTS `vseinstrumenti-seowork.vseinstrumenti_seowork_views.v_position_queries`;

CREATE VIEW `vseinstrumenti-seowork.vseinstrumenti_seowork_views.v_position_queries` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY created_at DESC, uploaded_at DESC) as state
    FROM `seowork.vseinstrumenti_source.position_queries`

)
SELECT
    id,
    position_project_id,
    query_id,
    name,
    cost,
    frequency1,
    frequency2,
    frequency3
FROM adapter
WHERE state = 1 AND is_active = true
ORDER BY query_id, position_project_id, id;
