DROP VIEW IF EXISTS `seowork.eldorado_views.vc_queries_categories`;
 
CREATE VIEW `seowork.eldorado_views.vc_queries_categories` AS
 
WITH dic_category AS (
    SELECT DISTINCT document_name, category_name FROM `seowork.eldorado_views.v_semantics_documents`
)
 
SELECT
 
vc.project_id,
vc.project_name,
vc.project_description,
vc.project_url,
vc.host_id,
vc.host_name,
vc.region_id,
vc.region_name,
vc.position_search_engine,
vc.position_is_mobile,
vc.position_queries_count,
vc.entity_id,
vc.NAME,
vc.query_frequency1,
vc.query_frequency2,
vc.query_frequency3,
vc.position_project_id,
vc.DATE,
vc.query_id,
IF((vc.POSITION = 0) IS NOT FALSE, 101, vc.POSITION) AS POSITION,
vc.document_name,
vc.is_zero,
vc.frequency1,
vc.frequency2,
vc.frequency3,
 
cat.category_name
 
FROM `seowork.eldorado_views.vc_position_by_queries` AS vc
LEFT JOIN dic_category AS cat USING(document_name)
