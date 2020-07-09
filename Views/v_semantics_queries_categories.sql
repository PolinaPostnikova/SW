-- присоединение таблиц

DROP VIEW IF EXISTS `raiffeisen-owox.SEO_Ashmanov.v_semantics_queries_categories`;
 
CREATE VIEW `raiffeisen-owox.SEO_Ashmanov.v_semantics_queries_categories` AS
 
SELECT 
    a.project_id,
    a.project_description,
    a.date,
    a.search_engine,
    a.region_id,
    a.region_name,
    a.is_mobile,
    a.query_name,
    a.document_id,
    a.document_name,
    a.real_document,
    a.rel_document,
    a.avg_position,
    a.frequency1,
    a.frequency2,
    a.frequency3,
    a.potential_traffic,
    b.category_id,
    b.category_name
FROM `raiffeisen-owox.SEO_Ashmanov.semantics_queries` as a
LEFT JOIN `raiffeisen-owox.SEO_Ashmanov.dict_query_category` as b USING (query_id, query_name)
