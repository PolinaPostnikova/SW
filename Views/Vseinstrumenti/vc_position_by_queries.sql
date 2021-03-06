DROP VIEW IF EXISTS `vseinstrumenti-seowork.vseinstrumenti_seowork_views.vc_position_by_queries`;

CREATE VIEW `vseinstrumenti-seowork.vseinstrumenti_seowork_views.vc_position_by_queries` AS

SELECT

pp.project_id,
pp.project_name,
pp.project_description,
pp.project_url,
pp.host_id,
pp.host_name,
pp.region_id,
pp.region_name,
pp.position_search_engine,
pp.position_is_mobile,
pp.position_queries_count,

pq.id as entity_id,
pq.name,
pq.cost,
pq.frequency1 as query_frequency1,
pq.frequency2 as query_frequency2,
pq.frequency3 as query_frequency3,

pd.position_project_id,
pd.date,
pd.query_id,
pd.position,
pd.is_zero,
pd.url,
ROUND(COALESCE(c.ctr, 0) * pd.frequency2, 2) as potential_traffic_query,
pd.frequency1,
pd.frequency2,
pd.frequency3

FROM `vseinstrumenti-seowork.vseinstrumenti_seowork_views.v_position_data` as pd
LEFT JOIN `vseinstrumenti-seowork.vseinstrumenti_seowork_views.v_position_queries` as pq USING (position_project_id, query_id)
LEFT JOIN `vseinstrumenti-seowork.vseinstrumenti_seowork_views.v_position_projects` as pp USING (position_project_id)
LEFT JOIN `seowork.vseinstrumenti_source.ctr_dict` as c ON c.position = pd.position AND c.search_engine = pp.position_search_engine
