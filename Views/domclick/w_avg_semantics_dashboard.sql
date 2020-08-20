DROP VIEW IF EXISTS `seowork.domclick_views.w_avg_semantics_dashboard`;

CREATE VIEW `seowork.domclick_views.w_avg_semantics_dashboard` AS

WITH adapter AS (SELECT sd.project_id, project_name, project_description, 
      region_id, region_name, is_mobile, search_engine, date,
      queries_count, documents_count, frequency2,
      avg_position,
      top3_count, top5_count, top10_count, top100_count,
      top3_percent, top5_percent, top10_percent, top100_percent,
      frequency2_top10_percent, potential_traffic, potential_traffic_percent
    FROM `seowork.domclick_views.v_semantics_dashboard` as sd
)
   , unique_keys AS (
    SELECT project_id, search_engine, date, count(*) as `rows`
    FROM adapter
    GROUP BY project_id, search_engine, date
    HAVING `rows` = 1
    ORDER BY date DESC, project_id, search_engine)
    
    , keys_with_projects AS (
    SELECT search_engine, date, count(*) as projects
    FROM unique_keys
    GROUP BY search_engine, date
    HAVING projects > 85
    ORDER BY date DESC, search_engine)
    
 SELECT uk.search_engine, uk.date, project_id,
    region_id, region_name, is_mobile,
    queries_count, documents_count, frequency2,
    avg_position,
    top3_count, top3_percent, top5_count, top5_percent, 
    top10_count, top10_percent, top100_count, top100_percent,
    frequency2_top10_percent,
    potential_traffic, potential_traffic_percent
FROM keys_with_projects as uk
RIGHT JOIN adapter USING (search_engine, date )
WHERE uk.search_engine IS NOT NULL AND uk.date IS NOT NULL
ORDER BY project_id, search_engine, date
  
  


