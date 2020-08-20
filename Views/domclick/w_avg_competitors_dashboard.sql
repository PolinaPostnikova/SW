DROP VIEW IF EXISTS `seowork.domclick_views.w_avg_competitors_dashboard`;

CREATE VIEW `seowork.domclick_views.w_avg_competitors_dashboard` AS

WITH adapter AS (SELECT sd.project_id, project_name, project_description, 
      region_id, region_name, is_mobile, search_engine, date, competitor,
      competitor_new, count, count_percent, frequency2,
      frequency2_percent, potential_traffic, potential_traffic_percent
    FROM `seowork.domclick_views.v_competitors_dashboard` as sd
)
   , unique_keys AS (
    SELECT project_id, competitor, search_engine, date, count(*) as `rows`
    FROM adapter
    GROUP BY project_id, competitor, search_engine, date
    HAVING `rows` = 1
    ORDER BY date DESC, project_id, competitor, search_engine)
    
    , keys_with_projects AS (
    SELECT search_engine, date, count(*) as projects
    FROM unique_keys
    GROUP BY search_engine, date
    HAVING projects > 500
    ORDER BY date DESC, search_engine)
    
 SELECT uk.search_engine, uk.date, project_id,
    region_id, region_name, is_mobile, competitor,
     competitor_new, count, count_percent, frequency2,
     frequency2_percent, potential_traffic, potential_traffic_percent
FROM keys_with_projects as uk
RIGHT JOIN adapter USING (search_engine, date )
WHERE uk.search_engine IS NOT NULL AND uk.date IS NOT NULL
ORDER BY project_id, search_engine, date
