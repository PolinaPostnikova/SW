SELECT

*,
frequency2 / frequency2_top30_count AS frequency2_top30_percent,
frequency2 / frequency2_top50_count AS frequency2_top50_percent


FROM

(SELECT 
q.document_id,
q.search_engine,
q.date,
q.frequency2_top30_count, 
q.frequency2_top50_count,

d.project_id,
d.project_name,
d.project_description,
d.region_id,
d.region_name,
d.is_mobile,
d.document_name,
d.category_id,
d.category_name,
d.queries_count,
d.avg_position,
d.frequency1,
d.frequency2,
d.frequency3,
d.top3_count,
d.top5_count,
d.top10_count,
d.top100_count,
d.top3_percent,
d.top5_percent,
d.top10_percent,
d.top100_percent,
d.frequency1_top10_percent,
d.frequency2_top10_percent,
d.frequency1_top10_count,
d.frequency2_top10_count,
d.potential_traffic,
d.potential_traffic_percent

FROM

(SELECT 

a.document_id,
a.search_engine,
a.date,
SUM (a.frequency2) AS frequency2_top30_count,

SUM (b.frequency2) AS frequency2_top50_count


FROM `seowork.ozon_views.v_semantics_queries` AS a

LEFT JOIN `seowork.ozon_views.v_semantics_queries` AS b USING (document_id, search_engine, date)

WHERE a.avg_position > 10 AND a.avg_position < 31 AND  b.avg_position > 30 AND b.avg_position < 51

GROUP BY document_id, search_engine, date) AS q

LEFT JOIN `seowork.ozon_views.v_semantics_documents` AS d USING(document_id, date, search_engine)
WHERE d.frequency2 > 0 AND frequency2_top30_count > 0 AND frequency2_top50_count > 0)
LIMIT 1000

