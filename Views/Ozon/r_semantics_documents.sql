DROP VIEW IF EXISTS `seowork.ozon_views.r_semantics_documents`;

CREATE VIEW `seowork.ozon_views.r_semantics_documents` AS

SELECT

*,
ROUND(frequency2_top10_30_ratio / queries_count, 2) AS queries_top10_30_ratio

FROM

(SELECT

*,
ROUND(frequency2_top30_count / frequency2 * 100, 2) AS frequency2_top30_percent,
ROUND(frequency2_top50_count / frequency2 * 100, 2) AS frequency2_top50_percent,
ROUND((LOG10(frequency2_top30_count+1/(frequency2_top10_count+1))*10),2) AS frequency2_top10_30_ratio


FROM

(SELECT 
d.project_id,
d.project_name,
d.project_description,
d.region_id,
d.region_name,
d.is_mobile,
q.search_engine,
q.date,
q.document_id,
d.document_name,
d.category_id,
d.category_name,
d.queries_count,
d.avg_position,
d.frequency2,
d.frequency3,
d.top3_count,
d.top5_count,
d.top10_count,
d.top3_percent,
d.top5_percent,
d.top10_percent,
d.frequency2_top10_percent,
d.frequency2_top10_count,
d.potential_traffic,
d.potential_traffic_percent,
IF((q.frequency2_top30 IS NULL) IS NOT FALSE, 0, SUM (q.frequency2_top30)) as frequency2_top30_count,
IF((q.frequency2_top50 IS NULL) IS NOT FALSE, 0, SUM (q.frequency2_top50)) as frequency2_top50_count

FROM

(SELECT
  document_id,
  search_engine,
  date,
  avg_position,
  CASE
    WHEN avg_position > 10 AND avg_position < 31 
    THEN SUM (frequency2)
  END frequency2_top30,

  CASE
    WHEN avg_position > 30 AND avg_position < 51 
    THEN SUM (frequency2)
  END frequency2_top50

FROM `seowork.ozon_views.v_semantics_queries`
GROUP BY document_id, search_engine, date, avg_position) AS q

LEFT JOIN `seowork.ozon_views.v_semantics_documents` AS d USING(document_id, date, search_engine)
WHERE d.frequency2 > 0 
GROUP BY 
d.project_id,
d.project_name,
d.project_description,
d.region_id,
d.region_name,
d.is_mobile,
q.search_engine,
q.date,
q.document_id,
d.document_name,
d.category_id,
d.category_name,
d.queries_count,
d.avg_position,
d.frequency2,
d.frequency3,
d.top3_count,
d.top5_count,
d.top10_count,
d.top3_percent,
d.top5_percent,
d.top10_percent,
d.frequency2_top10_percent,
d.frequency2_top10_count,
d.potential_traffic,
d.potential_traffic_percent,
q.frequency2_top30,
q.frequency2_top50))

