DROP VIEW IF EXISTS `seowork.ozon_views.w_avg_bu_semantics_dashboard`;

CREATE VIEW `seowork.ozon_views.w_avg_bu_semantics_dashboard` AS
-- адаптер исходных данных, и защита от деления на 0
WITH adapter AS (
    SELECT sd.project_id, project_name, bu_id, bu_name, search_engine, date,
      queries_count, documents_count, frequency2,
      top3_count, top5_count, top10_count, top100_count,
      frequency2_top10_count, potential_traffic
    FROM `seowork.ozon_views.v_semantics_dashboard` as sd
    RIGHT JOIN `seowork.ozon_views.v_bu_projects` USING (project_id)
    WHERE frequency2 > 0 AND queries_count > 0
)

-- проверка на уникальные строки по ключевым полям (исключаем дублирующие ключевые значения)
, unique_keys AS (
    SELECT project_id, bu_id, search_engine, date, count(*) as `rows`
    FROM adapter
    GROUP BY project_id, bu_id, search_engine, date
    HAVING `rows` = 1
    ORDER BY bu_id, date DESC, project_id, search_engine
)

-- по каждому набору ключей выбираем кол-во проектов
, keys_with_projects AS (
    SELECT bu_id, search_engine, date, count(*) as projects
    FROM unique_keys
    GROUP BY bu_id, search_engine, date
    ORDER BY projects DESC, date DESC, bu_id, search_engine
)

-- максимальное кол-во проектов среди дат по каждому набору ключей
, max_projects_by_date AS (
    SELECT bu_id, search_engine, max(projects) as max_projects
    FROM keys_with_projects
    GROUP BY bu_id, search_engine
    ORDER BY bu_id, search_engine
)

-- наборы ключей, пригодные для использования по максимальному числу проектов
, useful_keys AS (
    SELECT kwp.*
    FROM keys_with_projects as kwp
    LEFT JOIN max_projects_by_date as mpd USING(bu_id, search_engine)
    WHERE kwp.projects = mpd.max_projects
    ORDER BY projects DESC
)

-- подготовленные данные для усреднения среди проектов
, prepared_data AS (
  SELECT uk.bu_id, uk.search_engine, uk.date, project_id,
    bu_name,
    queries_count, documents_count, frequency2,
    top3_count, top5_count, top10_count, top100_count,
    frequency2_top10_count, potential_traffic
  FROM useful_keys as uk
  RIGHT JOIN adapter USING (bu_id, search_engine, date )
  WHERE uk.bu_id IS NOT NULL AND uk.search_engine IS NOT NULL AND uk.date IS NOT NULL
  ORDER BY bu_id, search_engine, date
)

SELECT
    bu_id, bu_name, date, search_engine, count(*) as projects,

    ROUND(  100 *   sum(top3_count) / sum(queries_count), 2) as avg_top3_prc,
    ROUND(  100 *   sum(top5_count) / sum(queries_count), 2) as avg_top5_prc,
    ROUND(  100 *  sum(top10_count) / sum(queries_count), 2) as avg_top10_prc,
    ROUND(  100 * sum(top100_count) / sum(queries_count), 2) as avg_top100_prc,
    ROUND(  100 * sum(frequency2_top10_count) / sum(frequency2), 2) as avg_ws2_top10_prc,
    ROUND( 1000 * sum(potential_traffic) / sum(frequency2) , 2) as avg_p_traf_prc,
    sum(queries_count) as sum_queries_count,
    sum(documents_count) as sum_documents_count,
    sum(frequency2) as sum_frequency2
FROM prepared_data
GROUP BY
    bu_id, date, search_engine, bu_name
ORDER BY
    date DESC, bu_id, search_engine
;
