DROP VIEW IF EXISTS `seowork.ozon_views.w_avg_bu_semantics_qieries`;

CREATE VIEW `seowork.ozon_views.w_avg_bu_semantics_qieries` AS
-- адаптер исходных данных, и защита от деления на 0
WITH adapter AS (
    SELECT sd.project_id, project_name, bu_id, bu_name, search_engine, date,
      query_id, query_name,
      frequency2, avg_position, avg_position, potential_traffic
    FROM `seowork.ozon_views.v_semantics_queries` as sd
    RIGHT JOIN `seowork.ozon_views.v_bu_projects` USING (project_id)
    WHERE frequency2 > 0 
)

-- проверка на уникальные строки по ключевым полям (исключаем дублирующие ключевые значения)
, unique_keys AS (
    SELECT project_id, bu_id, query_id, search_engine, date, count(*) as `rows`
    FROM adapter
    GROUP BY project_id, bu_id, query_id, search_engine, date
    HAVING `rows` = 1
    ORDER BY bu_id, query_id, date DESC, project_id, search_engine
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
    query_id, query_name, 
    bu_name, frequency2, avg_position, potential_traffic
  FROM useful_keys as uk
  RIGHT JOIN adapter USING (bu_id, search_engine, date, query_id )
  WHERE uk.bu_id IS NOT NULL AND uk.search_engine IS NOT NULL AND uk.date IS NOT NULL
  ORDER BY bu_id, search_engine, date
)

SELECT
    bu_id, bu_name, date, search_engine, count(*) as query_count,
    ROUND(  100 *   sum(avg_position) / query_count, 2) as avg_position_count,
    sum(query_count) as sum_queries_count,
    sum(frequency2) as sum_frequency2
FROM prepared_data
GROUP BY
    bu_id, date, search_engine, bu_name
ORDER BY
    date DESC, bu_id, search_engine
;
