-- w_awg_bu_competitors_dashboard

DROP VIEW IF EXISTS `seowork.ozon_views.w_avg_bu_competitors_dashboard`;

CREATE VIEW `seowork.ozon_views.w_avg_bu_competitors_dashboard` AS


WITH vs_bu_projects_competitors as (
  SELECT bc.bu_id, bp.bu_name, bp.project_id, bc.competitor
  FROM `seowork.ozon_views.v_bu_projects` as bp, `seowork.ozon_views.v_bu_competitors` as bc
  WHERE bc.bu_id = bp.bu_id
),
vs_dashboard as (
  SELECT vs.bu_id, vs.bu_name, vs.project_id, vs.competitor, sd.date, sd.search_engine, sd.queries_count, sd.frequency2
  FROM `vs_bu_projects_competitors` as vs, `seowork.ozon_views.v_semantics_dashboard` as sd
  WHERE vs.project_id = sd.project_id
  AND sd.frequency2 > 0 AND sd.queries_count > 0
),
-- адаптер исходных данных, и защита от деления на 0
adapter as (
  SELECT vsd.*,
    COALESCE(cd.frequency2, 0) as frequency2_top10,
    COALESCE(cd.count, 0) as top10_count,
    COALESCE(cd.potential_traffic, 0) as potential_traffic_top10
  FROM `vs_dashboard` as vsd
  LEFT JOIN `seowork.ozon_views.v_competitors_dashboard` as cd USING(project_id, date, search_engine, competitor)
)

-- проверка на уникальные строки по ключевым полям (исключаем дублирующие ключевые значения)
, unique_keys AS (
    SELECT project_id, bu_id, search_engine, competitor, date, count(*) as `rows`
    FROM adapter
    GROUP BY project_id, bu_id, search_engine, competitor, date
    HAVING `rows` = 1
    ORDER BY bu_id, date DESC, project_id, search_engine
)

-- по каждому набору ключей выбираем кол-во проектов
, keys_with_projects AS (
    SELECT bu_id, search_engine, competitor, date, count(*) as projects
    FROM unique_keys
    GROUP BY bu_id, search_engine, competitor, date
    ORDER BY projects DESC, date DESC, bu_id, search_engine
)

-- максимальное кол-во проектов среди дат по каждому набору ключей
, max_projects_by_date AS (
    SELECT bu_id, search_engine, competitor, max(projects) as max_projects
    FROM keys_with_projects
    GROUP BY bu_id, search_engine, competitor
    ORDER BY bu_id, competitor, search_engine
)

-- наборы ключей, пригодные для использования по максимальному числу проектов
, useful_keys AS (
    SELECT kwp.*
    FROM keys_with_projects as kwp
    LEFT JOIN max_projects_by_date as mpd USING(bu_id, search_engine, competitor)
    WHERE kwp.projects = mpd.max_projects
    ORDER BY projects DESC
)

-- подготовленные данные для усреднения среди проектов
, prepared_data AS (
  SELECT uk.bu_id, uk.search_engine, uk.competitor, uk.date, project_id,
    bu_name,
    queries_count, frequency2, top10_count, frequency2_top10, potential_traffic_top10
  FROM useful_keys as uk
  RIGHT JOIN adapter USING (bu_id, search_engine, competitor, date )
  WHERE uk.bu_id IS NOT NULL AND uk.search_engine IS NOT NULL
    AND uk.competitor IS NOT NULL AND uk.date IS NOT NULL
  ORDER BY bu_id, search_engine, competitor, date
)

SELECT
    bu_id, bu_name, date, search_engine, competitor, count(*) as projects,
    ROUND(  100 *  sum(top10_count) / sum(queries_count), 2) as avg_top10_prc,
    ROUND(  100 * sum(frequency2_top10) / sum(frequency2), 2) as avg_ws2_top10_prc,
    ROUND( 1000 * sum(potential_traffic_top10) / sum(frequency2) , 2) as avg_p_traf_top10_prc,
FROM prepared_data
GROUP BY
    bu_id, date, search_engine, bu_name, competitor
ORDER BY
    date DESC, bu_id, competitor, search_engine
;
