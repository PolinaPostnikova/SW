DROP VIEW IF EXISTS `ozontest.Client_6.v_position_data`;

CREATE VIEW `ozontest.Client_6.v_position_data` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY position_project_id, query_id, `date` ORDER BY created_at DESC, uploaded_at DESC) as state
    FROM `ozontest.Client_6.position_data`

)
SELECT
    position_project_id,
    date,
    query_id,
    position,
    is_zero,
    url,
    frequency1,
    frequency2,
    frequency3
FROM adapter
WHERE state = 1
ORDER BY `date` DESC, position_project_id, query_id;



-- -- критерий
-- SELECT position_project_id, date, count(*) as `rows`  FROM `ozontest.Client_6.v_position_data`
-- GROUP BY date, position_project_id ORDER BY `rows` DESC, date DESC, position_project_id;
