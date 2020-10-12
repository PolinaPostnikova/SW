DROP VIEW IF EXISTS `seowork.iledebeaute_views.v_traffic_segments_google_analytics`;

CREATE VIEW `seowork.iledebeaute_views.v_traffic_segments_google_analytics` AS

WITH adapter AS (
    SELECT *, ROW_NUMBER()
    OVER(PARTITION BY date, project_id, segment_id, source_id, host_id ORDER BY uploaded_at DESC) as state
    FROM `seowork.iledebeaute_source.traffic_segments_google_analytics`
)
SELECT
    project_id,
    project_name,
    IF((project_description = '') IS NOT FALSE, project_name, project_description) as project_description,
    date,
    project_is_mobile,
    segment_id,
    segment_mask,
    segment_name ,
    host_id, 
    host_name, 
    source_id, 
    source_medium, 
    source_source, 
    device_category, 
    sessions, 
    avg_time_on_page, 
    page_views, 
    bounce_rate, 
    uploaded_at
FROM adapter
WHERE state = 1
ORDER BY date DESC, project_id, segment_id;
