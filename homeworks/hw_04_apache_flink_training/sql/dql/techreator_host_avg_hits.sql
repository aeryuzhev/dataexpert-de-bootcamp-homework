SELECT
    host,
    ROUND(AVG(num_hits)) AS avg_hits
FROM 
    processed_events_aggregated_session
WHERE
    host LIKE '%techcreator.io'
GROUP BY
    host;
