SELECT
    host,
    ROUND(AVG(num_hits)) AS avg_hits
FROM 
    processed_events_aggregated_session
WHERE
    host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY
    host;
