DROP TABLE IF EXISTS devices_deduplicated;
-- --------------------------------------------------------------------------------------
CREATE TABLE devices_deduplicated AS (
    WITH duplicate_identified AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device_id, browser_type, os_type) AS duplicate_id
        FROM
            devices
    )
    -- ------------------------------
    SELECT
        device_id,
        browser_type,
        browser_version_major,
        browser_version_minor,
        browser_version_patch,
        device_type,
        device_version_major,
        device_version_minor,
        device_version_patch,
        os_type,
        os_version_major,
        os_version_minor,
        os_version_patch
    FROM
        duplicate_identified
    WHERE
        duplicate_id = 1
);