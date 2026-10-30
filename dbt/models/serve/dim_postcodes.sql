{{ config(materialized='table') }}

WITH bua_names AS (
    SELECT
        bua24cd,
        MAX(bua24nm) AS bua24nm
    FROM {{ source('ingest', 'onspd_bua_names') }}
    GROUP BY bua24cd
),

lad_names AS (
    SELECT
        lad25cd,
        MAX(lad25nm) AS lad25nm
    FROM {{ source('ingest', 'onspd_lad_names') }}
    GROUP BY lad25cd
)

SELECT
    pcds,
    dointr,
    doterm,
    east1m,
    north1m,
    ctry25cd,
    o.lad25cd,
    o.bua24cd,
    lad25nm,
    bua24nm,
    pa.postcode_area,
    IF(lat < 90.0, lat, NULL) AS lat,
    IF(lat < 90.0, long, NULL) AS long,
    COALESCE(bua24nm, lad25nm, 'SQL Town') AS city_town,
    SPLIT(pcds, ' ')[OFFSET(0)] AS postcode_district,
    CASE ctry25cd
        WHEN 'E92000001' THEN 'England'
        WHEN 'S92000003' THEN 'Scotland'
        WHEN 'W92000004' THEN 'Wales'
        WHEN 'N92000002' THEN 'Northern Ireland'
        WHEN 'L93000001' THEN 'Channel Islands'
        WHEN 'M83000003' THEN 'Isle of Man'
    END AS country
FROM {{ source('ingest', 'ONSPD') }} AS o
LEFT JOIN bua_names AS b ON o.bua24cd = b.bua24cd
LEFT JOIN lad_names AS l ON o.lad25cd = l.lad25cd
LEFT JOIN {{ source('ingest', 'onspd_postcode_areas') }} AS pa
    ON pa.area_code = REGEXP_EXTRACT(o.pcds, r'^[A-Z]+')
WHERE
    doterm IS NULL
    AND pcds <> 'GIR 0AA'
    -- UK geographical mainland bounding box:
    -- Lat: 49.5°N (South of Lizard Point, Cornwall ~49.96°N) to 61.0°N (North of Shetland ~60.86°N)
    -- Long: -11.0°W (West of Northern Ireland ~8.18°W) to 2.5°E (East of Lowestoft Ness, Suffolk ~1.76°E)
    -- Excludes: Channel Islands (Jersey/Guernsey), Isle of Man (IKEA UK does not deliver there),
    -- and records with missing/null ONS coordinates
    AND lat IS NOT NULL
    AND long IS NOT NULL
    AND lat BETWEEN 49.5 AND 61.0
    AND long BETWEEN -11.0 AND 2.5
