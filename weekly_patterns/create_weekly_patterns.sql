SET client_encoding = ‘UTF8’;

BEGIN;

CREATE TEMP TABLE tmp (
    safegraph_place_id text,
    location_name text,
    street_address text,
    city text,
    region text,
    postal_code varchar(5),
    iso_country_code varchar(2),
    safegraph_brand_ids text,
    brands text,
    date_range_start timestamp,
    date_range_end timestamp,
    raw_visit_counts int,
    raw_visitor_counts int,
    visits_by_day text,
    visits_by_each_hour text,
    poi_cbg varchar(12),
    visitor_home_cbgs json,
    visitor_daytime_cbgs json,
    visitor_country_of_origin json,
    distance_from_home int,
    median_dwell double precision,
    bucketed_dwell_times json,
    related_same_day_brand json,
    related_same_week_brand json,
    device_type json
);

\COPY tmp FROM pstdin WITH NULL AS '' DELIMITER ',' CSV HEADER;

INSERT INTO weekly_patterns.:"DATE" 
SELECT * FROM tmp;

COMMIT;