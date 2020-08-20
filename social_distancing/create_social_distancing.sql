CREATE TEMP TABLE tmp (
    origin_census_block_group VARCHAR(12),
    date_range_start timestamp,
    date_range_end timestamp,
    device_count integer,
    distance_traveled_from_home text,
    bucketed_distance_traveled json,
    median_dwell_at_bucketed_distance_traveled text,
    completely_home_device_count integer,
    median_home_dwell_time integer,
    bucketed_home_dwell_time json,
    at_home_by_each_hour text,
    part_time_work_behavior_devices integer,
    full_time_work_behavior_devices integer,
    destination_cbgs json,
    delivery_behavior_devices integer,
    median_non_home_dwell_time integer,
    candidate_device_count integer,
    bucketed_away_from_home_time json,
    median_percentage_time_home integer,
    bucketed_percentage_time_home json
);

\COPY tmp FROM pstdin WITH NULL AS '' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS social_distancing.:"DATE";
SELECT 
    origin_census_block_group::VARCHAR(12),
    date_range_start::timestamp,
    date_range_end::timestamp,
    device_count::integer,
    nullif(distance_traveled_from_home, '')::integer as distance_traveled_from_home,
    bucketed_distance_traveled,
    median_dwell_at_bucketed_distance_traveled,
    completely_home_device_count,
    median_home_dwell_time,
    bucketed_home_dwell_time,
    at_home_by_each_hour,
    part_time_work_behavior_devices,
    full_time_work_behavior_devices,
    destination_cbgs::json,
    delivery_behavior_devices,
    median_non_home_dwell_time,
    candidate_device_count::integer,
    bucketed_away_from_home_time,
    median_percentage_time_home,
    bucketed_percentage_time_home,
    NULL::integer as mean_home_dwell_time,
    NULL::integer as mean_non_home_dwell_time,
    NULL::integer as mean_distance_traveled_from_home
INTO social_distancing.:"DATE"
FROM tmp;

CREATE INDEX :IDX ON social_distancing.:"DATE" (origin_census_block_group);