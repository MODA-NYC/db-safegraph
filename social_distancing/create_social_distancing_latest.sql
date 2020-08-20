CREATE TEMP TABLE tmp (
    origin_census_block_group VARCHAR(12),
    date_range_start timestamp,
    date_range_end timestamp,
    device_count integer,
    distance_traveled_from_home integer,
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
    bucketed_percentage_time_home json,
    mean_home_dwell_time integer,
    mean_non_home_dwell_time integer,
    mean_distance_traveled_from_home integer
);

\COPY tmp FROM pstdin WITH NULL AS '' DELIMITER ',' CSV HEADER;

SELECT * INTO social_distancing.:"DATE" FROM tmp;

CREATE INDEX :IDX ON social_distancing.:"DATE" (origin_census_block_group);