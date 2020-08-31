#!/bin/bash
source config.sh

DATES=$(psql -q -At $SAFEGRAPH -c "\copy (
     SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'social_distancing'
) to STDOUT CSV")

psql $SAFEGRAPH -f social_distancing/init_trips_by_state.sql

for DATE in $(echo $DATES)
do 
    max_bg_procs 1
    (
         psql $SAFEGRAPH -v DATE=$DATE -f social_distancing/trips_by_state.sql
    ) &
done;