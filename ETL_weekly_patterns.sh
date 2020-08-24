#!/bin/bash
source config.sh

BASEPATH=sg/sg-c19-response/weekly-patterns-delivery/weekly/patterns
SUBPATH=$(
    for INFO in $(mc ls --recursive --json $BASEPATH)
    do 
        KEY=$(echo $INFO | jq -r '.key')
        # FILENAME=$(basename $KEY)
        SUBPATH=$(echo $KEY | cut -c-13)
        echo "$SUBPATH"
    done | sort -u
)

for SUB in $(echo $SUBPATH) 
do
    _DATE=$(echo $SUB | cut -c-10)
    DATE=$(python3 -c "print('$_DATE'.replace('/', '-'))")
    LOADED=$(psql -q -At $SAFEGRAPH -c "
            SELECT '$DATE' IN (
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'weekly_patterns'
            )")
    case $LOADED in
        f)
            psql $SAFEGRAPH -c "
            CREATE TABLE IF NOT EXISTS weekly_patterns.\"$DATE\" (
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
            );"
            for INFO in $(mc ls --recursive --json $BASEPATH/$SUB)
            do 
                (
                    KEY=$(echo $INFO | jq -r '.key')
                    FILENAME=$(basename $KEY)
                    if ! [ "$FILENAME" = "_SUCCESS" ]; then
                        CSVNAME=${FILENAME//.gz/}
                        mkdir -p tmp && (
                            cd tmp
                            mc cp $BASEPATH/$SUB/$KEY $FILENAME
                            gunzip -dc $FILENAME | 
                            psql $SAFEGRAPH \
                                -v ON_ERROR_STOP=1\
                                -v DATE=$DATE \
                                -f ../create_weekly_patterns.sql 
                        )
                    else echo "ignore ..."
                    fi
                ) &
            done
            wait
        ;;
        *) 
        echo "$DATE is already loaded!"
        ;;
    esac
    rm -rf tmp
done;