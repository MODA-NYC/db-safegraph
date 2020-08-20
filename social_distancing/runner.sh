#!/bin/bash
source config.sh

BASEPATH=sg/sg-c19-response/social-distancing/v2/
for INFO in $(mc ls --recursive --json $BASEPATH)
do
    max_bg_procs 15
    (
        # Extract file path from json response
        KEY=$(echo $INFO | jq -r '.key') 

        # Extract file name from file path
        FILENAME=$(basename $KEY)

        # Take first 10 characters of file name as date
        DATE=$(echo $FILENAME | cut -c1-10)
        DATEIDX=${DATE//-/}
        # Check if the data for this date is already loaded
        LOADED=$(psql -q -At $SAFEGRAPH -c "
            SELECT '$DATE' IN (
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'social_distancing'
            )")
        
        case $LOADED in
            f) # If not loaded, then load into database
                echo "Loading $FILENAME right now ..."
                mc cp $BASEPATH/$KEY $FILENAME
                {
                    # Try new schema first
                    gzip -dc $FILENAME | 
                        psql $SAFEGRAPH \
                            -v ON_ERROR_STOP=1\
                            -v IDX="destination_cbgs_idx_$DATEIDX"\
                            -v DATE=$DATE \
                            -f create_social_distancing_latest.sql
                } || {
                    # If failed, then try old schema
                    gzip -dc $FILENAME | 
                        psql $SAFEGRAPH \
                            -v ON_ERROR_STOP=1\
                            -v IDX="destination_cbgs_idx_$DATEIDX"\
                            -v DATE=$DATE \
                            -f create_social_distancing.sql
                }
                rm $FILENAME
            ;;
            *) # Otherwise, print the following message:
                echo "$FILENAME is already loaded!"
            ;;
        esac
    ) &
done;

wait
echo "import complete!"