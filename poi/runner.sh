#!/bin/bash
source config.sh

BASEPATH=sg/sg-c19-response/weekly-patterns-delivery/weekly/patterns/
for INFO in $(mc ls --recursive --json $BASEPATH)
do
    max_bg_procs 15
    (
        # Extract file path from json response
        KEY=$(echo $INFO | jq -r '.key') 

        # Extract file name from file path
        FILENAME=$(basename $KEY)

        # Take first 10 characters of file path as date
        DATE=$(echo $KEY | cut -c-10)


        DATEIDX=${DATE//-/}
        # Check if the data for this date is already loaded
        LOADED=$(psql -q -At $SAFEGRAPH -c "
            SELECT '$DATE' IN (
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'poi'
            )")
        
        case $LOADED in
            f) # If not loaded, then load into database
                echo "Not loaded"
                echo "Loading $FILENAME right now ..."
                mc cp $BASEPATH$KEY $FILENAME
                {
                    # Try new schema first
                    gzip -dc $FILENAME | 
                    psql $SAFEGRAPH \
                            -v ON_ERROR_STOP=1\
                            -v IDX="safegraph_place_id_idx_$DATEIDX"\
                            -v DATE=$DATE \
                            -f poi/create_poi.sql
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