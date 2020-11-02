#!/bin/bash
SG_BASEPATH=sg/sg-c19-response/geo-supplement
RDP_BASEPATH=rdp/recovery-data-partnership/geo_supplement

for INFO in $(mc ls --recursive --json $SG_BASEPATH)
do 
    (
        KEY=$(echo $INFO | jq -r '.key')
        DATE=$(echo $INFO | jq -r '.lastModified' | cut -c1-10)
        FILENAME=$(basename $KEY)
        PARTITION="dt=$DATE"
        
        echo $RDP_BASEPATH/$PARTITION/$FILENAME
        # Check existence
        STATUS=$(mc stat --json $RDP_BASEPATH/$PARTITION/$FILENAME | jq -r '.status')
        if [[ $FILENAME == *.csv.gz ]]; then
            case $STATUS in
            success)
                # If already synced, skip
                echo "$KEY is already synced, skipping ..."
            ;;
            error)
                # if not, create a sync ...
                mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$PARTITION/$FILENAME
            ;;
            esac
        else echo "ignore ..."
        fi
    ) &
done

wait
echo "Syncing New Complete !"