#!/bin/bash
SG_BASEPATH_BACKFILL=sg/sg-c19-response/weekly-patterns-delivery-2020-12-backfill/release-2021-07/weekly/home_panel_summary_backfill
SG_BASEPATH=sg/sg-c19-response/weekly-patterns-delivery-2020-12/release-2021-07/weekly/home_panel_summary
RDP_BASEPATH=rdp/recovery-data-partnership/home_panel_summary_202107

function max_bg_procs {
    if [[ $# -eq 0 ]] ; then
            echo "Usage: max_bg_procs NUM_PROCS.  Will wait until the number of background (&)"
            echo "           bash processes (as determined by 'jobs -pr') falls below NUM_PROCS"
            return
    fi
    local max_number=$((0 + ${1:-0}))
    while true; do
            local current_number=$(jobs -pr | wc -l)
            if [[ $current_number -lt $max_number ]]; then
                    break
            fi
            sleep 1
    done
}

for INFO in $(mc ls --recursive --json $SG_BASEPATH_BACKFILL)
do 
    max_bg_procs 5
    (
        KEY=$(echo $INFO | jq -r '.key')
        FILENAME=$(basename $KEY)
        PARENT=$(dirname $KEY)
        NEW_KEY=$(python3 -c "print('$PARENT'[-10:].replace('/', '-'))")
        # DATE=$(echo $FILENAME | cut -c1-10)
        # PARTITION="dt=$DATE"
        # SUBPATH=$(echo $KEY | cut -c-13)
        echo "PARENT: " $PARENT
        echo "NEW_KEY: " $NEW_KEY

        if [ "${FILENAME#*.}" = "csv" ]; then

            # Check existence
            # STATUS=$(mc stat --json $RDP_BASEPATH/$PARTITION/$FILENAME | jq -r '.status')
            STATUS=$(mc stat --json $RDP_BASEPATH/$NEW_KEY-$FILENAME | jq -r '.status')
            
            case $STATUS in
            success)
                # echo "$KEY is already synced to $PARTITION/$FILENAME, skipping ..."
                echo "$KEY is already synced to $NEW_KEY-$FILENAME, skipping ..."
            ;;
            error)
                # Download data and unzip, remove README.txt and the original .zip file
                # mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$PARTITION/$FILENAME
                mc cp $SG_BASEPATH_BACKFILL/$KEY $RDP_BASEPATH/$NEW_KEY-$FILENAME
            ;;
            esac
        else echo "ignore $NEW_KEY-$FILENAME"
        fi
    ) &
done
wait
echo "Syncing Backfill Complete!"

for INFO in $(mc ls --recursive --json $SG_BASEPATH)
do 
    max_bg_procs 5
    (
        KEY=$(echo $INFO | jq -r '.key')
        FILENAME=$(basename $KEY)
        PARENT=$(dirname $KEY)
        NEW_KEY=$(python3 -c "print('$PARENT'[-13:-3].replace('/', '-'))")
        # DATE=$(echo $NEW_KEY | cut -c1-10)
        # PARTITION="dt=$DATE"
        # SUBPATH=$(echo $KEY | cut -c-13)
        echo "PARENT: " $PARENT
        echo "NEW_KEY: " $NEW_KEY

        if  [ "${FILENAME#*.}" = "csv" ]; then

            # Check existence
            # STATUS=$(mc stat --json $RDP_BASEPATH/$PARTITION/$NEW_KEY | jq -r '.status')
            STATUS=$(mc stat --json $RDP_BASEPATH/$NEW_KEY-$FILENAME | jq -r '.status')
            
            case $STATUS in
            success)
                # echo "$KEY is already synced to $PARTITION/$NEW_KEY, skipping ..."
                echo "$KEY is already synced to $NEW_KEY-$FILENAME, skipping ..."
            ;;
            error)
                # Download data and unzip, remove README.txt and the original .zip file
                # mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$PARTITION/$NEW_KEY
                mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$NEW_KEY-$FILENAME
            ;;
            esac
        else echo "ignore $NEW_KEY-$FILENAME"
        fi
    ) &
done

wait
echo "Syncing New Complete !"