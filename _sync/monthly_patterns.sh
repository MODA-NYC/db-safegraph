#!/bin/bash
SG_BASEPATH_BACKFILL=sg/sg-c19-response/monthly-patterns-2020-12/patterns_backfill
SG_BASEPATH=sg/sg-c19-response/monthly-patterns-2020-12/patterns
RDP_BASEPATH=rdp/recovery-data-partnership/monthly_patterns_new

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
        # NEW_KEY=$(python3 -c "print('$KEY'.replace('/', '-'))")
        FILENAME=$(basename $KEY)
        # DATE=$(echo $FILENAME | cut -c1-10)
        # PARTITION="dt=$DATE"
        # SUBPATH=$(echo $KEY | cut -c-13)
        PARENT=$(dirname $KEY)
        YEARMONTH=$(python3 -c "print('$PARENT'[-7:].replace('/',''))")
        echo "KEY: " $KEY
        echo "PARENT: " $PARENT
        echo "YEARMONTH: " $YEARMONTH
        
        # 3 months are duplicated between backfill and new data
        if [ $YEARMONTH != '202011' ] && [ $YEARMONTH != '202012' ] && [ $YEARMONTH != '202101' ]; then
            if ! [ "$FILENAME" = "_SUCCESS" ]; then

                # Check existence
                # STATUS=$(mc stat --json $RDP_BASEPATH/$PARTITION/$NEW_KEY | jq -r '.status')
                STATUS=$(mc stat --json $RDP_BASEPATH/$YEARMONTH-BF-$FILENAME | jq -r '.status')
                
                case $STATUS in
                success)
                    echo "$KEY is already synced to $YEARMONTH-BF-$FILENAME, skipping ..."
                ;;
                error)
                    # Download data and unzip, remove README.txt and the original .zip file
                    mc cp $SG_BASEPATH_BACKFILL/$KEY $RDP_BASEPATH/$YEARMONTH-BF-$FILENAME
                ;;
                esac
            else echo "ignore _SUCCESS"
            fi
        fi
    ) &
done
wait
echo "Syncing Backfill Complete !"

for INFO in $(mc ls --recursive --json $SG_BASEPATH)
do 
    max_bg_procs 5
    (
        KEY=$(echo $INFO | jq -r '.key')
        # NEW_KEY=$(python3 -c "print('$KEY'.replace('/', '-'))")
        FILENAME=$(basename $KEY)
        # SUBPATH=$(echo $KEY | cut -c-13)
        PARENT=$(dirname $KEY)
        GPARENT=$(dirname $PARENT)
        GGPARENT=$(dirname $GPARENT)
        PREFIX=${GGPARENT////}
        echo "KEY: " $KEY
        echo "PARENT: " $PARENT
        echo "GPARENT: " $GPARENT
        echo "GGPARENT: " $GGPARENT
        echo "PREFIX: " $PREFIX

        if ! [ "$FILENAME" = "_SUCCESS" ]; then

            # Check existence
            STATUS=$(mc stat --json $RDP_BASEPATH/$PREFIX-$FILENAME | jq -r '.status')
            
            case $STATUS in
            success)
                echo "$KEY is already synced to $PREFIX-$FILENAME, skipping ..."
            ;;
            error)
                # Download data and unzip, remove README.txt and the original .zip file
                mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$PREFIX-$FILENAME
            ;;
            esac
        else echo "ignore _SUCCESS"
        fi
    ) &
done

wait
echo "Syncing New Complete !"