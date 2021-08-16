#!/bin/bash
SG_BASEPATH_BACKFILL=sg/sg-c19-response/weekly-patterns-delivery-2020-12/release-2021-07/weekly/patterns_backfill
SG_BASEPATH=sg/sg-c19-response/weekly-patterns-delivery-2020-12/release-2021-07/weekly/patterns
RDP_BASEPATH=rdp/recovery-data-partnership/weekly_patterns_202107

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

#### UNCOMMENT THE BACK FILL SECTION IF BACKFILL CHANGED ####

# for INFO in $(mc ls --recursive --json $SG_BASEPATH_BACKFILL)
# do 
#     max_bg_procs 5
#     (
#         KEY=$(echo $INFO | jq -r '.key')
#         NEW_KEY=$(python3 -c "print('$KEY'[14:].replace('/', '-'))")
#         YEARMONTH=$(python3 -c "print('$NEW_KEY'[:7])")
#         FILENAME=$(basename $KEY)
#         # DATE=$(echo $FILENAME | cut -c1-10)
#         # PARTITION="dt=$DATE"
#         # SUBPATH=$(echo $KEY | cut -c-13)
        
#         if [ "${FILENAME#*.}" = "csv.gz" ] && [ $YEARMONTH != '2018-01' ]; then
            
#             # Check existence
#             # STATUS=$(mc stat --json $RDP_BASEPATH/$PARTITION/$NEW_KEY | jq -r '.status')
#             STATUS=$(mc stat --json $RDP_BASEPATH/$NEW_KEY | jq -r '.status')
            
#             case $STATUS in
#             success)
#                 # echo "$KEY is already synced to $PARTITION/$NEW_KEY, skipping ..."
#                 echo "$KEY is already synced to $NEW_KEY, skipping ..."
#             ;;
#             error)
#                 # Download data and unzip, remove README.txt and the original .zip file
#                 # mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$PARTITION/$NEW_KEY
#                 mc cp $SG_BASEPATH_BACKFILL/$KEY $RDP_BASEPATH/$NEW_KEY
#             ;;
#             esac
#         else echo "ignore $NEW_KEY"
#         fi
#     ) &
# done
# wait
# echo "Syncing Backfill Complete!"

for INFO in $(mc ls --recursive --json $SG_BASEPATH)
do 
    max_bg_procs 5
    (
        KEY=$(echo $INFO | jq -r '.key')
        NEW_KEY=$(python3 -c "print('$KEY'[:11].replace('/', '-')+'$KEY'[14:])")
        FILENAME=$(basename $KEY)
        echo "NEW_KEY:" $NEW_KEY
        
        if [ "${FILENAME#*.}" = "csv.gz" ]; then

            # Check existence
            STATUS=$(mc stat --json $RDP_BASEPATH/$NEW_KEY | jq -r '.status')
            
            case $STATUS in
            success)
                echo "$KEY is already synced to $NEW_KEY, skipping ..."
            ;;
            error)
                # Download data and unzip, remove README.txt and the original .zip file
                mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$NEW_KEY
            ;;
            esac
        else echo "ignore $NEW_KEY"
        fi
    ) &
done
wait
echo "Syncing Complete!"
