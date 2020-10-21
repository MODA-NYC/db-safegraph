#!/bin/bash
SG_BASEPATH=sg/sg-c19-response/weekly-patterns/v2/main-file
SG_BASEPATH_NEW=sg/sg-c19-response/weekly-patterns-delivery/weekly/patterns
RDP_BASEPATH=rdp/recovery-data-partnership/weekly_patterns
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

# for INFO in $(mc ls --recursive --json $SG_BASEPATH)
# do 
#     max_bg_procs 5
#     (
#         KEY=$(echo $INFO | jq -r '.key')
#         NEW_KEY=$(python3 -c "print('$KEY'.replace('/', '-'))")
#         FILENAME=$(basename $KEY)
#         DATE=$(echo $FILENAME | cut -c1-10)
#         PARTITION="dt=$DATE"
#         SUBPATH=$(echo $KEY | cut -c-13)

#         if [ "${FILENAME#*.}" = "csv.gz" ]; then

#             # Check existence
#             STATUS=$(mc stat --json $RDP_BASEPATH/$PARTITION/$NEW_KEY | jq -r '.status')
            
#             case $STATUS in
#             success)
#                 echo "$KEY is already synced to $PARTITION/$NEW_KEY, skipping ..."
#             ;;
#             error)
#                 # Download data and unzip, remove README.txt and the original .zip file
#                 mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$PARTITION/$NEW_KEY
#             ;;
#             esac
#         else echo "ignore $FILENAME"
#         fi
#     ) &
# done
# wait
# echo "Syncing Backfill Complete!"

for INFO in $(mc ls --recursive --json $SG_BASEPATH_NEW)
do 
    max_bg_procs 5
    (
        KEY=$(echo $INFO | jq -r '.key')
        NEW_KEY=$(python3 -c "print('$KEY'.replace('/', '-'))")
        DATE=$(echo $NEW_KEY | cut -c1-10)
        PARTITION="dt=$DATE"
        FILENAME=$(basename $KEY)
        SUBPATH=$(echo $KEY | cut -c-13)
        if  [ "${FILENAME#*.}" = "csv.gz" ]; then

            # Check existence
            STATUS=$(mc stat --json $RDP_BASEPATH/$PARTITION/$NEW_KEY | jq -r '.status')
            
            case $STATUS in
            success)
                echo "$KEY is already synced to $PARTITION/$NEW_KEY, skipping ..."
            ;;
            error)
                mkdir -p tmp
                mc cp $SG_BASEPATH_NEW/$KEY tmp/$FILENAME
                (
                    # Remove placekey, effective after 2020 october
                    cd tmp
                    gunzip $FILENAME
                    CSVNAME=$(python3 -c "print('$FILENAME'.replace('.gz', ''))")
                    cut -f1 -d, --complement $CSVNAME > _$CSVNAME
                    rm $CSVNAME
                    gzip _$CSVNAME
                )
                mc cp tmp/_$FILENAME $RDP_BASEPATH/$PARTITION/$NEW_KEY
                rm tmp/_$FILENAME
            ;;
            esac
        else echo "ignore $FILENAME"
        fi
    ) &
done

wait
echo "Syncing New Complete !"