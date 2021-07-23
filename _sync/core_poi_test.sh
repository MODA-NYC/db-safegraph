#!/bin/bash
SG_BASEPATH_CORE=sg/sg-c19-response/core-places-delivery/core_poi
SG_BASEPATH_BRAND=sg/sg-c19-response/core-places-delivery/brand_info
RDP_BASEPATH=rdp/recovery-data-partnership/core_poi_new2

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

for INFO in $(mc ls --recursive --json $SG_BASEPATH_CORE)
do 
    max_bg_procs 5
    (
        KEY=$(echo $INFO | jq -r '.key')
        FILENAME=$(basename $KEY)
        PARENT=$(dirname $KEY)
        GPARENT=$(dirname $PARENT)
        GGPARENT=$(dirname $GPARENT)
        # LOCAL_KEY=tmp/$KEY
        # LOCAL_PARENT=$(dirname $LOCAL_KEY)
        PREFIX=${GGPARENT////}
        # DATE=$(python3 -c "print('$GGPARENT'.replace('/', '-')+'-01')")
        # PARTITION="dt=$DATE"

        # SafeGraph mistakingly uploaded '2020/11/06/11/brand_info.csv' and instructed us to ignore.
        # 2020/11 core poi data doesn't have parent_placekey
        if [ $GGPARENT != '2020/11' ]; then
            if [ "${FILENAME#*.}" = "csv.gz" ]; then

                # Check existence
                # STATUS=$(mc stat --json $RDP_BASEPATH/poi/$PARTITION/$PREFIX-$FILENAME | jq -r '.status')
                STATUS=$(mc stat --json $RDP_BASEPATH/poi/$PREFIX-$FILENAME | jq -r '.status')
                
                case $STATUS in
                success)
                    # echo "$KEY is already synced to $RDP_BASEPATH/poi/$PARTITION/$PREFIX-$FILENAME, skipping ..."
                    echo "$KEY is already synced to $RDP_BASEPATH/poi/$PREFIX-$FILENAME, skipping ..."
                ;;
                error)
                    mkdir -p tmp
                    mc cp $SG_BASEPATH_CORE/$KEY tmp/$PREFIX-$FILENAME
                    (
                        cd tmp
                        echo "ls -la on tmp before anything is done"
                        ls -la
                        python3 ../core_add_date.py
                        echo "ls -la on tmp after all is done"
                        ls -la
                        # awk -v d="$PREFIX" -F"," 'BEGIN { OFS = "," } {$1=d; print}' $CSVNAME > _$CSVNAME
                        # rm $CSVNAME
                        # gzip _$CSVNAME
                    )
                    # Transfer data
                    mc cp tmp/$PREFIX-$FILENAME $RDP_BASEPATH/poi/$PREFIX-$FILENAME
                    rm tmp/$PREFIX-$FILENAME                    
                    # echo "Copy $SG_BASEPATH_CORE/$KEY to $RDP_BASEPATH/poi/$PREFIX-$FILENAME"
                    # mc cp $SG_BASEPATH_CORE/$KEY $RDP_BASEPATH/poi/$PREFIX-$FILENAME
                    # echo "Copy $SG_BASEPATH_CORE/$KEY to $RDP_BASEPATH/poi/$PARTITION/$PREFIX-$FILENAME"
                    # mc cp $SG_BASEPATH_CORE/$KEY $RDP_BASEPATH/poi/$PARTITION/$PREFIX-$FILENAME
                 ;;
                esac
            else echo "ignore $FILENAME"
            fi
        fi
    ) &
done
wait
echo "POI data sync complete!"

# for INFO in $(mc ls --recursive --json $SG_BASEPATH_BRAND)
# do 
#     max_bg_procs 5
#     (
#         KEY=$(echo $INFO | jq -r '.key')
#         FILENAME=$(basename $KEY)
#         PARENT=$(dirname $KEY)
#         GPARENT=$(dirname $PARENT)
#         GGPARENT=$(dirname $GPARENT)
#         # LOCAL_KEY=tmp/$KEY
#         # LOCAL_PARENT=$(dirname $LOCAL_KEY)
#         PREFIX=${GGPARENT////}
#         # DATE=$(python3 -c "print('$GGPARENT'.replace('/', '-')+'-01')")
#         # PARTITION="dt=$DATE"

#         # SafeGraph mistakingly uploaded '2020/11/06/11/brand_info.csv' and instructed us to ignore.
#         if [ $PARENT != '2020/11/06/11' ]; then
#             if [ "${FILENAME#*.}" = "csv" ]; then

#                 # Check existence
#                 # STATUS=$(mc stat --json $RDP_BASEPATH/brand_info/$PARTITION/$PREFIX-brand_info.csv | jq -r '.status')
#                 STATUS=$(mc stat --json $RDP_BASEPATH/brand_info/$PREFIX-brand_info.csv | jq -r '.status')
                
#                 case $STATUS in
#                 success)
#                     # echo "$KEY is already synced to $RDP_BASEPATH/brand_info/$PARTITION/$PREFIX-brand_info.csv, skipping ..."
#                     echo "$KEY is already synced to $RDP_BASEPATH/brand_info/$PREFIX-brand_info.csv, skipping ..."
#                 ;;
#                 error)
#                     # Transfer data
#                     # echo "Copy $SG_BASEPATH_CORE/$KEY to $RDP_BASEPATH/brand_info/$PARTITION/$PREFIX-brand_info.csv"
#                     # mc cp $SG_BASEPATH_CORE/$KEY $RDP_BASEPATH/brand_info/$PARTITION/$PREFIX-brand_info.csv
#                     echo "Copy $SG_BASEPATH_BRAND/$KEY to $RDP_BASEPATH/brand_info/$PREFIX-brand_info.csv"
#                     mc cp $SG_BASEPATH_BRAND/$KEY $RDP_BASEPATH/brand_info/$PREFIX-brand_info.csv
#                 ;;
#                 esac
#             else echo "ignore $FILENAME"
#             fi
#         fi
#     ) &
# done
# wait
# echo "Brand info data sync complete!"