#!/bin/bash
# SG_BASEPATH=sg/sg-c19-response/neighborhood-patterns/neighborhood-patterns/2021/07/07/release-2021-07-01/neighborhood_patterns
SG_BASEPATH=sg/sg-c19-response/neighborhood-patterns/neighborhood-patterns/2021
RDP_BASEPATH=rdp/recovery-data-partnership/neighborhood_patterns_202107

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

for INFO in $(mc ls --recursive --json $SG_BASEPATH)
do 
    max_bg_procs 5
    (
        KEY=$(echo $INFO | jq -r '.key')
        FILENAME=$(basename $KEY)
        PATH_MONTH=$(dirname $KEY)
        PATH_YEAR=$(dirname $PATH_MONTH)
        PATH_DATATYPE=$(dirname $PATH_YEAR)
        DATATYPE=$(basename $PATH_DATATYPE)

        echo "KEY: " $KEY
        echo "PATH_MONTH: " $PATH_MONTH
        echo "PATH_YEAR: " $PATH_YEAR
        echo "PATH_DATATYPE: " $PATH_DATATYPE
        echo "DATATYPE: " $DATATYPE
         
        if [ "${FILENAME#*.}" = "csv.gz" ] && [ $DATATYPE = "neighborhood_patterns" ]; then
            
            # Check existence
            STATUS=$(mc stat --json $RDP_BASEPATH/$KEY | jq -r '.status')
            
            case $STATUS in
            success)
                echo "$KEY is already synced to $KEY, skipping ..."
            ;;
            error)
                # mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$KEY
            ;;
            esac
        else echo "ignore $KEY"
        fi
    ) &
done
wait
echo "Syncing Complete!"


