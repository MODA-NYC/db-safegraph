#!/bin/bash
SG_BASEPATH_CORE=sg/sg-c19-response
RDP_BASEPATH=rdp/recovery-data-partnership/output/raw

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
    max_bg_procs 10
    (
        KEY=$(echo $INFO | jq -r '.key')
        FILENAME=$(basename $KEY)
        STATUS=$(mc stat --json $RDP_BASEPATH/$FILENAME | jq -r '.status')
        case $STATUS in
        success)
            # If already synced, skip
            echo "$KEY is already synced, skipping ..."
        ;;
        error)
            # if not, create a sync ...
            mc cp $SG_BASEPATH/$KEY $RDP_BASEPATH/$FILENAME
        ;;
        esac
    ) &
done
wait
echo "raw data sync is complete"