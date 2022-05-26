#!/bin/bash


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

function copy_sg { 
#$1 is SG_BASEPATH, $2 is RDP_BASEPATH
for INFO in $(mc ls --recursive --json $1)
do 
    echo "Info: $Info"
    max_bg_procs 10
    (
        KEY=$(echo $INFO | jq -r '.key')
        STATUS=$(mc stat --json $2/$KEY | jq -r '.status')
        echo $STATUS
        case $STATUS in
        success)
            # If already synced, skip
            echo "$KEY is already synced, skipping ..."
        ;;
        error)
            # if not, create a sync ...
            mc cp $1/$KEY $2/$KEY
        ;;
        esac
    ) &
done
}
echo "copying..."
copy_sg safegraph-places-outgoing/nyc_gov/weekly /safegraph-post-rdp/patterns
copy_sg safegraph-places-outgoing/neighborhood-patterns/release-2021-07-01 /safegraph-post-rdp/neighborhood-patterns/r2021-07/
#let try to force it.
mc cp --recursive safegraph-places-outgoing/nyc_gov/weekly /safegraph-post-rdp/patterns
wait
echo "raw data sync is complete"