#!/bin/bash
function sync {
    case $1 in
    social_distancing | core_poi | monthly_patterns)
        ./_sync/$1.sh
    ;;
    *)
    echo "$1.sh is not included in _sync"
    ;;
    esac
}
register 'run' 'sync' '{ recipe name }' sync

function setup {
    mc config host add sg $SG_S3_ENDPOINT $SG_ACCESS_KEY_ID $SG_SECRET_ACCESS_KEY --api S3v4
    mc config host add rdp $RDP_S3_ENDPOINT $RDP_ACCESS_KEY_ID $RDP_SECRET_ACCESS_KEY --api S3v4
}
register 'setup' '' '' setup