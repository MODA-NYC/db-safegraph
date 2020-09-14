#!/bin/bash
SG_BASEPATH=sg/sg-c19-response/core
RDP_BASEPATH=rdp/recovery-data-partnership/core_poi

for INFO in $(mc ls --recursive --json $SG_BASEPATH)
do
    mkdir -p tmp
    KEY=$(echo $INFO | jq -r '.key')
    PARENT=$(dirname $KEY)
    LOCAL_KEY=tmp/$KEY
    LOCAL_PARENT=$(dirname $LOCAL_KEY)
    PREFIX=${PARENT////}

    # Check existence
    STATUS=$(mc stat --json $RDP_BASEPATH/brand_info/$PREFIX-brand_info.csv | jq -r '.status')
    
    case $STATUS in
    success)
        echo "$KEY is already synced, skipping ..."
    ;;
    error)
        # Download data and unzip, remove README.txt and the original .zip file
        mc cp $SG_BASEPATH/$KEY $LOCAL_KEY
        unzip $LOCAL_KEY -d $LOCAL_PARENT
        rm $LOCAL_PARENT/*.zip && rm $LOCAL_PARENT/*.txt
        
        # Upload brand_info to a seperate folder
        mc cp $LOCAL_PARENT/brand_info.csv $RDP_BASEPATH/brand_info/$PREFIX-brand_info.csv
        
        # Upload *.csv.gz files one by one
        for FILE in $(ls $LOCAL_PARENT/*.gz)
        do
            (
                FILENAME=$(basename $FILE)
                mc cp $FILE $RDP_BASEPATH/poi/$PREFIX-$FILENAME
            ) &
        done;
        wait

        # Clean up
        rm -rf tmp
    ;;
    esac
done;