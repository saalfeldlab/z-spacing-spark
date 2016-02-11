#!/bin/bash

SCRIPT_DIR="$(dirname ${BASH_SOURCE})"
echo $SCRIPT_PATH

START=$1
STOP=$2
SOURCE=${SOURCE:-$(realpath data)"/%04d.tif"}
STAGES=${STAGES:-1}
MAX_OFFSET=${MAX_OFFSET:-0}

if [ -d mask ]; then
    DEFAULT_MASK=$(realpath mask)"/%04d.tif"
else
    DEFAULT_MASK=""
fi

MASK=${MASK:-$DEFAULT_MASK}

WIDTH=$(identify -ping -format '%W' `printf "$SOURCE" $START`)
HEIGHT=$(identify -ping -format '%H' `printf "$SOURCE" $START`)

STAGE=0

while [ $STAGE -lt $STAGES ]; do
    [ "$STAGE" -gt "0" ] && OPTS="$OPTS,\n"
    OPTS="$OPTS \
{\n \
    \"steps\": [\n \
        $WIDTH,\n \
        $HEIGHT\n \
    ],\n \
    \"radii\": [\n \
        $(($WIDTH/2+1)),\n \
        $(($HEIGHT/2+1))\n \
    ],\n \
    \"maxOffsets\": [\n \
        $MAX_OFFSET,\n \
        $MAX_OFFSET\n \
    ],\n \
    \"correlationBlockRadii\": [\n \
        $(($WIDTH/3)),\n \
        $(($HEIGHT/3))\n \
    ]"

    if [ $STAGE -gt 0 ]; then
        OPTS="$OPTS,\n \
    \"inference\": {\n \
        \"regularizationType\": \"NONE\",\n \
        \"coordinateUpdateRegularizerWeight\": `expr \"0.1*$STAGE\" | bc`,\n \
        \"comparisonRange\": 20\n \
    }"
    fi

OPTS="${OPTS}\n \
}"

    STAGE=$(($STAGE+1))
    WIDTH=$((WIDTH/2))
    HEIGHT=$((HEIGHT/2))
done


TARGET_DIR=$PWD/$(date +%Y%m%d_%H%M%S)
mkdir $TARGET_DIR
( cd $SCRIPT_DIR && cp config.json run.sh render-transform-spark.sh render-spark.sh -t $TARGET_DIR )
OUT=$(realpath $TARGET_DIR/out)
sed -i -e "s#START#$START#" -e "s#STOP#$STOP#" -e "s#TARGET#$OUT#" -e "s#SOURCE#$SOURCE#" -e "s#OPTS#${OPTS}#" -e "s#MASK#$MASK#" $TARGET_DIR/config.json
chmod u+w $TARGET_DIR/config.json
