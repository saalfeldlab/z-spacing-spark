#!/bin/bash

SCRIPT_DIR="$(dirname ${BASH_SOURCE})"

USAGE="[SOURCE=<pattern>] [STAGES=<int>] [SCALE=<integer>] [MAX_OFFSET=<integer>] [JOIN_STEP_SIZE=<integer>] [LOG_MATRICES={bool|bool[]|int[]}] $0 START STOP"


EXIT_CODE=1
if [ "$#" -lt 2 ]; then
    echo "Wrong number of arguments."
    echo "Usage:"
    echo $USAGE
    exit $EXIT_CODE
fi
((++EXIT_CODE))

START=$1
STOP=$2
SOURCE=${SOURCE:-$(realpath data)"/%04d.tif"}
STAGES=${STAGES:-1}
SCALE=${SCALE:-0}
MAX_OFFSET=${MAX_OFFSET:-0}
JOIN_STEP_SIZE=${JOIN_STEP_SIZE:-$(($STOP-$START))}
LOG_MATRICES=${LOG_MATRICES:-false}

WIDTH=$(identify -ping -format '%W' `printf "$SOURCE" $START`)
HEIGHT=$(identify -ping -format '%H' `printf "$SOURCE" $START`)

if [ -n "$SCALE" ]; then
    ((WIDTH/=2**$SCALE))
    ((HEIGHT/=2**$SCALE))
fi

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
sed -i \
    -e "s#START#$START#" -e "s#STOP#$STOP#" -e "s#TARGET#$OUT#" -e "s#SOURCE#$SOURCE#" -e "s#OPTS#${OPTS}#" \
    -e "s#SCALE#$SCALE#" -e "s#LOG_MATRICES#$LOG_MATRICES#" -e "s#JOIN_STEP_SIZE#$JOIN_STEP_SIZE#" \
    $TARGET_DIR/config.json
chmod u+w $TARGET_DIR/config.json
