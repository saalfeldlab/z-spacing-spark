#!/bin/bash

ITERATION=$2
ROOT_DIR=$(dirname `readlink -f $0`)
BASE_DIR=`printf $ROOT_DIR/out/%02d $ITERATION`
FLINTSTONE=$HOME/flintstone/src/main/shell
INFLAME=$FLINTSTONE/inflame.sh
N_NODES=$1
JAR=${JAR:-$HOME/z_spacing-spark-0.0.3-SNAPSHOT.jar}
CLASS=org.janelia.thickness.experiments.SparkRender
OUT=$BASE_DIR/render/%04d.tif
CONFIG=$ROOT_DIR/config.json
$INFLAME $N_NODES $JAR $CLASS $CONFIG $ITERATION $OUT
