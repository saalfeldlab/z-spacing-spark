#!/bin/bash

ITERATION=$2
ROOT_DIR=$(dirname `readlink -f $0`)
BASE_DIR=`printf $ROOT_DIR/out/%02d $ITERATION`
FLINTSTONE=$HOME/flintstone/flintstone.sh
N_NODES=$1
JAR=${JAR:-$HOME/z-spacing-spark-scala-1.0.0-SNAPSHOT.jar}
CLASS=org.janelia.thickness.utility.SparkRender
OUT=$BASE_DIR/render/%04d.tif
CONFIG=$ROOT_DIR/config.json
$FLINTSTONE $N_NODES $JAR $CLASS "'$CONFIG'" $ITERATION $OUT
