#!/bin/bash

ROOT_DIR=$(dirname `readlink -f $0`)
FLINTSTONE=$HOME/flintstone/src/main/shell
INFLAME=$FLINTSTONE/inflame.sh
N_NODES=$1
ITERATION=$2
JAR=${JAR:-$HOME/z_spacing-spark-scala-0.0.3-SNAPSHOT.jar}
CLASS=org.janelia.thickness.SparkRenderTransformedTransform
CONFIG="$ROOT_DIR/config.json"
OUT="$ROOT_DIR/out/`printf %02d $ITERATION`/forward-transformed/%04d.tif"
$INFLAME $N_NODES $JAR $CLASS $CONFIG $ITERATION $OUT
