#!/bin/bash

ROOT_DIR=$(dirname `readlink -f $0`)
FLINTSTONE=$HOME/flintstone/src/main/shell
INFLAME=$FLINTSTONE/inflame.sh
N_NODES=$1
JAR=$HOME/z_spacing-spark-scala-0.0.2-SNAPSHOT.jar
CLASS=org.janelia.thickness.experiments.SparkRenderWave
IN="$ROOT_DIR/../../data/%04d.tif"
TRANSFORM="$ROOT_DIR/backward/%04d.tif"
OUT=$ROOT_DIR/render/%04d.tif
$INFLAME $N_NODES $JAR $CLASS $IN $TRANSFORM $OUT 5 5 0 475
