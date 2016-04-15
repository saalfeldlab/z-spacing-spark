#!/bin/bash

ROOT_DIR=$(dirname `readlink -f $0`)
FLINTSTONE=$HOME/flintstone/src/main/shell
INFLAME=$FLINTSTONE/inflame.sh
N_NODES=$1
JAR=${JAR:-$HOME/z_spacing-spark-0.0.3-SNAPSHOT.jar}
CLASS=org.janelia.thickness.ZSpacing
CONFIG=$ROOT_DIR/config.json
SCALE=0
$INFLAME $N_NODES $JAR $CLASS $CONFIG  >> runs.log
chmod -w $CONFIG
