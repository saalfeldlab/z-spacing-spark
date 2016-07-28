#!/bin/bash

ROOT_DIR=$(dirname `readlink -f $0`)
FLINTSTONE=$HOME/flintstone/flintstone.sh
N_NODES=$1
JAR=${JAR:-$HOME/z-spacing-spark-scala-1.0.0-SNAPSHOT.jar}
CLASS=org.janelia.thickness.ZSpacing
CONFIG=$ROOT_DIR/config.json
SCALE=0
$FLINTSTONE $N_NODES $JAR $CLASS "'$CONFIG'"  >> runs.log
chmod -w $CONFIG
