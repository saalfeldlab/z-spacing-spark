#!/bin/bash

ROOT_DIR=$(dirname `readlink -f $0`)
FLINTSTONE=$HOME/flintstone/src/main/shell
INFLAME=$FLINTSTONE/inflame.sh
N_NODES=$1
JAR=${JAR:-$HOME/z_spacing-spark-scala-0.0.2-SNAPSHOT.jar}
CLASS=org.janelia.thickness.ZSpacing
CONFIG=$ROOT_DIR/config.json
SCALE=0
$INFLAME $N_NODES $JAR $CLASS $CONFIG
chmod -w $CONFIG
