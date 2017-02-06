#!/bin/bash

ROOT_DIR=$(dirname `readlink -f $0`)
FLINTSTONE=$HOME/flintstone/flintstone.sh
MASTER_JOB_ID=$1
JAR=${JAR:-$HOME/z_spacing-spark-0.0.5-SNAPSHOT.jar}
CLASS=org.janelia.thickness.ZSpacing
CONFIG=$ROOT_DIR/config.json
SPARK_VERSION=2 $FLINTSTONE $N_NODES $JAR $CLASS $CONFIG  >> runs.log
chmod -w $CONFIG
