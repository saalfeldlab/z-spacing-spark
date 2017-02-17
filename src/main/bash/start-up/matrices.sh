#!/bin/bash

ROOT_DIR=$(dirname `readlink -f $0`)
FLINTSTONE=$HOME/flintstone/flintstone.sh
MASTER_JOB_ID=$1
JAR=${JAR:-$HOME/z_spacing-spark-0.0.5-SNAPSHOT.jar}
CLASS=org.janelia.thickness.matrix.ComputeMatrices
CONFIG=$ROOT_DIR/config.json
export N_DRIVER_THREADS=${N_DRIVER_THREADS:-4}
export SPARK_VERSION=2
CMD="SPARK_VERSION=2 $FLINTSTONE $MASTER_JOB_ID $JAR $CLASS $CONFIG"
$CMD  >> runs.log
chmod -w $CONFIG
