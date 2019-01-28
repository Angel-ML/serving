#!/usr/bin/env bash
export SERVING_HOME="$(cd "`dirname "$0"`"/..; pwd)"

#export JAVA_HOME=

export SERVING_CONF_DIR=$SERVING_HOME/conf/
export SERVING_CLASSPATH="$SERVING_CONF_DIR"

for f in  $SERVING_HOME/lib/*.jar; do
    export SERVING_CLASSPATH=$SERVING_CLASSPATH:$f
done