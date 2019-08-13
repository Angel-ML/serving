#!/usr/bin/env bash
export SERVING_HOME="$(cd "`dirname "$0"`"/..; pwd)"
export EXTRA_LIB_JARS_HOME=

#export JAVA_HOME=

export SERVING_CONF_DIR=$SERVING_HOME/conf/
export SERVING_CLASSPATH="$SERVING_CONF_DIR"

for f in  $SERVING_HOME/lib/*.jar; do
    export SERVING_CLASSPATH=$SERVING_CLASSPATH:$f
done
for f in  EXTRA_LIB_JARS_HOME/lib/*.jar; do
    export SERVING_CLASSPATH=$SERVING_CLASSPATH:$f
done