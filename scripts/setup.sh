#!/usr/bin/env bash


if [ -z "$JAVA_HOME" ] ; then
  echo "JAVA_HOME environment variable not defined"
  exit -1;
fi

pathToFile="`dirname $0`/../"
cd $pathToFile
cd - >/dev/null
export HADOOP_UTILS_PATH=`pwd`
echo $HADOOP_UTILS_PATH
cd $HADOOP_UTILS_PATH
mvn clean compile assembly:single
cd - >/dev/null
JAR_PATH="$HADOOP_UTILS_PATH/target/hadoop-utils-1.0-SNAPSHOT-jar-with-dependencies.jar"
COMMAND_PATH="$HADOOP_UTILS_PATH/scripts/bin"
export CLASSPATH=${CLASSPATH}:$JAR_PATH
export PATH=${PATH}:$COMMAND_PATH

