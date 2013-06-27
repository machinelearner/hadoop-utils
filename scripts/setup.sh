#!/usr/bin/env bash


if [ -z "$JAVA_HOME" ] ; then
  echo "JAVA_HOME environment variable not defined"
  exit -1;
fi

pathToFile="`dirname $0`/../"
cd $pathToFile
export HADOOP_UTILS_PATH=`pwd`
cd - >/dev/null
echo $HADOOP_UTILS_PATH
cd $HADOOP_UTILS_PATH
mvn clean compile assembly:single
cd - >/dev/null
JAR_PATH="$HADOOP_UTILS_PATH/target/hadoop-utils-1.0-SNAPSHOT-jar-with-dependencies.jar"
COMMAND_PATH="$HADOOP_UTILS_PATH/scripts/bin"
echo $CLASSPATH
export CLASSPATH="${CLASSPATH}:$JAR_PATH"
echo $CLASSPATH
echo $PATH
export PATH="${PATH}:$COMMAND_PATH"
echo $PATH

