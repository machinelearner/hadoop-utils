#!/bin/sh

if [ -z "$JAVA_HOME" ] ; then
  echo "JAVA_HOME environment variable not defined"
  exit -1;
fi

pathToFile="`dirname $0`/../"
cd $pathToFile
export HADOOP_UTILS_PATH=`pwd`
cd - >/dev/null
cd $HADOOP_UTILS_PATH
mvn clean compile assembly:single
if [ -z $?  ]; then
    exit -1;
fi
cd - >/dev/null
JAR_PATH="$HADOOP_UTILS_PATH/target/hadoop-utils-1.0-SNAPSHOT-jar-with-dependencies.jar"
sudo cp -f $JAR_PATH $JAVA_HOME/lib/hadoop-utils.jar
sudo cp -f $HADOOP_UTILS_PATH/scripts/bin/* /usr/bin
echo "##############################################"
echo "##############################################"
echo "Hadoop Utils"
echo "INSTALLATION SUCCESSFUL"
echo "Start shell usage with hmount command"
echo "##############################################"
echo "##############################################"
