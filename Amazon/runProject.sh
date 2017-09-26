#!/bin/sh


export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
../../bin/hadoop com.sun.tools.javac.Main Amazon.java
jar cf .jar Amazon*.class
rm -rf output/
mkdir output/
../../bin/hadoop jar .jar Amazon input output/test
