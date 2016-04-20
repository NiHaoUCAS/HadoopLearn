#!/bin/bash

name=$1

mkdir ${name}_class
javac ${name}.java -d ${name}_class || exit 1;
jar -cvf ${name}.jar -C ${name}_class . || exit 1;

hadoop fs -put input nihao/hadoop_learn/${name} || hadoop fs -rmr nihao/hadoop_learn/${name}/* && hadoop fs -put input nihao/hadoop_learn/${name} || exit 1;

hadoop jar ${name}.jar ${name} nihao/hadoop_learn/${name}/input nihao/hadoop_learn/${name}/output || exit 1;

hadoop fs -get nihao/hadoop_learn/${name}/output ./
