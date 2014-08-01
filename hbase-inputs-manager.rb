#!/usr/bin/env ruby

`export HADOOP_CLASSPATH=$HBASE_HOME/*:$HBASE_HOME/conf/:$HADOOP_CLASSPATH`

command = 'yarn jar target/hbase-inputs-manager-1.0-SNAPSHOT.jar com.factual.HbaseInputsManager '
arguments = ARGV.join(" ");

full_command = command + " " + arguments

`#{full_command}`
