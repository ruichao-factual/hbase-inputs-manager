package com.factual;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.cli.*;


// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class HbaseInputsManager {
  public static void main(String[] args) throws IOException {
    Option help = new Option("h", "prints this message");
    OptionBuilder.withArgName("command");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("command [put, search, update]");
    Option command = OptionBuilder.create("c");




    Configuration conf = HBaseConfiguration.create();
    HTable hTable = new HTable(conf, "HBaseSamples");
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("test"), Bytes.toBytes("col3"), Bytes.toBytes("value4"));
    put1.add(Bytes.toBytes("test"), Bytes.toBytes("col5"), Bytes.toBytes("value6"));
    hTable.put(put1);
    hTable.close();
  }
}
