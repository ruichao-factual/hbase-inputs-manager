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


// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class Test {
  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HTable hTable = new HTable(conf, "HBaseSamples");
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("test"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
    put1.add(Bytes.toBytes("test"), Bytes.toBytes("col2"), Bytes.toBytes("value2"));
    hTable.put(put1);
    hTable.close();
  }
}
