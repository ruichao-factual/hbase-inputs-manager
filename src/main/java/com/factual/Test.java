package com.factual;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;

public class Test {
  public static void main(String[] args) throws IOException
  {
    Configuration conf = HBaseConfiguration.create();
    HTable hTable = new HTable(conf, "-ROOT-");
    System.out.println("Table is:" + Bytes.toString(hTable.getTableName()));
    hTable.close();

    System.out.println("Done......");
  }
}
