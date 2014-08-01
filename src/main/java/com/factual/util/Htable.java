package com.factual.util;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.util.Iterator;

import org.json.JSONObject;
import com.google.gson.Gson;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Htable {

  public HTable hTable;
  private Gson gson;

  public Htable(String tableName) throws IOException{
    Configuration conf = HBaseConfiguration.create();
    hTable = new HTable(conf, tableName);
    gson = new Gson();
  }

  public boolean put(String rawInput) {
    try {
      JSONObject obj = new JSONObject(rawInput);
      String md5 = gson.toJson(obj.get("md5"));
      JSONObject payload = (JSONObject)obj.get("payload");
      JSONObject inputMeta = (JSONObject)obj.get("inputMeta");
      String processingState = gson.toJson(obj.get("processingState"));
      String inputDate = gson.toJson(obj.get("inputDate"));
      Put put = new Put(Bytes.toBytes(md5));
      putColumnFamily(put, payload, "payload");
      putColumnFamily(put, inputMeta, "inputMeta");
      put.add(Bytes.toBytes("info"), Bytes.toBytes("inputDate"), Bytes.toBytes(inputDate));
      put.add(Bytes.toBytes("info"), Bytes.toBytes("processingState"), Bytes.toBytes(processingState));
      hTable.put(put);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  private void putColumnFamily(Put put, JSONObject jsonObj, String familyName) throws Exception{
    Iterator<?> keys = jsonObj.keys();
    while (keys.hasNext()) {
      String stringKey = (String)keys.next();
      String value = gson.toJson(jsonObj.get(stringKey));
      put.add(Bytes.toBytes(familyName), Bytes.toBytes(stringKey), Bytes.toBytes(value));
    }
  }
}
