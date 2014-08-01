package com.factual.util;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
  
  public String queryMd5(String md5) {
    Get row = new Get(Bytes.toBytes(md5));
    Result queryResult = null;
	try {
	  queryResult = hTable.get(row);
    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
    }
	
	String result = null;
    try {
      result = getJsonFromResult(queryResult);
	} catch (JSONException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    return result;
  }
  
  public List<String>queryPayload(String label, String value) {
    return query("payload", label, value);
  } 

  private List<String> query(String family, String column, String value) {
    Scan scan = new Scan();
    
    List<Filter> filters = new ArrayList<Filter>();
    SingleColumnValueFilter scValueFilter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareFilter.CompareOp.EQUAL, new RegexStringComparator(value, Pattern.CASE_INSENSITIVE));
    filters.add(scValueFilter);
    
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    scan.setFilter(filterList);
    
    ResultScanner scanner = null;
	try {
		scanner = hTable.getScanner(scan);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

    List<String> queryResults = new ArrayList<String>();
    try {
      for (Result rowResult : scanner) {
        queryResults.add(getJsonFromResult(rowResult));
      }
    } catch (JSONException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} finally {
      scanner.close();
    }
    return queryResults;
  }

  private String getJsonFromResult(Result row) throws JSONException {
    JSONObject json = new JSONObject();
    JSONObject payload  = new JSONObject();
    JSONObject inputMeta = new JSONObject();
    String md5 = Bytes.toString(row.getRow());
    for (KeyValue keyValue : row.raw()) {
      String family = Bytes.toString(keyValue.getFamily());
      String qualifier = Bytes.toString(keyValue.getQualifier());
      String value = Bytes.toString(keyValue.getValue());
      
      if ("payload".equals(family)){
        payload.put(qualifier, value);
      } else if ("inputMeta".equals(family)){
        inputMeta.put(qualifier, value);
      } else {
        json.put(qualifier, value);
      }
    }
    
    json.put("payload", payload);
    json.put("inputMeta", inputMeta);
    json.put("md5", md5);
    return json.toString();
  }

}
