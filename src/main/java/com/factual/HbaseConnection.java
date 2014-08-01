package com.factual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import java.util.regex.Pattern;

public class HbaseConnection
{
  public static void main(String[] args) throws IOException {
   	/* When you create a HBaseConfiguration, it reads in whatever you've set
   	into your hbase-site.xml and in hbase-default.xml, as long as these can
   	be found on the CLASSPATH */
   
   	org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
   	
   	/*This instantiates an HTable object that connects you to the "test" table*/
   
   	HTable table = new HTable(config, "User");
   	
   	/* To add to a row, use Put. A Put constructor takes the name of the row
   	you want to insert into as a byte array. */
   
   	//Put p = new Put(Bytes.toBytes("row1"));
   
   	/* To set the value you'd like to update in the row 'row1', specify
   	the column family, column qualifier, and value of the table cell you'd
   	like to update.  The column family must already exist in your table
   	schema.  The qualifier can be anything.  */
   
   	//p.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("Emp1"));
   
   	//p.add(Bytes.toBytes("Name"),Bytes.toBytes("col2"),Bytes.toBytes("Archana"));

   	//p.add(Bytes.toBytes("Haha"),Bytes.toBytes("col3"),Bytes.toBytes("Woca"));
   
   	/* Once you've adorned your Put instance with all the updates you want to
   	make, to commit it do the following */
   
   	//table.put(p);
   
   	// Now, to retrieve the data we just wrote.
   
   	//Get g = new Get(Bytes.toBytes("row1"));
   
   	//Result r = table.get(g);
   
   	//byte [] value = r.getValue(Bytes.toBytes("Id"),Bytes.toBytes("col1"));
   
   	//byte [] value1 = r.getValue(Bytes.toBytes("Name"),Bytes.toBytes("col2"));
   
   	//String valueStr = Bytes.toString(value);
   
   	//String valueStr1 = Bytes.toString(value1);
   
   	//System.out.println("GET: " +"Id: "+ valueStr+"Name: "+valueStr1);
   
   	Scan s = new Scan();
   
   	//s.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("col1"));
   
   	//s.addColumn(Bytes.toBytes("Name"), Bytes.toBytes("col2"));
   
   	List<Filter> filters = new ArrayList<Filter>();
   	SingleColumnValueFilter scValueFilter = new SingleColumnValueFilter(Bytes.toBytes("Id"), Bytes.toBytes("col1"), CompareFilter.CompareOp.EQUAL, new RegexStringComparator("emp", Pattern.CASE_INSENSITIVE));
   	filters.add(scValueFilter);
   	
    FilterList fl = new FilterList( FilterList.Operator.MUST_PASS_ALL, filters);
    s.setFilter(fl);
    
   	ResultScanner scanner = table.getScanner(s);
   
   	try
   	{
   	  for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
   		  for (KeyValue kv : rr.raw()) {
   		    System.out.println("Row: " + Bytes.toString(kv.getRow()));
   		    System.out.println("Family: " + Bytes.toString(kv.getFamily()));
   		    System.out.println("Qualifier: " + Bytes.toString(kv.getQualifier()));
   		    System.out.println("Value: " + Bytes.toString(kv.getValue()));
   		    System.out.println("Time: " + kv.getTimestamp());
   	    System.out.println("");
   		  }
   	    System.out.println("");
   	    System.out.println("");
   	  }
   	} finally {
   	  // Make sure you close your scanners when you are done!
   	  scanner.close();
   	}
  }
}
