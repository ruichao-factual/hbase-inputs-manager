package com.factual;

import java.io.IOException;

import com.factual.util.Htable;
import org.apache.commons.cli.*;

public class HbaseInputsManager {
  public static void main(String[] args) throws IOException {
    // Option help = new Option("h", "prints this message");
    // OptionBuilder.withArgName("command");
    // OptionBuilder.hasArg();
    // OptionBuilder.withDescription("command [put, search, update]");
   // Option command = OptionBuilder.create("c");
    //String rawInput = "{\"md5\":\"1b4a2e612ddca716e\",\"payload\":{\"name\":\"香港丽思卡尔顿酒店\",\"address\":\"上海`油尖旺区尖沙咀柯士甸道西1号环球贸易广场102-118楼\",\"tel\":\"852 2263 2263\",\"website\":\"http://www.ritzcarlton.com/zh-cn/Properties/HongKong/Default.htm\",\"category_labels\":[\"hotel\"],\"latitude\":\"22.30348018828219\",\"longitude\":\"114.16021227836609\",\"foreign_id\":\"297\",\"country\":\"hk\",\"hours\":null},\"inputMeta\":{\"origin\":\"SEED\",\"sourceUrl\":\"http://www.160.com.hk/hotel/bencandy-htm-city_id-1-fid-24-id-297.html\",\"notes\":{\"_rs_id\":1367,\"_ds_id\":28,\"_rs_v\":1378268958},\"targetViewAlias\":\"places-hk\"},\"processingState\":\"UNPROCESSED\",\"inputDate\":1378290224000}\t";
    //System.out.println("start");
    //System.out.println(rawInput);
    //Htable htable = new Htable("hk");
    //htable.put(rawInput);
    //System.out.println("done");

    Htable htable = new Htable("hk");
    for (String result : htable.queryPayload("address", "九龙")) {
      System.out.println(result);
    }
    System.out.println("-------");
    for (String result : htable.queryPayload("address", "上海")) {
      System.out.println(result);
    }
    System.out.println("-------");
    System.out.println(htable.queryMd5("006c669f4aad31d1b4ace612ddca716e"));
  }
}
