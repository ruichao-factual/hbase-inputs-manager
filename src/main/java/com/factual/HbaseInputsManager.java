package com.factual;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import com.factual.util.Htable;
import com.factual.validation.InputValidators.Validator;
import com.factual.validation.InputValidators.ValidatorFactory;
import com.factual.validation.InputValidators.ValidatorFactory.ValidatorTypes;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.commons.cli.*;

public class HbaseInputsManager {

  final Map<String, ValidatorTypes> inputTypeToValidatorType = ImmutableMap.<String, ValidatorTypes>builder()
      .put("raw_input", ValidatorTypes.RAW_DATA_KEY)
      .put("uuid_attachment", ValidatorTypes.UUID_ATTACHMENT_KEY)
      .put("md5_attachment", ValidatorTypes.MD5_ATTACHMENT_KEY)
      .put("validation", ValidatorTypes.VALIDATION_DATA_KEY)
      .build();

  private Gson gson = new Gson();
  public static final void main(String[] args) throws IOException {
    Option help = new Option("h", "prints this message");
    OptionBuilder.withArgName("cmd");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("command [put, search, update]");
    Option cmdOption = OptionBuilder.create("cmd");

    OptionBuilder.withArgName("type");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("Type [raw_input, uuid_attachment, md5_attachment, validation]");
    Option typeOption = OptionBuilder.create("type");


    OptionBuilder.withArgName("file");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("path to input file");
    Option fileOption = OptionBuilder.create("file");

    OptionBuilder.withArgName("country");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("path to input file");
    Option countryOption = OptionBuilder.create("country");

    OptionBuilder.withArgName("label");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("query label");
    Option labelOption = OptionBuilder.create("label");
    
    OptionBuilder.withArgName("value");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("query value");
    Option valueOption = OptionBuilder.create("value");
    
    Options options = new Options();
    options.addOption(help);
    options.addOption(cmdOption);
    options.addOption(typeOption);
    options.addOption(fileOption);
    options.addOption(countryOption);
    options.addOption(labelOption);
    options.addOption(valueOption);
    
    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      String command = "";
      if (line.hasOption("cmd")) {
        command = line.getOptionValue("cmd");
      }
      String file = "";
      if (line.hasOption("file")) {
        file = line.getOptionValue("file");
      }
      String inputType="";
      if (line.hasOption("type")) {
        inputType = line.getOptionValue("type");
      }
      String country="";
      if (line.hasOption("country")) {
        country = line.getOptionValue("country");
      }
      String queryLabel="";
      if (line.hasOption("label")) {
        queryLabel = line.getOptionValue("label");
      }
      String queryValue="";
      if (line.hasOption("value")) {
        queryValue = line.getOptionValue("value");
      }

      if (command.equals("upload")) {
        new HbaseInputsManager().upload(file, country, inputType);
      }
      
      if (command.equals("query")) {
        if (queryLabel != null && queryValue != null 
            && !"".equals(queryLabel) && !"".equals(queryValue)){
          new HbaseInputsManager().query(queryLabel, queryValue, country);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void upload(String inputPath, String tableName, String inputType) throws Exception{
    Htable htable = new Htable(tableName);
    ValidatorTypes validatorType = inputTypeToValidatorType.get(inputType);
    try {
      BufferedReader inputs = new BufferedReader(new FileReader(inputPath));
      String currentLine;
      while ((currentLine = inputs.readLine()) != null) {
        if (validateInput(currentLine, validatorType)) {
          htable.put(currentLine);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void query(String label, String value, String tableName) throws IOException{
    Htable htable = new Htable(tableName);
    if (label.equalsIgnoreCase("md5")) {
      System.out.println(htable.queryMd5(value));
    } else {
      for (String record : htable.queryPayload(label, value)) {
        System.out.println(record);
      }
    }
  }

  private boolean validateInput(String input, ValidatorTypes inputType) {
    Validator validator = ValidatorFactory.getValidator(input, inputType);
    validator.validate();
    return validator.isValid();
  }
}
