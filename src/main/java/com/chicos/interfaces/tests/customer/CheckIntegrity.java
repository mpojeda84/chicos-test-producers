package com.chicos.interfaces.tests.customer;

import com.chicos.interfaces.customer.CustomerDAO;
import com.chicos.interfaces.common.VBStore;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ojai.Document;

public class CheckIntegrity {

	private static final Logger log = LogManager.getLogger(CheckIntegrity.class.getName());

  private CustomerDAO dao;

  public CheckIntegrity(String tablePathToRead) {
    this.dao = new CustomerDAO(tablePathToRead, false);
  }

  public boolean check(){
    VBStore store = this.dao.getStore();
    Document document = store.findById("last-generated");

    Map<String, Object> expected = document.asMap();
    expected.remove("_id");

    boolean okay = expected.keySet().stream().map(x-> {
      List<String> expectation = (List<String>) expected.get(x);
      Document found = store.findById("test-" + x);
      List<Object> reality = found.getList("sequence");

      log.info(x +  " -- Size of Expectation: " + expectation.size() + " and Size of Real: " + reality.size());

      List<String> realityClean = reality.stream().map(String::valueOf).distinct().collect(
          Collectors.toList());

      if(reality.size() != realityClean.size())
        log.info("Elements could have been processed more than once for: " + x);


      StringBuffer line = new StringBuffer("Expectation: ");
      for (int i = 0; i < expectation.size(); i++) {
        line.append(expectation.get(i));
        line.append(", ");
      }
      line.append(" -- Reality: ");
      for (int i = 0; i < reality.size(); i++) {
        line.append(reality.get(i));
        line.append(", ");
      }
      log.info(line.toString());

      for (int i = 0; i < expectation.size(); i++) {
        String exp = expectation.get(i);
        String real = realityClean.get(i);

        if(!exp.equalsIgnoreCase(real))
          return false;
      }
      return true;
    })
        .filter(x -> x == false)
        .findFirst()
        .orElse(true);

    return okay;
  }

// ---------------------------------------------------------------------------------------

  public static void main(String[] args) throws ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse( generateOptions(), args);

    String tablePathToRead = commandLine.getOptionValue("t"); //"/chicos/tables/cu14-3-h100k.db";

    CheckIntegrity checkIntegrity = new CheckIntegrity(tablePathToRead);
    if(checkIntegrity.check())
      System.out.println("----->> ALL GOOD");
    else
      System.out.println("----->> BAD SEQUENCE");

  }

  private static Options generateOptions() {
    Options options = new Options();
    options.addOption("t", true, "Source Table");
    return options;
  }
}
