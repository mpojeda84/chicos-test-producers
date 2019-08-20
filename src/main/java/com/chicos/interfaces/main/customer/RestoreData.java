package com.chicos.interfaces.main.customer;

import com.chicos.interfaces.customer.CustomerDAO;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RestoreData {

	private static final Logger log = LogManager.getLogger(RestoreData.class.getName());

  private CustomerDAO dao;

  public RestoreData(String tablePathToRead) {
    this.dao = new CustomerDAO(tablePathToRead, false);
  }

  public void restore(){
      this.dao.restoreFromQuarantine();
  }


// ---------------------------------------------------------------------------------------

  public static void main(String[] args) throws ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse( generateOptions(), args);

    String tablePathToRead = commandLine.getOptionValue("t");
    RestoreData restoreData = new RestoreData(tablePathToRead);
    restoreData.restore();
    log.info("done");

  }

  private static Options generateOptions() {
    Options options = new Options();
    options.addOption("t", true, "Source Table");
    return options;
  }
}
