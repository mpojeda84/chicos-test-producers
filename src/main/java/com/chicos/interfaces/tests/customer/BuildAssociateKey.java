package com.chicos.interfaces.tests.customer;

import com.chicos.interfaces.customer.CustomerDAO;
import com.chicos.interfaces.customer.CustomerService;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ojai.Document;
import org.ojai.store.DocumentMutation;

public class BuildAssociateKey {

	private static final Logger log = LogManager.getLogger(BuildAssociateKey.class.getName());

  private CustomerDAO customerDao;
  private CustomerService customerService;

  public BuildAssociateKey(String customerTable) {
    this.customerDao = new CustomerDAO(customerTable, false);
    this.customerService = new CustomerService();
  }

  private void run(){

    String fieldname_associate_match_key = "associate_match_key";
    Iterator<Document> iterator = customerDao.getIterator();

    while(iterator.hasNext()) {
      Document current = iterator.next();
      String associateKeys = customerService.extractAssociateMatchKeys(current);
      if(associateKeys != null) {
        DocumentMutation mutation = customerDao.getConnection().newMutation();
        mutation.set(fieldname_associate_match_key, associateKeys);
        customerDao.getStore().update(current.getIdString(), mutation);
        log.info("associate_match_key set to ["+associateKeys+"[ for object with id " + current.getIdString());
      } else {
        log.info("associate_match_key not set for object with id " + current.getIdString());
      }
    }
  }


// ---------------------------------------------------------------------------------------

  public static void main(String[] args) throws ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse( generateOptions(), args);

    String customerTable = commandLine.getOptionValue("c");

    BuildAssociateKey matchAssociate = new BuildAssociateKey(customerTable);
    matchAssociate.run();

    log.info("done");

  }

  private static Options generateOptions() {
    Options options = new Options();
    options.addOption("c", true, "Customer Table");
    return options;
  }
}
