package com.chicos.interfaces.main.associate;

import com.chicos.interfaces.common.NonUniqueResultException;
import com.chicos.interfaces.associate.AssociateDAO;
import com.chicos.interfaces.associate.AssociateService;
import com.chicos.interfaces.customer.CustomerDAO;
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

public class MatchAssociate {

  private static final Logger log = LogManager.getLogger(
      BuildAssociateKey.class.getName());

  private AssociateDAO associateDAO;
  private CustomerDAO customerDAO;
  private AssociateService associateService;

  public MatchAssociate(String associateTable, String customerTable) {
    this.associateDAO = new AssociateDAO(associateTable, false);
    this.customerDAO = new CustomerDAO(customerTable, false);
    this.associateService = new AssociateService();
  }

  private void run(){

    String fieldname_associate_match_key = "associate_match_key";
    Iterator<Document> iterator = associateDAO.getIterator();

    while(iterator.hasNext()) {
      Document current = iterator.next();
      String matchKey = current.getString(fieldname_associate_match_key);
      if(matchKey != null && !matchKey.isEmpty()) {
        Document customer = null;
        try {
          customer = customerDAO.findByAssociateKeys(matchKey);
        } catch (NonUniqueResultException nonUniqueResultException) {
          log.error("More than one Customer has the same key: " + matchKey);
        }
        if(customer == null)
          continue;

        DocumentMutation mutation = customerDAO.getConnection().newMutation();
        mutation.set("associate_id", customer);
        customerDAO.getStore().update(current.getIdString(), mutation);

      }
    }
  }


// ---------------------------------------------------------------------------------------

  public static void main(String[] args) throws ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse( generateOptions(), args);

    String associateTable = commandLine.getOptionValue("a");
    String customerTable = commandLine.getOptionValue("c");

    MatchAssociate matchAssociate = new MatchAssociate(associateTable, customerTable);
    matchAssociate.run();

    log.info("done");

  }

  private static Options generateOptions() {
    Options options = new Options();
    options.addOption("a", true, "Associate Table");
    options.addOption("c", true, "Customer Table");
    return options;
  }

}
