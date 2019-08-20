package com.chicos.interfaces.main.associate;

import com.chicos.interfaces.associate.AssociateDAO;
import com.chicos.interfaces.associate.AssociateService;
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

	private static final Logger log = LogManager.getLogger(
      BuildAssociateKey.class.getName());

  private AssociateDAO associateDAO;
  private AssociateService associateService;

  public BuildAssociateKey(String associateTable) {
    this.associateDAO = new AssociateDAO(associateTable, false);
    this.associateService = new AssociateService();
  }

  private void run(){

    String fieldname_associate_match_key = "associate_match_key";
    Iterator<Document> iterator = associateDAO.getIterator();

    while(iterator.hasNext()) {
      Document current = iterator.next();
      String associateKeys = associateService.getAssociateMatchKey(current);
      if(associateKeys != null) {
        String extraMsg = "";
        if(!associateKeys.equals(current.getString(fieldname_associate_match_key)))
          extraMsg = "Warning! -- Associate Key was " + current.getString(fieldname_associate_match_key);

        DocumentMutation mutation = associateDAO.getConnection().newMutation();
        mutation.set(fieldname_associate_match_key, associateKeys);
        associateDAO.getStore().update(current.getIdString(), mutation);
        log.info("associate_match_key set to ["+associateKeys+"[ for object with id " + current.getIdString() + " " + extraMsg);
      } else {
        log.info("associate_match_key not set for object with id " + current.getIdString());
      }
    }
  }


// ---------------------------------------------------------------------------------------

  public static void main(String[] args) throws ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse( generateOptions(), args);

    String associateTable = commandLine.getOptionValue("a");

    BuildAssociateKey matchAssociate = new BuildAssociateKey(associateTable);
    matchAssociate.run();

    log.info("done");

  }

  private static Options generateOptions() {
    Options options = new Options();
    options.addOption("a", true, "Associate Table");
    return options;
  }
}
