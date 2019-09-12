package com.chicos.interfaces.util;

import com.chicos.interfaces.common.VBStore;
import com.chicos.interfaces.customer.CustomerDAO;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.Utils;
import org.ojai.Document;

public class MurmurHashIdentifierUtil {

  /**
   * Returns a map of generated Hash -> List[Id] that meet the conditions not to be excluded
   * @param partitionsNumber the ampunt of partitions to generate data for
   * @param idsPerPartition the amount of ids to find for each partition
   * @param dao the data access object to retrieve the documents using the document stream iterator
   * @return a map where the Value is a list of document ids and the key is the (same) hashed value of all the elements under that Key
   */
  public Map<Integer, List<String>> getIdsPerHash(int partitionsNumber, int idsPerPartition,
      CustomerDAO dao) {

    VBStore store = dao.getStore();
    Document lastGeneratedDocument = store.findById("last-generated");
    Map<String, Object> lastGenerated = lastGeneratedDocument == null ? new HashMap<>() : lastGeneratedDocument.asMap();


    Map<Integer, List<String>> ids = new HashMap<>(partitionsNumber);
    for (int i = 0; i < partitionsNumber; i++) {
      ids.put(i, new ArrayList<>());
    }

    int count = 0;
    Iterator<Document> it = dao.getIterator();

    System.out.println("iterator created");

    while (it.hasNext()){
      Document document = it.next();
      if(exclude(document, lastGenerated)) {
        System.out.println("excluded " + document.getIdString());
        continue;
      }


      int hash = hash(document.getIdString(), partitionsNumber);
      if (ids.get(hash).size() < idsPerPartition) {
        ids.get(hash).add(document.getIdString());
        count ++;
      }

      if(count == partitionsNumber * idsPerPartition)
        break;
    }
    return ids;
  }

  private boolean exclude (Document document, Map<String, Object> lastGenerated) {

    if(document.getValue("vb") != null)
      return true;
    System.out.println("1");
    if(document.getIdString().contains("stream"))
      return true;

    System.out.println("2");
    if(document.getList("consolidations") == null || document.getList("consolidations").isEmpty())
      return true;

    System.out.println("3");
    try{
      document.getLong("consolidations[0].old_brand_id");
      document.getLong("consolidations[0].old_customer_no");
    } catch (Exception e)
    {
      System.out.println("Exception on document.getLong(\"consolidations[0].old_brand_id\");");
      System.out.println("Exception on document.getLong(\"consolidations[0].old_customer_no\");");
      return true;
    }
    System.out.println("4");

    if(lastGenerated != null && lastGenerated.containsKey(document.getIdString()))
      return true;

    System.out.println("5");
    return false;
  }

  private int hash(String id, int partitionsNumber) {
    return Utils.abs(Utils.murmur2(id.getBytes())) % partitionsNumber;
  }

// ---------------------------------------

  public static void main(String[] args) {

    int partitionsNumber = 4;
    int idsPerPartition = 2;
    CustomerDAO dao = new CustomerDAO("/chicos/tables/cu14-3-h100k.db", false);

    MurmurHashIdentifierUtil murmurHashIdentifierUtil = new MurmurHashIdentifierUtil();

    Map<Integer, List<String>> ids = murmurHashIdentifierUtil
        .getIdsPerHash(partitionsNumber, idsPerPartition, dao);

    ids.entrySet()
        .stream()
        .flatMap(x->x.getValue().stream())
        .map(x-> "\"" + x + "\",")
        .forEach(System.out::println);

    ids.entrySet()
        .stream()
        .flatMap(x -> x.getValue().stream().map(y -> y + " hashed to: " + x.getKey()))
        .forEach(System.out::println);

  }

}
