package com.chicos.interfaces.util;

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
    Map<Integer, List<String>> ids = new HashMap<>(partitionsNumber);
    for (int i = 0; i < partitionsNumber; i++) {
      ids.put(i, new ArrayList<>());
    }

    int count = 0;
    Iterator<Document> it = dao.getIterator();
    while (it.hasNext()){
      Document document = it.next();
      if(exclude(document))
        continue;

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

  private boolean exclude (Document document) {
    if(document.getIdString().contains("stream"))
      return true;

    if(document.getList("consolidations") == null || document.getList("consolidations").isEmpty())
      return true;

    try{
      document.getLong("consolidations[0].old_brand_id");
      document.getLong("consolidations[0].old_customer_no");
    } catch (Exception e)
    {
      return true;
    }

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
