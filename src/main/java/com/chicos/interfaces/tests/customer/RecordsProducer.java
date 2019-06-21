package com.chicos.interfaces.tests.customer;

import com.chicos.interfaces.common.Pair;
import com.chicos.interfaces.customer.CustomerDAO;
import com.chicos.interfaces.customer.CustomerService;
import com.chicos.interfaces.customer.VBProducer;
import com.chicos.interfaces.tests.customer.common.MurmurHashIdentifier;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.ojai.Document;
import org.ojai.json.Json;

public class RecordsProducer {

  private String topic;
  private VBProducer<String, String> producer;
  private CustomerDAO dao;
  private CustomerService service;

  public RecordsProducer(String tablePathToRead, String streamTopicToWrite) {
    this.producer = createProducer();
    this.topic = streamTopicToWrite;
    this.dao = new CustomerDAO(tablePathToRead, false);
    this.service = new CustomerService();
  }

  /**
   * Prepares the queue of records to be sent to the topic
   * @param total initial amount, that will grow based based on the amount of ids and a random number for each of them
   * @param idArray the array of Strings that are database IDs in the table wrapped by DAO
   * @return The Queue of elements to be sent to the topic, with the id that will be used as Key in that process
   */
  public Queue<Pair<Document, String>> prepareData(int total, List<String> idArray) {

    Random random = new Random();
    Queue<Pair<Document,String>> ids = new LinkedList<>();

    // retrieving the documents from the db and associating them to the consolidations Id that corresponds to them
    List<Pair<Document, String>> pairs = new ArrayList<>(idArray.size());
    for (int j = 0; j < idArray.size(); j++) {
      Document document = dao.get(idArray.get(j));
      String fromConsolidations = service.getFirstConsolidationId(document);
      pairs.add(new Pair<>(document,fromConsolidations));
    }

    while (total > 0) {
      for (int j = 0; j < pairs.size(); j++) {

        Document doc = pairs.get(j).getFirst();
        String fromConsolidations = pairs.get(j).getSecond();

        int amount = random.nextInt(3);
        for (int i = 0; i < amount; i++) {
          ids.add(i % 2 == 0 ? new Pair<>(doc,fromConsolidations) : new Pair<>(doc,doc.getIdString()));
        }
      }
      total--;
    }

    return ids;
  }

  /**
   * Sends messages to the topic from the queue, but before it changes the record updating the values needed for testing and for the final processing in the consumers to work
   * @param ids
   * @param toStream
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void produce(Queue<Pair<Document,String>> ids, int numberOfPartitions,boolean toStream)
      throws ExecutionException, InterruptedException {

    int recordCount = 0;
    while (!ids.isEmpty()) {

      Pair<Document, String> pair = ids.remove();
      Document document = Json.newDocument(pair.getFirst());
      String originalId = document.getIdString();

      service.replaceCustomerNoAndBrandIdFromId(document);
      service.setMarketingEmail(document, "test" + recordCount + "@test.com");
      service.removeConsolidationsArray(document);

      int partition = hash(pair.getSecond(),
          numberOfPartitions); // hashing to the "wrong" partition, based in the "Wrong id"
      //int responsePartition = -1;
      if (toStream) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
            topic,
            partition,
            pair.getSecond(),
            document.asJsonString());
        Future<RecordMetadata> result = producer.send(record);
        //responsePartition = result.get().partition();
      }
      System.out.println(String.format(
          "Producing record with real ID: %s , assigned partition is %d -- it will be found %s",
          originalId, partition,
          originalId.equals(pair.getSecond()) ? " by id." : "in the consolidation array"));
      System.out.println("Processed: " + recordCount++);
    }
    producer.close();
  }

  private int hash(String id, int partitionsNumber) {
    return Utils.abs(Utils.murmur2(id.getBytes())) % partitionsNumber;
  }
  private VBProducer<String, String> createProducer() {

    final Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

    return new VBProducer<>(new KafkaProducer<>(producerProps));
  }

// ---------------------------------------------------------------------------------------

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    int total = 200;  // the number of elements to generate will grow from total to a max times of idsPerPartition * number-of-partitions in the stream
    String tablePathToRead = "/chicos/tables/cu14-3-h100k.db";
    String outputTopic = "/user/mapr/streams/chicos/customer-test:all-from8-2k-4partitions";
    int idsPerPartition = 2;

    MurmurHashIdentifier murmurHashIdentifier = new MurmurHashIdentifier();
    RecordsProducer myProducerWrapper = new RecordsProducer(tablePathToRead, outputTopic);
    int partitions = myProducerWrapper.producer.partitionsFor(outputTopic).size();

    Map<Integer, List<String>> idsPerHash = murmurHashIdentifier.getIdsPerHash(partitions, idsPerPartition, myProducerWrapper.dao);

    List<String> idArray = idsPerHash.entrySet()
        .stream()
        .flatMap(x->x.getValue().stream())
        .collect(Collectors.toList());

    Queue<Pair<Document,String>> ids = myProducerWrapper.prepareData(total, idArray);
    myProducerWrapper.produce(ids, partitions,true);
  }


}
