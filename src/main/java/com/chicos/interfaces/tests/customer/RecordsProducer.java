package com.chicos.interfaces.tests.customer;

import com.chicos.interfaces.common.Pair;
import com.chicos.interfaces.customer.CustomerDAO;
import com.chicos.interfaces.customer.CustomerService;
import com.chicos.interfaces.customer.VBProducer;
import com.chicos.interfaces.customer.VBStore;
import com.chicos.interfaces.tests.customer.common.MurmurHashIdentifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.ojai.Document;
import org.ojai.json.Json;

public class RecordsProducer {

	private static final Logger log = LogManager.getLogger(RecordsProducer.class.getName());

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
  public void produce(Queue<Pair<Document,String>> ids, int numberOfPartitions,boolean toStream) {

    // for printing
    Map<String, List<String>> toPrint = new HashMap<>();
    ids.stream().map(x -> x.getFirst().getIdString()).distinct().forEach(x -> {
      toPrint.put(x, new LinkedList<>());
    });
    // end for printing

    int recordCount = 0;
    while (!ids.isEmpty()) {

      Pair<Document, String> pair = ids.remove();

      // for printing
      toPrint.get(pair.getFirst().getIdString()).add(String.valueOf(recordCount));
      // end for printing

      Document document = Json.newDocument(pair.getFirst());
      String originalId = document.getIdString();

      document.setId(pair.getSecond());
      service.replaceCustomerNoAndBrandIdFromId(document);
//      service.setMarketingEmail(document, "test" + recordCount + "@test.com");
      service.setVB(document, recordCount);
      service.removeConsolidationsArray(document);
      service.removeAlternateKeysArray(document);

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
      log.info (String.format(
              "#%d new record with real ID: %s, assigned partition %d - it'll be found %s",
              recordCount++, originalId, partition,
              originalId.equals(pair.getSecond()) ? " by id" : "in the consolidation array"));
    }
    producer.close(); // VB ??

    toPrint.keySet().forEach(x -> {
      System.out.println("Printing email sequence for id: " + x);
      toPrint.get(x).forEach(y -> System.out.print(", "+y));
      System.out.println();
//      toPrint.get(x).forEach(System.out::println);
    });
  }

  private int hash(String id, int partitionsNumber) {
    return Utils.abs(Utils.murmur2(id.getBytes())) % partitionsNumber;
  }
  private VBProducer<String, String> createProducer() {

    final Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

    return new VBProducer<>(new KafkaProducer<>(producerProps));
  }

// ---------------------------------------------------------------------------------------

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, ParseException {

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse( generateOptions(), args);

    String tablePathToRead = commandLine.getOptionValue("s"); //"/chicos/tables/cu14-3-h100k.db";
    String outputTopic = commandLine.getOptionValue("t");  //"/user/mapr/streams/chicos/customer-test:all-from8-2k-4partitions";
    int idsPerPartition = Integer.parseInt(commandLine.getOptionValue("n")); // 2;
    int total = Integer.parseInt(commandLine.getOptionValue("i")); // 200;  // the number of elements to generate will grow from total to a max times of idsPerPartition * number-of-partitions in the stream

    MurmurHashIdentifier murmurHashIdentifier = new MurmurHashIdentifier();
    RecordsProducer myProducerWrapper = new RecordsProducer(tablePathToRead, outputTopic);
    int partitions = myProducerWrapper.producer.partitionsFor(outputTopic).size();

    Map<Integer, List<String>> idsPerHash = murmurHashIdentifier.getIdsPerHash(partitions, idsPerPartition, myProducerWrapper.dao);

    //printing:
    idsPerHash.entrySet().stream()
        .flatMap(x->x.getValue().stream())
        .map(x-> "\"" + x + "\",")
        .forEach(System.out::println);


    List<String> idArray = idsPerHash.entrySet().stream()
        .flatMap(x->x.getValue().stream())
        .collect(Collectors.toList());

    Queue<Pair<Document,String>> ids = myProducerWrapper.prepareData(total, idArray);
    myProducerWrapper.produce(ids, partitions,true);
  }

  private static Options generateOptions() {
    Options options = new Options();
    options.addOption("s", true, "Source Table");
    options.addOption("t", true, "Topic to output to");
    options.addOption("n", true, "Number of unique elements to generate records from");
    options.addOption("i", true, "iterations for each unique element");
    return options;
  }

}
