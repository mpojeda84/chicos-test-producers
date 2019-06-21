package com.chicos.interfaces.tests.customer;

import com.chicos.interfaces.customer.CustomerDAO;
import com.chicos.interfaces.customer.CustomerService;
import com.chicos.interfaces.customer.VBProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.jetty.util.ArrayQueue;
import org.ojai.Document;

import java.util.*;

@Deprecated
public class TestProducer {

    private String topic;

//    private KafkaProducer<String, String> producer;
    private VBProducer<String, String> producer;

    private CustomerDAO dao;
    private CustomerService service;

    public TestProducer(String tablePathToRead, String streamTopicToWrite) {
        this.producer = createProducer();
        this.topic = streamTopicToWrite;
        this.dao = new CustomerDAO(tablePathToRead, false);
        this.service = new CustomerService();
    }

//	private KafkaProducer<String, String> createProducer() {
    private VBProducer<String, String> createProducer() {
    	
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

        return new VBProducer<>(new KafkaProducer<>(producerProps));
//		return new KafkaProducer<>(producerProps);
    }

    public void produce( Map<String, Queue<String>> ids) {

        Random random = new Random();

        int count = 0;
        for (String x : ids.keySet()) {
            try {
                int take = 1 + random.nextInt(3);
                while (take > 0) {
                	
                    String id = ids.get(x).isEmpty() ? null : ids.get(x).remove();
                    if (id == null)
                        break;
                    
                    Document document = dao.get(x);
                    document.setId(id);
                    
                    service.replaceCustomerNoAndBrandIdFromId(document);
                    service.setMarketingEmail(document, take + "test@test.com");
                    service.removeConsolidationsArray(document);

                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, document.asJsonString());
                    producer.send(record);
                    
                    count++;
                    take--;
                }
                if (count % 10 == 0)
                    System.out.println(count + " elements sent so far");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    //----------------------------------------------

    public static void main(String[] args) {

    	TestProducer tp = new TestProducer ("/user/mapr/tables/chicos/customer.db", "/tmp/customer-test.st:all-2k");

        int total = 2000;
        try { total = Integer.parseInt(args[0]); }
        catch (Exception e) { }
        
        Random random = new Random();

        Map<String, Queue<String>> ids = new HashMap<>();

        Iterator<Document> iterator = tp.dao.getIterator();
        while(iterator.hasNext() && total > 0) {
            Document document = iterator.next();

            String id = document.getIdString();
            String fromConsolidations = tp.service.getFirstConsolidationId(document);

            int amount = random.nextInt(5);
            ids.put(id,new ArrayQueue<>());
            if(fromConsolidations == null)
                for (int i = 0; i < 3; i++)
                    ids.get(id).add(id);
            else
                for (int i = 0; i < amount; i++)
                    ids.get(id).add(i % 2 == 0 ? id : fromConsolidations);

            total--;
        }

        tp.produce(ids);
    }
}
