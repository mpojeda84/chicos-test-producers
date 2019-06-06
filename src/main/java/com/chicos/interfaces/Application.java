package com.chicos.interfaces;

import com.chicos.interfaces.customer.CustomerDAO;
import com.chicos.interfaces.customer.CustomerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.jetty.util.ArrayQueue;
import org.ojai.Document;

import java.util.*;

public class Application {

    private String topic;

    private KafkaProducer<String, String> producer;

    private CustomerDAO dao;

    private CustomerService service;

    public Application() {
        this.producer = createProducer();
        this.topic = "/user/mapr/streams/chicos/customer-test.st:all1";
        this.dao = new CustomerDAO();
        service = new CustomerService();
    }

    private KafkaProducer<String, String> createProducer() {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

        return new KafkaProducer<>(producerProps);
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
                    service.setMarketingEmail(document, take + "test@test.com");

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

        Application app = new Application();

        int total = 20000;
        Random random = new Random();


        Map<String, Queue<String>> ids = new HashMap<>();

        Iterator<Document> iterator = app.dao.getIterator();

        while(iterator.hasNext() && total > 0) {
            Document document = iterator.next();

            String id = document.getIdString();
            String fromConsolidations = app.service.getFirstConsolidationId(document);

            int amount = random.nextInt(5);
            ids.put(id,new ArrayQueue<>());
            if(fromConsolidations == null) {
                for (int i = 0; i < 3; i++)
                    ids.get(id).add(id);
            } else {
                for (int i = 0; i < amount; i++)
                    ids.get(id).add(i % 2 == 0 ? id : fromConsolidations);
            }

            total--;
        }


        app.produce(ids);


    }


}
