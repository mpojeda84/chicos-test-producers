package com.chicos.interfaces.customer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;


//=== Vadim Brodsky 2019-06-17 -2019-06-17 === 
public class VBProducer <K,V>
{
	private KafkaProducer<K, V> producer;
	private boolean print;

	private static final Logger log = LogManager.getLogger(VBProducer.class.getName());

	private static String millies (long n) 
	{ return String.format("%,dms @", (System.nanoTime()-n)/1000); }
	
	public KafkaProducer<K, V> getProducer() {
		return producer;
	}

	public VBProducer(KafkaProducer<K, V> producer) {
		this.producer = producer;
		print = true;
	}

	public void close() {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { producer.close();	}
		finally { if(print) log.info(millies(n)+m +"()"); }
	}

	public List<PartitionInfo> partitionsFor(String topic) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		List<PartitionInfo> r = null;
		long n = System.nanoTime();
		try { return r = producer.partitionsFor(topic);	}
		finally { if(print) log.info(millies(n)+m +"("+topic+") => "+r); }
	}

	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		Future<RecordMetadata> r = null;
		long n = System.nanoTime();
		try { return r = producer.send(record);	}
		finally { if(print) log.info(millies(n)+m +"("+record+") => "+r); }
	}

	public void flush() {
		String m = !print ? null : new Object(){}.getClass().getEnclosingMethod().getName();
		long n = System.nanoTime();
		try { producer.flush();	}
		finally { if(print) log.info(millies(n)+m +"()"); }
	}
}