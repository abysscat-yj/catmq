package com.abysscat.catmq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * cat broker.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:21
 */
public class CatBroker {

	private final Map<String, CatMq> mqMapping = new ConcurrentHashMap<>(64);

	public CatMq find(String topic) {
		return mqMapping.get(topic);
	}

	public CatMq createMq(String topic) {
		CatMq mq = new CatMq(topic);
		return mqMapping.putIfAbsent(topic, mq);
	}

	public CatProducer createProducer() {
		return new CatProducer(this);
	}

	public CatConsumer<?> createConsumer(String topic) {
		CatConsumer<?> consumer = new CatConsumer<>(this);
		consumer.subscribe(topic);
		return consumer;
	}

}
