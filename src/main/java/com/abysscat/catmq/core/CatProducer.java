package com.abysscat.catmq.core;

import lombok.AllArgsConstructor;

/**
 * cat producer.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:30
 */
@AllArgsConstructor
public class CatProducer {

	private CatBroker broker;

	public boolean send(String topic, CatMessage<?> message) {
		CatMq mq = broker.find(topic);
		if (mq == null) {
			throw new RuntimeException("topic not found");
		}
		return mq.send(message);
	}

}
