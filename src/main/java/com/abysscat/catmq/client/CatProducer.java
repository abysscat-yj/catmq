package com.abysscat.catmq.client;

import com.abysscat.catmq.model.Message;
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

	public boolean send(String topic, Message<?> message) {
		return broker.send(topic, message);
	}

}
