package com.abysscat.catmq.server;

import com.abysscat.catmq.model.CatMessage;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Description
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/2 1:55
 */
public class MessageQueue {

	private String topic;

	private CatMessage<?>[] queue = new CatMessage[1024 * 10];

	@Getter
	private Map<String, MessageSubscription> subscriptions = new HashMap<>();

	@Getter
	private int index = 0;

	public MessageQueue(String topic) {
		this.topic = topic;
	}

	public int send(CatMessage<?> message) {
		if (index >= queue.length) {
			return -1;
		}
		queue[index++] = message;
		return index;
	}

	public CatMessage<?> recv(int ind) {
		if (ind <= index) return queue[ind];
		return null;
	}

	public void subscribe(MessageSubscription subscription) {
		String consumerId = subscription.getConsumerId();
		subscriptions.putIfAbsent(consumerId, subscription);
	}

	public void unsubscribe(MessageSubscription subscription) {
		String consumerId = subscription.getConsumerId();
		subscriptions.remove(consumerId);
	}

}
