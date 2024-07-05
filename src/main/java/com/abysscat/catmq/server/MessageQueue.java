package com.abysscat.catmq.server;

import com.abysscat.catmq.model.Message;
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

	private Message<?>[] queue = new Message[1024 * 10];

	@Getter
	private Map<String, MessageSubscription> subscriptions = new HashMap<>();

	@Getter
	private int index = 0;

	public MessageQueue(String topic) {
		this.topic = topic;
	}

	public int send(Message<?> message) {
		if (index >= queue.length) {
			return -1;
		}
		// 消息头中加入offset，方便客户端消费后ack
		message.getHeaders().put("X-offset", String.valueOf(index));
		queue[index++] = message;
		return index;
	}

	public Message<?> recv(int ind) {
		if (ind <= index) return queue[ind];
		return null;
	}

	public void sub(MessageSubscription subscription) {
		String consumerId = subscription.getConsumerId();
		subscriptions.putIfAbsent(consumerId, subscription);
	}

	public void unsub(MessageSubscription subscription) {
		String consumerId = subscription.getConsumerId();
		subscriptions.remove(consumerId);
	}

}
