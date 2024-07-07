package com.abysscat.catmq.server;

import com.abysscat.catmq.model.Message;
import com.abysscat.catmq.model.Subscription;
import com.abysscat.catmq.store.Store;
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

	@Getter
	private Map<String, Subscription> subscriptions = new HashMap<>();

	@Getter
	private Store store;

	public MessageQueue(String topic) {
		this.topic = topic;
		this.store = new Store(topic);
		this.store.init();
	}

	public int send(Message<String> message) {
		int offset = store.pos();
		// 消息头中加入offset，方便客户端消费后ack
		message.getHeaders().put("X-offset", String.valueOf(offset));
		store.write(message);
		return offset;
	}

	public Message<?> recv(int offset) {
		return store.read(offset);
	}

	public void sub(Subscription subscription) {
		String consumerId = subscription.getConsumerId();
		subscriptions.putIfAbsent(consumerId, subscription);
	}

	public void unsub(Subscription subscription) {
		String consumerId = subscription.getConsumerId();
		subscriptions.remove(consumerId);
	}

}
