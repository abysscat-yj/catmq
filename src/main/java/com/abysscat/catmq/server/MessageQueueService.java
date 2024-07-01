package com.abysscat.catmq.server;

import com.abysscat.catmq.model.CatMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * Message queue operations.
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/2 1:41
 */
public class MessageQueueService {

	private static final Map<String, MessageQueue> QUEUES = new HashMap<>();

	private static final String TEST_TOPIC = "com.abysscat.catmq.test";

	static {
		QUEUES.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
	}

	public static void sub(MessageSubscription subscription) {
		MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
		if (messageQueue == null) throw new RuntimeException("topic not found");
		messageQueue.subscribe(subscription);
	}

	public static void unsub(MessageSubscription subscription) {
		MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
		if (messageQueue == null) return;
		messageQueue.unsubscribe(subscription);
	}

	public static int send(String topic, String consumerId, CatMessage<String> message) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		return messageQueue.send(message);
	}

	public static CatMessage<?> recv(String topic, String consumerId, int ind) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			return messageQueue.recv(ind);
		}
		throw new RuntimeException("subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}

	// 使用此方法，需要手工调用ack，更新订阅关系里的offset
	public static CatMessage<?> recv(String topic, String consumerId) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			int ind = messageQueue.getSubscriptions().get(consumerId).getOffset();
			return messageQueue.recv(ind);
		}
		throw new RuntimeException("subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}

	public static int ack(String topic, String consumerId, int offset) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			MessageSubscription subscription = messageQueue.getSubscriptions().get(consumerId);
			if (offset > subscription.getOffset() && offset <= messageQueue.getIndex()) {
				subscription.setOffset(offset);
				return offset;
			}
			return -1;
		}
		throw new RuntimeException("subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}

}
