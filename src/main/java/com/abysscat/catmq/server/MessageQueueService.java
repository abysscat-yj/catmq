package com.abysscat.catmq.server;

import com.abysscat.catmq.model.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

	// TODO 测试用，正常情况需要在配置平台提前创建
	static {
		QUEUES.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
		QUEUES.put("a", new MessageQueue("a"));
	}

	public static void sub(MessageSubscription subscription) {
		MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
		if (messageQueue == null) throw new RuntimeException("topic not found");
		messageQueue.sub(subscription);
	}

	public static void unsub(MessageSubscription subscription) {
		MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
		if (messageQueue == null) return;
		messageQueue.unsub(subscription);
	}

	public static int send(String topic, Message<String> message) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		return messageQueue.send(message);
	}

	public static Message<?> recv(String topic, String consumerId, int ind) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			return messageQueue.recv(ind);
		}
		throw new RuntimeException("subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}

	// 使用此方法，需要手工调用ack，更新订阅关系里的offset
	public static Message<?> recv(String topic, String consumerId) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			int ind = messageQueue.getSubscriptions().get(consumerId).getOffset();
			Message<?> recv = messageQueue.recv(ind + 1);
			System.out.println("recv message: " + recv);
			return recv;
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

	public static List<Message<?>> batch(String topic, String consumerId, int size) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			// 获取当前订阅关系里的offset，然后从offset+1开始取消息
			int offset = messageQueue.getSubscriptions().get(consumerId).getOffset() + 1;
			List<Message<?>> result = new ArrayList<>();
			Message<?> recv = messageQueue.recv(offset);
			while (recv != null) {
				result.add(recv);
				if (result.size() >= size) break;
				recv = messageQueue.recv(++offset);
			}
			System.out.println("batch last recv message: " + recv);
			return result;
		}
		throw new RuntimeException("batch subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}
}
