package com.abysscat.catmq.server;

import com.abysscat.catmq.model.Message;
import com.abysscat.catmq.model.Stat;
import com.abysscat.catmq.model.Subscription;
import com.abysscat.catmq.store.Indexer;
import com.abysscat.catmq.store.Store;

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
	}

	public static void sub(Subscription subscription) {
		MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
		if (messageQueue == null) throw new RuntimeException("topic not found");
		messageQueue.sub(subscription);
	}

	public static void unsub(Subscription subscription) {
		MessageQueue messageQueue = QUEUES.get(subscription.getTopic());
		if (messageQueue == null) return;
		messageQueue.unsub(subscription);
	}

	public static int send(String topic, Message<String> message) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		return messageQueue.send(message);
	}

	public static Message<?> recv(String topic, String consumerId, int offset) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			return messageQueue.recv(offset);
		}
		throw new RuntimeException("subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}

	// 使用此方法，需要手工调用ack，更新订阅关系里的offset
	public static Message<?> recv(String topic, String consumerId) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			int offset = messageQueue.getSubscriptions().get(consumerId).getOffset();
			int nextOffset = 0;
			if(offset > -1) {
				// 如果ack了offset，则从ack的offset下一个消息位置开始读取
				Indexer.Entry entry = Indexer.getEntry(topic, offset);
				nextOffset = offset + entry.getLength();
			}
			return messageQueue.recv(nextOffset);
		}
		throw new RuntimeException("subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}

	public static int ack(String topic, String consumerId, int offset) {
		MessageQueue messageQueue = QUEUES.get(topic);
		if (messageQueue == null) throw new RuntimeException("topic not found");
		if (messageQueue.getSubscriptions().containsKey(consumerId)) {
			Subscription subscription = messageQueue.getSubscriptions().get(consumerId);
			if (offset > subscription.getOffset() && offset <= Store.FILE_LEN) {
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
			// 获取当前订阅关系里的offset
			int offset = messageQueue.getSubscriptions().get(consumerId).getOffset();
			int nextOffset = 0;
			if(offset > -1) {
				Indexer.Entry entry = Indexer.getEntry(topic, offset);
				nextOffset = offset + entry.getLength();
			}
			List<Message<?>> result = new ArrayList<>();
			Message<?> recv = messageQueue.recv(nextOffset);
			while (recv != null) {
				result.add(recv);
				if (result.size() >= size) break;
				Indexer.Entry entry = Indexer.getEntry(topic, nextOffset);
				nextOffset = nextOffset + entry.getLength();
				recv = messageQueue.recv(nextOffset);
			}
			System.out.println("batch last recv message: " + recv);
			return result;
		}
		throw new RuntimeException("batch subscriptions not found for topic/consumerId = "
				+ topic + "/" + consumerId);
	}

	public static Stat stat(String topic, String consumerId) {
		MessageQueue queue = QUEUES.get(topic);
		Subscription subscription = queue.getSubscriptions().get(consumerId);
		return new Stat(subscription, queue.getStore().total(), queue.getStore().pos());
	}
}
