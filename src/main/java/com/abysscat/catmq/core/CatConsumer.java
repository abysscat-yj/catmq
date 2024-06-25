package com.abysscat.catmq.core;

/**
 * cat consumer.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:41
 */
public class CatConsumer<T> {

	CatBroker broker;

	String topic;

	CatMq mq;

	public CatConsumer(CatBroker broker) {
		this.broker = broker;
	}

	public void subscribe(String topic) {
		this.topic = topic;
		mq = broker.find(topic);
		if (mq == null) {
			throw new RuntimeException("topic not found");
		}
	}
	public CatMessage<T> poll(long timeout) {
		return mq.poll(timeout);
	}

	public void listen(CatListener<T> listener) {
		mq.addListener(listener);
	}
}
