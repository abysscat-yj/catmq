package com.abysscat.catmq.client;

import com.abysscat.catmq.model.Message;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * cat consumer.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:41
 */
public class CatConsumer<T> {

	private String id;
	CatBroker broker;

	@Getter
	private CatListener listener;

	static AtomicInteger idgen = new AtomicInteger(0);

	public CatConsumer(CatBroker broker) {
		this.broker = broker;
		this.id = "CID" + idgen.getAndIncrement();
	}

	public void sub(String topic) {
		broker.sub(topic, id);
	}

	public void unsub(String topic) {
		broker.unsub(topic, id);
	}

	public Message<T> recv(String topic) {
		return broker.recv(topic, id);
	}

	public boolean ack(String topic, int offset) {
		return broker.ack(topic, id, offset);
	}

	public boolean ack(String topic, Message<?> message) {
		int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
		return ack(topic, offset);
	}

	public void listen(String topic, CatListener<T> listener) {
		this.listener = listener;
		broker.addConsumer(topic, this);
	}

}
