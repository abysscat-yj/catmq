package com.abysscat.catmq.demo;

import com.abysscat.catmq.client.CatBroker;
import com.abysscat.catmq.client.CatConsumer;
import com.abysscat.catmq.model.CatMessage;
import com.abysscat.catmq.client.CatProducer;
import lombok.SneakyThrows;

/**
 * order demo.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:45
 */
public class OrderDemo {

	@SneakyThrows
	public static void main(String[] args) {

		long count = 0;

		String topic = "cat.topic.order";

		CatBroker broker = new CatBroker();
		broker.createMq(topic);

		CatProducer producer = broker.createProducer();
		CatConsumer<?> consumer = broker.createConsumer(topic);

		// 添加回调方法
		consumer.listen(message -> {
			System.out.println(" onMessage => " + message);
		});

		for (int i = 0; i < 10; i++) {
			Order order = new Order(count, "item" + count, 100 * count);
			producer.send(topic, new CatMessage<>(count++, order, null, null));
		}

		for (int i = 0; i < 10; i++) {
			CatMessage<Order> message = (CatMessage<Order>) consumer.poll(1000);
			System.out.println(message);
		}

		while (true) {
			char c = (char) System.in.read();
			if (c == 'q' || c == 'e') {
				break;
			}
			if (c == 'p') {
				Order order = new Order(count, "item" + count, 100 * count);
				producer.send(topic, new CatMessage<>(count++, order, null, null));
				System.out.println("send ok => " + order);
			}
			if (c == 'c') {
				CatMessage<Order> message = (CatMessage<Order>) consumer.poll(1000);
				System.out.println("poll ok => " + message);
			}
			if (c == 'a') {
				for (int i = 0; i < 10; i++) {
					Order order = new Order(count, "item" + count, 100 * count);
					producer.send(topic, new CatMessage<>(count++, order, null, null));
				}
				System.out.println("send 10 orders...");
			}
		}


	}
}
