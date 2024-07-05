package com.abysscat.catmq.demo;

import com.abysscat.catmq.client.CatBroker;
import com.abysscat.catmq.client.CatConsumer;
import com.abysscat.catmq.model.Message;
import com.abysscat.catmq.client.CatProducer;
import com.alibaba.fastjson.JSON;
import lombok.SneakyThrows;

/**
 * order demo.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:45
 */
public class Demo {

	@SneakyThrows
	public static void main(String[] args) {

		long ids = 0;

		String topic = "com.abysscat.catmq.test";

		CatBroker broker = CatBroker.Default;

		CatProducer producer = broker.createProducer();
		CatConsumer<?> consumer = broker.createConsumer(topic);

		// 当前消费者注册回调方法
		consumer.listen(topic, message -> {
			System.out.println(" onMessage => " + message); // 这里处理消息
		});

		for (int i = 0; i < 10; i++) {
			Order order = new Order(ids, "item" + ids, 100 * ids);
			producer.send(topic, new Message<>((long) ids++, JSON.toJSONString(order), null, null));
		}

		while (true) {
			char c = (char) System.in.read();
			if (c == 'q' || c == 'e') {
				consumer.unsub(topic);
				break;
			}
			if (c == 'p') {
				Order order = new Order(ids, "item" + ids, 100 * ids);
				producer.send(topic, new Message<>(ids++, JSON.toJSONString(order), null, null));
				System.out.println("produce ok => " + order);
			}
			if (c == 'c') {
				Message<String> message = (Message<String>) consumer.recv(topic);
				System.out.println("consume ok => " + message);
				consumer.ack(topic, message);
			}
			if (c == 'a') {
				for (int i = 0; i < 10; i++) {
					Order order = new Order(ids, "item" + ids, 100 * ids);
					producer.send(topic, new Message<>((long) ids++, JSON.toJSONString(order), null, null));
				}
				System.out.println("produce 10 orders...");
			}
		}


	}
}
