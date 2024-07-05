package com.abysscat.catmq.client;

import com.abysscat.catmq.model.Message;
import com.abysscat.catmq.model.Result;
import com.abysscat.catutils.utils.HttpUtils;
import com.abysscat.catutils.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * cat broker.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:21
 */
public class CatBroker {

	@Getter
	public static CatBroker Default = new CatBroker();

	public static String bokerUrl = "http://localhost:8765/catmq";

	static {
		init();
	}

	public static void init() {
		ThreadUtils.getDefault().init(1);
		ThreadUtils.getDefault().schedule(() -> {
			// 获取当前broker所有消费者
			MultiValueMap<String, CatConsumer<?>> consumers = getDefault().getConsumers();

			// 遍历消费者，消费消息
			consumers.forEach((topic, consumers1) -> consumers1.forEach(consumer -> {
				Message<?> recv = consumer.recv(topic);
				if (recv == null) return;
				try {
					consumer.getListener().onMessage(recv);
					consumer.ack(topic, recv);
				} catch (Exception ex) {
					// TODO
				}
			}));

		}, 100, 100);
	}

	public CatProducer createProducer() {
		return new CatProducer(this);
	}

	public CatConsumer<?> createConsumer(String topic) {
		CatConsumer<?> consumer = new CatConsumer<>(this);
		consumer.sub(topic);
		return consumer;
	}

	public boolean send(String topic, Message message) {
		System.out.println(" ==>> send topic/message: " + topic + "/" + message);
		System.out.println(JSON.toJSONString(message));
		Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message),
				bokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>() {
				});
		System.out.println(" ==>> send result: " + result);
		return result.getCode() == 1;
	}

	public void sub(String topic, String cid) {
		System.out.println(" ==>> sub topic/cid: " + topic + "/" + cid);
		Result<String> result = HttpUtils.httpGet(bokerUrl + "/sub?t=" + topic + "&cid=" + cid,
				new TypeReference<Result<String>>() {
				});
		System.out.println(" ==>> sub result: " + result);
	}

	public <T> Message<T> recv(String topic, String id) {
		System.out.println(" ==>> recv topic/id: " + topic + "/" + id);
		Result<Message<String>> result = HttpUtils.httpGet(
				bokerUrl + "/recv?t=" + topic + "&cid=" + id,
				new TypeReference<Result<Message<String>>>() {
				});
		System.out.println(" ==>> recv result: " + result);
		return (Message<T>) result.getData();
	}

	public void unsub(String topic, String cid) {
		System.out.println(" ==>> unsub topic/cid: " + topic + "/" + cid);
		Result<String> result = HttpUtils.httpGet(bokerUrl + "/unsub?t=" + topic + "&cid=" + cid,
				new TypeReference<Result<String>>() {
				});
		System.out.println(" ==>> unsub result: " + result);
	}

	public boolean ack(String topic, String cid, int offset) {
		System.out.println(" ==>> ack topic/cid/offset: " + topic + "/" + cid + "/" + offset);
		Result<String> result = HttpUtils.httpGet(
				bokerUrl + "/ack?t=" + topic + "&cid=" + cid + "&offset=" + offset,
				new TypeReference<Result<String>>() {
				});
		System.out.println(" ==>> ack result: " + result);
		return result.getCode() == 1;
	}

	@Getter
	private MultiValueMap<String, CatConsumer<?>> consumers = new LinkedMultiValueMap<>();

	public void addConsumer(String topic, CatConsumer<?> consumer) {
		consumers.add(topic, consumer);
	}
}
