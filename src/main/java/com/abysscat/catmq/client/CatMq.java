package com.abysscat.catmq.client;

import com.abysscat.catmq.model.CatMessage;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * cat mq.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:25
 */
public class CatMq {

	private String topic;

	private final LinkedBlockingQueue<CatMessage> queue = new LinkedBlockingQueue<>();

	private List<CatListener> listeners = new ArrayList<>();

	public CatMq(String topic) {
		this.topic = topic;
	}

	public boolean send(CatMessage<?> message){
		boolean offered = queue.offer(message);
		listeners.forEach(listener -> listener.onMessage(message));
		return offered;
	}

	// 拉模式
	@SneakyThrows
	public <T> CatMessage<T> poll(long timeout)  {
		return queue.poll(timeout, TimeUnit.MILLISECONDS);
	}

	// 推模式，添加监听器
	public <T> void addListener(CatListener<T> listener) {
		listeners.add(listener);
	}
}
