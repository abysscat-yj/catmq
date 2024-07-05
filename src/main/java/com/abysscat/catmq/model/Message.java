package com.abysscat.catmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * cat message class.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message<T> {

	private Long id;

	private T body;

	/**
	 * 系统属性
	 */
	private Map<String, String> headers = new HashMap<>();

	/**
	 * 业务属性
	 */
	private Map<String, String> properties = new HashMap<>();

	static AtomicLong ID_GEN = new AtomicLong(0);

	public static long genId() {
		return ID_GEN.incrementAndGet();
	}

	public static Message<String> create(
			String body, Map<String, String> headers, Map<String, String> properties) {
		return new Message<>(genId(), body, headers, properties);
	}

}
