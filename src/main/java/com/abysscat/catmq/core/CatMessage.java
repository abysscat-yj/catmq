package com.abysscat.catmq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * cat message class.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CatMessage<T> {

	private Long id;

	private T body;

	/**
	 * 系统属性
	 */
	private Map<String, String> headers;

	/**
	 * 业务属性
	 */
	private Map<String, String> properties;

}
