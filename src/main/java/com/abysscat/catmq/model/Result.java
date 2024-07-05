package com.abysscat.catmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * MQServer Result.
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/2 1:36
 */
@AllArgsConstructor
@Data
public class Result<T> {

	private int code; // 1:success, 0:fail

	private T data;

	public static Result<String> ok() {
		return new Result<>(1, "OK");
	}

	public static Result<String> ok(String msg) {
		return new Result<>(1, msg);
	}

	public static Result<Message<?>> msg(String msg) {
		return new Result<>(1, Message.create(msg, null, null));
	}

	public static Result<Message<?>> msg(Message<?> msg) {
		return new Result<>(1, msg);
	}

	public static Result<List<Message<?>>> msg(List<Message<?>> msg) {
		return new Result<>(1, msg);
	}
}
