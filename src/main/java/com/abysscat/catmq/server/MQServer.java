package com.abysscat.catmq.server;

import com.abysscat.catmq.model.Message;
import com.abysscat.catmq.model.Result;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * MQ server endpoint.
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/2 1:38
 */
@RestController
@RequestMapping("/catmq")
public class MQServer {

	// 发送消息
	@RequestMapping("/send")
	public Result<String> send(@RequestParam("t") String topic,
							   @RequestBody Message<String> message) {
		return Result.ok("" + MessageQueueService.send(topic, message));
	}

	// 拉取消息
	@RequestMapping("/recv")
	public Result<Message<?>> recv(@RequestParam("t") String topic,
								   @RequestParam("cid") String consumerId) {
		return Result.msg(MessageQueueService.recv(topic, consumerId));
	}

	// 批量拉取消息
	@RequestMapping("/batch")
	public Result<List<Message<?>>> batch(@RequestParam("t") String topic,
										  @RequestParam("cid") String consumerId,
										  @RequestParam(name = "size", required = false, defaultValue = "1000") int size) {
		return Result.msg(MessageQueueService.batch(topic, consumerId, size));
	}

	// 确认消息
	@RequestMapping("/ack")
	public Result<String> ack(@RequestParam("t") String topic,
							  @RequestParam("cid") String consumerId,
							  @RequestParam("offset") Integer offset) {
		return Result.ok("" + MessageQueueService.ack(topic, consumerId, offset));
	}

	// 订阅topic
	@RequestMapping("/sub")
	public Result<String> sub(@RequestParam("t") String topic,
							  @RequestParam("cid") String consumerId) {
		MessageQueueService.sub(new MessageSubscription(topic, consumerId, -1));
		return Result.ok();
	}

	// 取消订阅topic
	@RequestMapping("/unsub")
	public Result<String> unsub(@RequestParam("t") String topic,
								@RequestParam("cid") String consumerId) {
		MessageQueueService.unsub(new MessageSubscription(topic, consumerId, -1));
		return Result.ok();
	}

}
