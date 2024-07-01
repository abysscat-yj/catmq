package com.abysscat.catmq.server;

import com.abysscat.catmq.model.CatMessage;
import com.abysscat.catmq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * MQ server endpoint.
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/2 1:38
 */
@Controller
@RequestMapping("/catmq")
public class MQServer {

	// 发送消息
	@RequestMapping("/send")
	public Result<String> send(@RequestParam("t") String topic,
							   @RequestParam("cid") String consumerId,
							   @RequestBody CatMessage<String> message) {
		return Result.ok("" + MessageQueueService.send(topic, consumerId, message));
	}

	// 接收消息
	@RequestMapping("/recv")
	public Result<CatMessage<?>> recv(@RequestParam("t") String topic,
									  @RequestParam("cid") String consumerId) {
		return Result.msg(MessageQueueService.recv(topic, consumerId));
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
	public Result<String> subscribe(@RequestParam("t") String topic,
									@RequestParam("cid") String consumerId) {
		MessageQueueService.sub(new MessageSubscription(topic, consumerId, -1));
		return Result.ok();
	}

	// 取消订阅topic
	@RequestMapping("/unsub")
	public Result<String> unsubscribe(@RequestParam("t") String topic,
									  @RequestParam("cid") String consumerId) {
		MessageQueueService.unsub(new MessageSubscription(topic, consumerId, -1));
		return Result.ok();
	}

}
