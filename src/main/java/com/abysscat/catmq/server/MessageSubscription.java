package com.abysscat.catmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Message Subscription.
 *
 * 维护 topic 和 消费者 之间的订阅关系。
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/2 1:38
 */
@Data
@AllArgsConstructor
public class MessageSubscription {

	private String topic;

	private String consumerId;

	private int offset = -1;

}
