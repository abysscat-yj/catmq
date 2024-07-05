package com.abysscat.catmq.client;

import com.abysscat.catmq.model.Message;

/**
 * message listener.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:55
 */
public interface CatListener<T> {

	void onMessage(Message<T> message);

}
