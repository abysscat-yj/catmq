package com.abysscat.catmq.core;

/**
 * message listener.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:55
 */
public interface CatListener<T> {

	void onMessage(CatMessage<T> message);

}
