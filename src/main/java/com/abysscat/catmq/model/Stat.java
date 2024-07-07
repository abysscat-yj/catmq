package com.abysscat.catmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * stats for mq.
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/7 23:45
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class Stat {

	private Subscription subscription;

	private int total;

	private int position;

}
