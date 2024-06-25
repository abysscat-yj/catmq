package com.abysscat.catmq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * order entity.
 *
 * @Author: abysscat-yj
 * @Create: 2024/6/26 1:44
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Order {

	private long id;

	private String item;

	private double price;

}
