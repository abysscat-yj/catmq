package com.abysscat.catmq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * indexer for store entry.
 * <p>
 * 维护 MQ 持久化文件中每个消息存储的下标和长度，方便从中依次读取消息。
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/7 21:15
 */
@Slf4j
public class Indexer {

	@AllArgsConstructor
	@Data
	public static class Entry {
		int offset;
		int length;
	}

	/**
	 * 索引表，key 为 topic，value 为 topic 下所有消息的索引信息列表。
	 */
	static MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();

	/**
	 * 映射表，key 为 topic，value 为 topic 下所有消息 map（方便根据 offset 找到某个消息 entry）。
	 */
	static Map<String, Map<Integer, Entry>> mappings = new HashMap<>();

	public static void addEntry(String topic, int offset, int length) {
		log.info("=========> add entry: topic={}, offset={}, length={}", topic, offset, length);
		Entry entry = new Entry(offset, length);
		indexes.add(topic, entry);
		putMapping(topic, offset, entry);
	}

	private static void putMapping(String topic, int offset, Entry value) {
		mappings.computeIfAbsent(topic, k -> new HashMap<>()).put(offset, value);
	}

	public static List<Entry> getEntries(String topic) {
		return indexes.get(topic);
	}

	public static Entry getEntry(String topic, int offset) {
		Map<Integer, Entry> map = mappings.get(topic);
		return map == null ? null : map.get(offset);
	}

}
