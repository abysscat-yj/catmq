package com.abysscat.catmq.store;

import com.abysscat.catmq.model.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * mq store operator.
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/7 21:36
 */
public class Store {

	private String topic;

	public static final int FILE_LEN = 1024 * 10; // 10k字节长度，会占据内存空间

	public Store(String topic) {
		this.topic = topic;
	}

	@Getter
	MappedByteBuffer mappedByteBuffer = null;

	@SneakyThrows
	public void init() {
		File file = new File(topic + ".dat");
		if (!file.exists()) {
			file.createNewFile();
		}

		Path path = Paths.get(file.getAbsolutePath());
		FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);

		mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, FILE_LEN);

		// 启动时读取文件内容，加载消息offset，并且初始化下次写入位置
		ByteBuffer buffer = mappedByteBuffer.asReadOnlyBuffer();
		byte[] header = new byte[10];	// 10字节，消息头
		buffer.get(header);
		int pos = 0;
		while(header[9] > 0) {
			String trim = new String(header, StandardCharsets.UTF_8).trim();
			System.out.println(trim);
			int len = Integer.parseInt(trim) + 10;
			Indexer.addEntry(topic, pos, len);
			pos += len;
			System.out.println(" next = " + pos);
			buffer.position(pos);
			buffer.get(header);
		}
		buffer = null;
		System.out.println("init pos = " + pos);
		mappedByteBuffer.position(pos);

		// todo 如果总数据超过单个文件限制，使用多个数据文件的list来管理持久化数据
	}

	public int write(Message<String> km) {
		System.out.println(" write pos -> " + mappedByteBuffer.position());
		String msg = JSON.toJSONString(km);
		int len = msg.getBytes(StandardCharsets.UTF_8).length;
		// 消息头，10字节，消息长度
		String head = String.format("%010d", len);
		msg = head + msg;
		len = len +10;
		int position = mappedByteBuffer.position();
		// 记录当前消息的开始offset和长度
		Indexer.addEntry(topic, position, len);
		// 写入消息后，position会自动指向下一个消息的开始位置
		mappedByteBuffer.put(StandardCharsets.UTF_8.encode(msg));
		return position;
	}

	public int pos() {
		return mappedByteBuffer.position();
	}

	public Message<String> read(int offset) {
		ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
		Indexer.Entry entry = Indexer.getEntry(topic, offset);
		readOnlyBuffer.position(entry.getOffset() + 10);
		int len = entry.getLength() - 10;
		byte[] bytes = new byte[len];
		readOnlyBuffer.get(bytes, 0, len);
		String json = new String(bytes, StandardCharsets.UTF_8);
		System.out.println("  read json ==>> " + json);
		Message<String> message = JSON.parseObject(json, new TypeReference<Message<String>>() {
		});
		return message;
	}

	public int total() {
		return Indexer.getEntries(topic).size();
	}

}
