package com.abysscat.catmq.store;

import com.abysscat.catmq.model.Message;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
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
import java.util.Scanner;

/**
 * store test demo.
 *
 * @Author: abysscat-yj
 * @Create: 2024/7/7 0:37
 */
public class StoreDemo {


	@SneakyThrows
	public static void main(String[] args) {
		String content = """
				com.abysscat.catmq.store.
				mq store file.
				""";

		int length = content.getBytes(StandardCharsets.UTF_8).length;
		System.out.println("file content len = " + length);

		// 创建文件
		File file = new File("test.dat");
		if (!file.exists()) {
			file.createNewFile();
		}

		Path path = Paths.get(file.getAbsolutePath());
		try (FileChannel channel = (FileChannel) Files.newByteChannel(path,
				StandardOpenOption.READ, StandardOpenOption.WRITE)) {
			MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);

			// 向 mappedByteBuffer 中写入数据，会直接同步到文件
			for (int i = 0; i < 10; i++) {
				System.out.println(i + " => " + mappedByteBuffer.position());
				Message<String> km = Message.create(i + ":" + content, null, null);
				String msg = JSON.toJSONString(km);

				// 写入当前消息在文件中的位置索引和长度
				Indexer.addEntry("test", mappedByteBuffer.position(), msg.getBytes(StandardCharsets.UTF_8).length);

				mappedByteBuffer.put(StandardCharsets.UTF_8.encode(JSON.toJSONString(km)));
			}

			// 从 mappedByteBuffer 中读取内容
			ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
			Scanner sc = new Scanner(System.in);
			while (sc.hasNext()) {
				// 输入文件中的某个offset，读取后面的消息内容
				String line = sc.nextLine();
				if (line.equals("q")) break;
				System.out.println(" IN = " + line);
				int offset = Integer.parseInt(line);
				Indexer.Entry entry = Indexer.getEntry("test", offset);

				// 从指定offset开始，读取后面一个消息内容
				readOnlyBuffer.position(entry.getOffset());
				int len = entry.getLength();

				byte[] bytes = new byte[len];
				readOnlyBuffer.get(bytes, 0, len);
				String s = new String(bytes, StandardCharsets.UTF_8);
				System.out.println("  read only ==>> " + s);
				Message<String> message = JSON.parseObject(s, new TypeReference<Message<String>>() {
				});
				System.out.println(" message.body = " + message.getBody());
			}
		}


	}
}
