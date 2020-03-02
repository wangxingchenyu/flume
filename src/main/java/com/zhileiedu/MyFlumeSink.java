package com.zhileiedu;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: wzl
 * @Date: 2020/3/2 12:45
 */
public class MyFlumeSink extends AbstractSink implements Configurable {
	private String prefix;
	private String suffix;
	private static final Logger logger = LoggerFactory.getLogger(MyFlumeSink.class);

	public Status process() throws EventDeliveryException {
		Status status = null;
		// 获取channel
		Channel ch = getChannel();
		// 开启事务
		Transaction txn = ch.getTransaction();
		// 开启事务
		txn.begin();
		try {
			// 带走数据
			Event event;

			// 进行数据处理
			while ((event = ch.take()) == null) {
				Thread.sleep(500);
			}
			byte[] body = event.getBody();
			String s = new String(body, "utf-8");
			logger.info(prefix + s + suffix);
			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
			txn.rollback();
			status = Status.BACKOFF;

			if (t instanceof Error) {
				throw (Error) t;
			}
		} finally {
			// 关闭事务
			txn.close();
		}

		return status;
	}


	public void configure(Context context) {
		prefix = context.getString("prefix", "p_");
		suffix = context.getString("suffix", "_s");
	}
}
