package com.zhileiedu;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

/**
 * @Author: wzl
 * @Date: 2020/3/2 10:11
 */
public class MyFlumeSource extends AbstractSource implements Configurable, PollableSource {

	private long delay;
	private String field;

	public Status process() throws EventDeliveryException {
		try {
			// 设置Eveent header
			HashMap<String, String> headers = new HashMap<String, String>();
			SimpleEvent event = new SimpleEvent();
			for (int i = 0; i < 5; i++) {
				event.setHeaders(headers);
				event.setBody((field + i).getBytes());
				getChannelProcessor().processEvent(event);
			}
		} catch (Exception e) {
			return Status.BACKOFF;
		}
		return Status.READY;
	}

	public long getBackOffSleepIncrement() {
		return 0;
	}

	public long getMaxBackOffSleepInterval() {
		return 0;
	}

	public void configure(Context context) {
		delay = context.getLong("delay", 2000L);
		field = context.getString("field", "atguigu");
	}
}
