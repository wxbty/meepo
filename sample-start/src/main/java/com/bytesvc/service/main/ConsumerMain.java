package com.bytesvc.service.main;

import com.bytesvc.service.ITransferService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 远程调用场景
 */
public class ConsumerMain {

	static ClassPathXmlApplicationContext context = null;

	public static void main(String... args) throws Throwable {
		System.out.println("-------remove test---begin consumer main----------");
		startup();

		ITransferService transferSvc = (ITransferService) context.getBean("genericTransferService");
		try {
			transferSvc.transfer("1001", "2001", 1.00d);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			//shutdown();
		}

	}

	public static void startup() {
		context = new ClassPathXmlApplicationContext("application-consumer.xml");
		context.start();
		waitForMillis(1000);
	}

	public static void waitForMillis(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
	}

	public static void shutdown() {
		waitForMillis(1000 * 5);
		if (context != null) {
			context.close();
		}
		System.exit(0);
	}

}
