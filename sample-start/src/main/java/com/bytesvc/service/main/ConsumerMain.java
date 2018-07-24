package com.bytesvc.service.main;

import com.bytesvc.ServiceException;
import com.bytesvc.service.ITransferService;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 远程调用场景
 */
public class ConsumerMain {

	static ClassPathXmlApplicationContext context = null;
	@javax.annotation.Resource(name = "jdbcTemplate2")
	private static  JdbcTemplate jdbcTemplate;


	public static void main(String... args) throws Throwable {
		System.out.println("-------begin consumer main----------");
		startup();

//		jdbcTemplate = (JdbcTemplate)context.getBean("jdbcTemplate2");
//		jdbcTemplate.update("delete from  apple where 1=1");
		ITransferService transferSvc = (ITransferService) context.getBean("genericTransferService");
		try {
//			for (int tnum = 0;tnum < 200;tnum++) {
//				Thread thread = new MyThread(transferSvc);
//				thread.start();
//			}
//			waitForMillis(6000);
			transferSvc.transfer("1001", "2001", 1.00d);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			shutdown();
		}

	}

	static class MyThread extends Thread {

		private  ITransferService transferSvc;
		public MyThread(ITransferService transferSvc) {
			this.transferSvc = transferSvc;
		}

		@Override
		public void run() {

			try {
				transferSvc.transfer("1001", "2001", 1.00d);
			} catch (ServiceException e) {
				e.printStackTrace();
			}
//			System.out.println(transferSvc.getSum());
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
