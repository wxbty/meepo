
package org.feisoft.jta.work;

import org.feisoft.transaction.TransactionBeanFactory;
import org.feisoft.transaction.TransactionRecovery;
import org.feisoft.transaction.aware.TransactionBeanFactoryAware;
import org.feisoft.transaction.supports.TransactionTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.resource.spi.work.Work;

public class TransactionWork implements Work, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionWork.class);

	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;

	static final long SECOND_MILLIS = 1000L;
	private long stopTimeMillis = -1;
	private long delayOfStoping = SECOND_MILLIS * 15;
	private long recoveryInterval = SECOND_MILLIS * 60;

	public void run() {

		TransactionTimer transactionTimer = beanFactory.getTransactionTimer();
		TransactionRecovery transactionRecovery = beanFactory.getTransactionRecovery();
		try {
			transactionRecovery.startRecovery();
			transactionRecovery.timingRecover();
		} catch (RuntimeException rex) {
			logger.error("TransactionRecovery init failed!", rex);
		}

		long nextExecutionTime = 0;
		long nextRecoveryTime = System.currentTimeMillis() + this.recoveryInterval;
		while (this.currentActive()) {

			long current = System.currentTimeMillis();
			if (current >= nextExecutionTime) {
				nextExecutionTime = current + SECOND_MILLIS;
				try {
					transactionTimer.timingExecution();
				} catch (RuntimeException rex) {
					logger.error(rex.getMessage(), rex);
				}
			}

			if (current >= nextRecoveryTime) {
				nextRecoveryTime = current + this.recoveryInterval;
				try {
					transactionRecovery.timingRecover();
				} catch (RuntimeException rex) {
					logger.error(rex.getMessage(), rex);
				}
			}

			this.waitForMillis(100L);

		} // end-while (this.currentActive())
	}

	private void waitForMillis(long millis) {
		try {
			Thread.sleep(millis);
		} catch (Exception ignore) {
			// ignore
		}
	}

	public void release() {
		this.stopTimeMillis = System.currentTimeMillis() + this.delayOfStoping;
	}

	protected boolean currentActive() {
		return this.stopTimeMillis <= 0 || System.currentTimeMillis() < this.stopTimeMillis;
	}

	public long getDelayOfStoping() {
		return delayOfStoping;
	}

	public void setDelayOfStoping(long delayOfStoping) {
		this.delayOfStoping = delayOfStoping;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}
}
