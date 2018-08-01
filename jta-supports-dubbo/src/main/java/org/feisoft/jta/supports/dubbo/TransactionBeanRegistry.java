
package org.feisoft.jta.supports.dubbo;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.TransactionBeanFactory;
import org.feisoft.transaction.aware.TransactionBeanFactoryAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionBeanRegistry implements TransactionBeanFactoryAware, EnvironmentAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionBeanRegistry.class);

	private static final TransactionBeanRegistry instance = new TransactionBeanRegistry();

	private RemoteCoordinator consumeCoordinator;
	private Environment environment;
	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;

	private Lock lock = new ReentrantLock();
	private Condition condition = this.lock.newCondition();

	private TransactionBeanRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static TransactionBeanRegistry getInstance() {
		return instance;
	}

	public RemoteCoordinator getConsumeCoordinator() {
		if (this.consumeCoordinator != null) {
			return this.consumeCoordinator;
		} else {
			return this.doGetConsumeCoordinator();
		}
	}

	private RemoteCoordinator doGetConsumeCoordinator() {
		try {
			this.lock.lock();
			while (this.consumeCoordinator == null) {
				try {
					this.condition.await(1, TimeUnit.SECONDS);
				} catch (InterruptedException ex) {
					logger.debug(ex.getMessage());
				}
			}

			// ConsumeCoordinator is injected by the TransactionConfigPostProcessor, which has a slight delay.
			return consumeCoordinator;
		} finally {
			this.lock.unlock();
		}
	}

	public void setConsumeCoordinator(RemoteCoordinator consumeCoordinator) {
		try {
			this.lock.lock();
			if (this.consumeCoordinator == null) {
				this.consumeCoordinator = consumeCoordinator;
				this.condition.signalAll();
			} else {
				throw new IllegalStateException(
						"Field 'consumeCoordinator' has already been set, please check your app whether it imports jta repeatedly!");
			}
		} finally {
			this.lock.unlock();
		}
	}

	public Environment getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

}
