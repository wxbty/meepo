
package org.feisoft.transaction.work;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkListener;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleWorkListener implements WorkListener {
	static final Logger logger = LoggerFactory.getLogger(SimpleWorkListener.class);

	private long acceptedTime = -1;
	private long startedTime = -1;

	private final WorkListener delegate;
	private final Lock lock = new ReentrantLock();
	private final Condition condition = this.lock.newCondition();

	public SimpleWorkListener(WorkListener workListener) {
		this.delegate = workListener;
	}

	public long waitForStart() {
		try {
			this.lock.lock();
			while (this.acceptedTime < 0 || this.startedTime < 0) {
				try {
					this.condition.await();
				} catch (InterruptedException ex) {
					logger.debug(ex.getMessage());
				}
			}
			return this.startedTime - this.acceptedTime;
		} finally {
			this.lock.unlock();
		}
	}

	public void signalStarted() {
		try {
			this.lock.lock();
			this.condition.signalAll();
		} finally {
			this.lock.unlock();
		}
	}

	public void workAccepted(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workAccepted(event);
		}
		this.acceptedTime = System.currentTimeMillis();
	}

	public void workCompleted(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workCompleted(event);
		}
	}

	public void workRejected(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workRejected(event);
		}
	}

	public void workStarted(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workStarted(event);
		}
		this.startedTime = System.currentTimeMillis();
		this.signalStarted();
	}

}
