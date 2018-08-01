
package org.feisoft.transaction.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.Synchronization;


public class SynchronizationImpl implements Synchronization {
	static final Logger logger = LoggerFactory.getLogger(SynchronizationImpl.class);

	private Synchronization delegate;
	private boolean beforeRequired;
	private boolean finishRequired;

	public SynchronizationImpl(Synchronization sync) {
		if (sync == null) {
			throw new IllegalArgumentException();
		} else {
			this.delegate = sync;
			this.beforeRequired = true;
			this.finishRequired = true;
		}
	}

	public void beforeCompletion() {
		if (this.beforeRequired) {
			try {
				this.delegate.beforeCompletion();
			} catch (RuntimeException rex) {
				// ignore
			} finally {
				this.beforeRequired = false;
			}
		}
	}

	public void afterCompletion(int status) {
		if (this.finishRequired) {
			try {
				this.delegate.afterCompletion(status);
			} catch (RuntimeException rex) {
				// ignore
			} finally {
				this.finishRequired = false;
			}
		}
	}

	public String toString() {
		return String.format("[%s] delegate: %s", this.getClass().getSimpleName(), this.delegate);
	}
}
