
package org.feisoft.transaction.internal;

import org.feisoft.transaction.supports.TransactionResourceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.List;

public class TransactionResourceListenerList implements TransactionResourceListener {
	static final Logger logger = LoggerFactory.getLogger(TransactionResourceListenerList.class);

	private final List<TransactionResourceListener> listeners = new ArrayList<TransactionResourceListener>();

	public void registerTransactionResourceListener(TransactionResourceListener listener) {
		this.listeners.add(listener);
	}

	public void onEnlistResource(Xid xid, XAResource xares) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionResourceListener listener = this.listeners.get(i);
				listener.onEnlistResource(xid, xares);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onDelistResource(Xid xid, XAResource xares) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionResourceListener listener = this.listeners.get(i);
				listener.onDelistResource(xid, xares);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

}
