
package org.feisoft.transaction.internal;

import org.feisoft.transaction.supports.TransactionListener;
import org.feisoft.transaction.supports.TransactionListenerAdapter;
import org.feisoft.transaction.xa.TransactionXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TransactionListenerList extends TransactionListenerAdapter {
	static final Logger logger = LoggerFactory.getLogger(TransactionListenerList.class);

	private final List<TransactionListener> listeners = new ArrayList<TransactionListener>();

	public void registerTransactionListener(TransactionListener listener) {
		this.listeners.add(listener);
	}

	public void onPrepareStart(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onPrepareStart(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onPrepareSuccess(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onPrepareSuccess(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onPrepareFailure(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onPrepareFailure(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitStart(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitStart(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitSuccess(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitSuccess(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitFailure(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitFailure(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitHeuristicMixed(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitHeuristicMixed(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitHeuristicRolledback(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitHeuristicRolledback(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onRollbackStart(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onRollbackStart(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onRollbackSuccess(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onRollbackSuccess(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onRollbackFailure(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onRollbackFailure(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

}
