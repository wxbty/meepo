
package org.feisoft.transaction.supports;

import org.feisoft.transaction.xa.TransactionXid;

public class TransactionListenerAdapter implements TransactionListener {

	public void onPrepareStart(TransactionXid xid) {
	}

	public void onPrepareSuccess(TransactionXid xid) {
	}

	public void onPrepareFailure(TransactionXid xid) {
	}

	public void onCommitStart(TransactionXid xid) {
	}

	public void onCommitSuccess(TransactionXid xid) {
	}

	public void onCommitFailure(TransactionXid xid) {
	}

	public void onCommitHeuristicMixed(TransactionXid xid) {
	}

	public void onCommitHeuristicRolledback(TransactionXid xid) {
	}

	public void onRollbackStart(TransactionXid xid) {
	}

	public void onRollbackSuccess(TransactionXid xid) {
	}

	public void onRollbackFailure(TransactionXid xid) {
	}

}
