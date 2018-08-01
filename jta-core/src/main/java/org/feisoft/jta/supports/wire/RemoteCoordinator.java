
package org.feisoft.jta.supports.wire;

import org.feisoft.transaction.Transaction;
import org.feisoft.transaction.TransactionContext;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public interface RemoteCoordinator extends XAResource {

	public String getApplication();

	public String getIdentifier();

	public void forgetQuietly(Xid xid);

	public Transaction start(TransactionContext transactionContext, int flags) throws XAException;

	public Transaction end(TransactionContext transactionContext, int flags) throws XAException;

}
