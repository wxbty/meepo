
package org.feisoft.transaction;

import org.feisoft.transaction.xa.TransactionXid;

public interface TransactionLock {

	public boolean lockTransaction(TransactionXid transactionXid, String identifier);

	public void unlockTransaction(TransactionXid transactionXid, String identifier);

}
