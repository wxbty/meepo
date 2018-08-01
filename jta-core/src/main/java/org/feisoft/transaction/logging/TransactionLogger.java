
package org.feisoft.transaction.logging;

import org.feisoft.transaction.archive.TransactionArchive;
import org.feisoft.transaction.archive.XAResourceArchive;
import org.feisoft.transaction.recovery.TransactionRecoveryCallback;

public interface TransactionLogger {

	/* transaction */
	public void createTransaction(TransactionArchive archive);

	public void updateTransaction(TransactionArchive archive);

	public void deleteTransaction(TransactionArchive archive);

	/* resource */
	public void updateResource(XAResourceArchive archive);

	/* recovery */
	public void recover(TransactionRecoveryCallback callback);

}
