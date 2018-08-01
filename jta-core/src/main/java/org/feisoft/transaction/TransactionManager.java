
package org.feisoft.transaction;

import javax.transaction.SystemException;

public interface TransactionManager extends javax.transaction.TransactionManager {

	public int getTimeoutSeconds();

	public void setTimeoutSeconds(int timeoutSeconds);

	public void associateThread(Transaction transaction);

	public Transaction desociateThread();

	public Transaction getTransaction(Thread thread);

	public Transaction getTransactionQuietly();

	public Transaction getTransaction() throws SystemException;

	public Transaction suspend() throws SystemException;

}
