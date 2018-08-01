
package org.feisoft.transaction;

import org.feisoft.transaction.xa.TransactionXid;

import java.util.List;

public interface TransactionRepository {

	// active-transaction & error-transaction
	public void putTransaction(TransactionXid xid, Transaction transaction);

	public Transaction getTransaction(TransactionXid xid);

	public Transaction removeTransaction(TransactionXid xid);

	// error-transaction
	public void putErrorTransaction(TransactionXid xid, Transaction transaction);

	public Transaction getErrorTransaction(TransactionXid xid);

	public Transaction removeErrorTransaction(TransactionXid xid);

	public List<Transaction> getErrorTransactionList();

	public List<Transaction> getActiveTransactionList();

}
