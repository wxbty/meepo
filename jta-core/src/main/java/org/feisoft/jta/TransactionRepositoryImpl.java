
package org.feisoft.jta;

import org.feisoft.transaction.Transaction;
import org.feisoft.transaction.TransactionRepository;
import org.feisoft.transaction.xa.TransactionXid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionRepositoryImpl implements TransactionRepository {
	private final Map<TransactionXid, Transaction> xidToTxMap = new ConcurrentHashMap<TransactionXid, Transaction>();
	private final Map<TransactionXid, Transaction> xidToErrTxMap = new ConcurrentHashMap<TransactionXid, Transaction>();

	public void putTransaction(TransactionXid globalXid, Transaction transaction) {
		this.xidToTxMap.put(globalXid, transaction);
	}

	public Transaction getTransaction(TransactionXid globalXid) {
		return this.xidToTxMap.get(globalXid);
	}

	public Transaction removeTransaction(TransactionXid globalXid) {
		return this.xidToTxMap.remove(globalXid);
	}

	public void putErrorTransaction(TransactionXid globalXid, Transaction transaction) {
		this.xidToErrTxMap.put(globalXid, transaction);
	}

	public Transaction getErrorTransaction(TransactionXid globalXid) {
		return this.xidToErrTxMap.get(globalXid);
	}

	public Transaction removeErrorTransaction(TransactionXid globalXid) {
		return this.xidToErrTxMap.remove(globalXid);
	}

	public List<Transaction> getErrorTransactionList() {
		return new ArrayList<Transaction>(this.xidToErrTxMap.values());
	}

	public List<Transaction> getActiveTransactionList() {
		return new ArrayList<Transaction>(this.xidToTxMap.values());
	}

}
