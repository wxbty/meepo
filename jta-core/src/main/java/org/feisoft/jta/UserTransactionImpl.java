
package org.feisoft.jta;

import org.feisoft.transaction.TransactionManager;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.transaction.*;
import java.io.Serializable;

public class UserTransactionImpl implements UserTransaction, Referenceable, Serializable {
	private static final long serialVersionUID = 1L;

	@javax.inject.Inject
	private transient TransactionManager transactionManager;

	public void begin() throws NotSupportedException, SystemException {
		this.transactionManager.begin();
	}

	public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException,
			RollbackException, SecurityException, SystemException {
		this.transactionManager.commit();
	}

	public int getStatus() throws SystemException {
		return this.transactionManager.getStatus();
	}

	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		this.transactionManager.rollback();
	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
		this.transactionManager.setRollbackOnly();
	}

	public void setTransactionTimeout(int timeout) throws SystemException {
		this.transactionManager.setTransactionTimeout(timeout);
	}

	public Reference getReference() throws NamingException {
		throw new NamingException("Not supported yet!");
	}

	public TransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

}
