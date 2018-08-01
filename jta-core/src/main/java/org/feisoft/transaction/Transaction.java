
package org.feisoft.transaction;

import org.feisoft.transaction.archive.TransactionArchive;
import org.feisoft.transaction.supports.TransactionListener;
import org.feisoft.transaction.supports.TransactionResourceListener;
import org.feisoft.transaction.supports.resource.XAResourceDescriptor;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

public interface Transaction extends javax.transaction.Transaction {

	public boolean isLocalTransaction();

	public void setRollbackOnlyQuietly();

	public int getTransactionStatus();

	public void setTransactionStatus(int status);

	public void resume() throws SystemException;

	public void suspend() throws SystemException;

	public boolean isTiming();

	public void setTransactionTimeout(int seconds);

	public void registerTransactionListener(TransactionListener listener);

	public void registerTransactionResourceListener(TransactionResourceListener listener);

	public Object getTransactionalExtra();

	public void setTransactionalExtra(Object transactionalExtra);

	public XAResourceDescriptor getResourceDescriptor(String identifier);

	public XAResourceDescriptor getRemoteCoordinator(String application);

	public TransactionContext getTransactionContext();

	public TransactionArchive getTransactionArchive();

	public int participantPrepare() throws RollbackRequiredException, CommitRequiredException;

	public void participantCommit(boolean opc) throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, CommitRequiredException, SystemException;

	public void participantRollback() throws IllegalStateException, RollbackRequiredException, SystemException;

	public void forget() throws SystemException;

	public void forgetQuietly();

	public void recover() throws SystemException;

	public void recoveryCommit() throws CommitRequiredException, SystemException;

	public void recoveryRollback() throws RollbackRequiredException, SystemException;

	public int participantStart() throws  SystemException;

}
