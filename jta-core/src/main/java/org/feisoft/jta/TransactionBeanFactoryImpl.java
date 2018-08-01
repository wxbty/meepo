
package org.feisoft.jta;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.*;
import org.feisoft.transaction.logging.ArchiveDeserializer;
import org.feisoft.transaction.logging.TransactionLogger;
import org.feisoft.transaction.supports.TransactionTimer;
import org.feisoft.transaction.supports.rpc.TransactionInterceptor;
import org.feisoft.transaction.supports.serialize.XAResourceDeserializer;
import org.feisoft.transaction.xa.XidFactory;

public class TransactionBeanFactoryImpl implements TransactionBeanFactory {


	private TransactionManager transactionManager;
	private XidFactory xidFactory;
	private TransactionTimer transactionTimer;
	private TransactionLogger transactionLogger;
	private TransactionRepository transactionRepository;
	private TransactionInterceptor transactionInterceptor;
	private TransactionRecovery transactionRecovery;
	private RemoteCoordinator transactionCoordinator;
	private TransactionLock transactionLock;

	private ArchiveDeserializer archiveDeserializer;
	private XAResourceDeserializer resourceDeserializer;

	public TransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	public XidFactory getXidFactory() {
		return xidFactory;
	}

	public void setXidFactory(XidFactory xidFactory) {
		this.xidFactory = xidFactory;
	}

	public TransactionTimer getTransactionTimer() {
		return transactionTimer;
	}

	public void setTransactionTimer(TransactionTimer transactionTimer) {
		this.transactionTimer = transactionTimer;
	}

	public TransactionRepository getTransactionRepository() {
		return transactionRepository;
	}

	public void setTransactionRepository(TransactionRepository transactionRepository) {
		this.transactionRepository = transactionRepository;
	}

	public TransactionInterceptor getTransactionInterceptor() {
		return transactionInterceptor;
	}

	public void setTransactionInterceptor(TransactionInterceptor transactionInterceptor) {
		this.transactionInterceptor = transactionInterceptor;
	}

	public TransactionLock getTransactionLock() {
		return transactionLock;
	}

	public void setTransactionLock(TransactionLock transactionLock) {
		this.transactionLock = transactionLock;
	}

	public TransactionRecovery getTransactionRecovery() {
		return transactionRecovery;
	}

	public void setTransactionRecovery(TransactionRecovery transactionRecovery) {
		this.transactionRecovery = transactionRecovery;
	}

	public RemoteCoordinator getTransactionCoordinator() {
		return transactionCoordinator;
	}

	public void setTransactionCoordinator(RemoteCoordinator remoteCoordinator) {
		this.transactionCoordinator = remoteCoordinator;
	}

	public TransactionLogger getTransactionLogger() {
		return transactionLogger;
	}

	public void setTransactionLogger(TransactionLogger transactionLogger) {
		this.transactionLogger = transactionLogger;
	}

	public ArchiveDeserializer getArchiveDeserializer() {
		return archiveDeserializer;
	}

	public void setArchiveDeserializer(ArchiveDeserializer archiveDeserializer) {
		this.archiveDeserializer = archiveDeserializer;
	}

	public XAResourceDeserializer getResourceDeserializer() {
		return resourceDeserializer;
	}

	public void setResourceDeserializer(XAResourceDeserializer resourceDeserializer) {
		this.resourceDeserializer = resourceDeserializer;
	}

}
