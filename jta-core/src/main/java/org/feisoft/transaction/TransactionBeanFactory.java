
package org.feisoft.transaction;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.logging.ArchiveDeserializer;
import org.feisoft.transaction.logging.TransactionLogger;
import org.feisoft.transaction.supports.TransactionTimer;
import org.feisoft.transaction.supports.rpc.TransactionInterceptor;
import org.feisoft.transaction.supports.serialize.XAResourceDeserializer;
import org.feisoft.transaction.xa.XidFactory;

public interface TransactionBeanFactory {


	public TransactionManager getTransactionManager();

	public XidFactory getXidFactory();

	public TransactionTimer getTransactionTimer();

	public TransactionRepository getTransactionRepository();

	public TransactionInterceptor getTransactionInterceptor();

	public TransactionRecovery getTransactionRecovery();

	public RemoteCoordinator getTransactionCoordinator();

	public TransactionLogger getTransactionLogger();

	public ArchiveDeserializer getArchiveDeserializer();

	public XAResourceDeserializer getResourceDeserializer();

}
