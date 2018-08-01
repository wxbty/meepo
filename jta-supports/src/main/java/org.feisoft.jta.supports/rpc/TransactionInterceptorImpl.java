
package org.feisoft.jta.supports.rpc;

import org.feisoft.jta.supports.resource.RemoteResourceDescriptor;
import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.Transaction;
import org.feisoft.transaction.TransactionBeanFactory;
import org.feisoft.transaction.TransactionContext;
import org.feisoft.transaction.TransactionManager;
import org.feisoft.transaction.aware.TransactionBeanFactoryAware;
import org.feisoft.transaction.supports.rpc.TransactionInterceptor;
import org.feisoft.transaction.supports.rpc.TransactionRequest;
import org.feisoft.transaction.supports.rpc.TransactionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

public class TransactionInterceptorImpl implements TransactionInterceptor, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionInterceptorImpl.class);

	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;

	public void beforeSendRequest(TransactionRequest request) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction == null) {
			return;
		}

		if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
			throw new IllegalStateException(
					"Transaction has been marked as rollback only, can not propagate its context to remote branch.");
		} // end-if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK)

		TransactionContext srcTransactionContext = transaction.getTransactionContext();
		TransactionContext transactionContext = srcTransactionContext.clone();
		request.setTransactionContext(transactionContext);
		try {
			RemoteCoordinator resource = request.getTargetTransactionCoordinator();

			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setDelegate(resource);

			boolean participantEnlisted = transaction.enlistResource(descriptor);
			((TransactionRequestImpl) request).setParticipantEnlistFlag(participantEnlisted);
		} catch (IllegalStateException ex) {
			logger.error("TransactionInterceptorImpl.beforeSendRequest(TransactionRequest)", ex);
			throw ex;
		} catch (RollbackException ex) {
			transaction.setRollbackOnlyQuietly();
			logger.error("TransactionInterceptorImpl.beforeSendRequest(TransactionRequest)", ex);
			throw new IllegalStateException(ex);
		} catch (SystemException ex) {
			logger.error("TransactionInterceptorImpl.beforeSendRequest(TransactionRequest)", ex);
			throw new IllegalStateException(ex);
		}
	}

	public void beforeSendResponse(TransactionResponse response) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction == null) {
			return;
		}

		RemoteCoordinator coordinator = this.beanFactory.getTransactionCoordinator();

		TransactionContext srcTransactionContext = transaction.getTransactionContext();
		TransactionContext transactionContext = srcTransactionContext.clone();
		transactionContext.setPropagatedBy(srcTransactionContext.getPropagatedBy());

		response.setTransactionContext(transactionContext);
		// response.setSourceTransactionCoordinator(coordinator);
		try {
			coordinator.end(transactionContext, XAResource.TMSUCCESS);
		} catch (XAException ex) {
			throw new IllegalStateException(ex);
		}

	}

	public void afterReceiveRequest(TransactionRequest request) throws IllegalStateException {
		TransactionContext srcTransactionContext = request.getTransactionContext();
		if (srcTransactionContext == null) {
			return;
		}

		RemoteCoordinator coordinator = this.beanFactory.getTransactionCoordinator();

		TransactionContext transactionContext = srcTransactionContext.clone();
		transactionContext.setPropagatedBy(srcTransactionContext.getPropagatedBy());
		try {
			coordinator.start(transactionContext, XAResource.TMNOFLAGS);
		} catch (XAException ex) {
			throw new IllegalStateException(ex);
		}
	}

	public void afterReceiveResponse(TransactionResponse response) throws IllegalStateException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		TransactionContext transactionContext = response.getTransactionContext();
		RemoteCoordinator resource = response.getSourceTransactionCoordinator();

		boolean participantEnlistFlag = ((TransactionResponseImpl) response).isParticipantEnlistFlag();
		// boolean participantDelistFlag = ((TransactionResponseImpl) response).isParticipantDelistFlag();

		if (transaction == null || transactionContext == null) {
			return;
		} else if (participantEnlistFlag == false) {
			return;
		} else if (resource == null) {
			logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest): remote coordinator is null.");
			throw new IllegalStateException("remote coordinator is null.");
		}

		try {
			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setDelegate(resource);
			// descriptor.setIdentifier(resource.getIdentifier());

			transaction.delistResource(descriptor, XAResource.TMSUCCESS);
			// transaction.delistResource(descriptor, participantDelistFlag ? XAResource.TMFAIL : XAResource.TMSUCCESS);
		} catch (IllegalStateException ex) {
			logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest)", ex);
			throw ex;
		} catch (SystemException ex) {
			logger.error("TransactionInterceptorImpl.afterReceiveResponse(TransactionRequest)", ex);
			throw new IllegalStateException(ex);
		}

	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
