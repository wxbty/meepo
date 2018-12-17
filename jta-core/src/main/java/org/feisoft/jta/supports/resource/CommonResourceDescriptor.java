
package org.feisoft.jta.supports.resource;

import org.feisoft.transaction.supports.resource.XAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class CommonResourceDescriptor implements XAResourceDescriptor {
	static Logger logger = LoggerFactory.getLogger(CommonResourceDescriptor.class);

	private XAResource delegate;
	private String identifier;

	private transient Xid recoverXid;
	private transient Object managed;
	// private transient boolean recved;

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException {
		throw new IllegalStateException();
	}

	public String toString() {
		return String.format("common-resource[id= %s]", this.identifier);
	}

	public void setTransactionTimeoutQuietly(int timeout) {
		try {
			this.delegate.setTransactionTimeout(timeout);
		} catch (Exception ex) {
			// ignore
		}
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		delegate.commit(arg0, arg1);
	}

	public void end(Xid arg0, int arg1) throws XAException {
		delegate.end(arg0, arg1);
	}

	public void forget(Xid arg0) throws XAException {
		try {
			delegate.forget(arg0);
		} finally {
			this.closeIfNecessary();
		}
	}

	private void closeIfNecessary() {
		if (this.recoverXid != null && this.managed != null) {
			if (javax.sql.XAConnection.class.isInstance(this.managed)) {
				this.closeQuietly((javax.sql.XAConnection) this.managed);
			} else if (javax.jms.XAConnection.class.isInstance(this.managed)) {
				this.closeQuietly((javax.jms.XAConnection) this.managed);
			} else if (javax.resource.spi.ManagedConnection.class.isInstance(this.managed)) {
				this.closeQuietly((javax.resource.spi.ManagedConnection) this.managed);
			}
		}
	}

	private void closeQuietly(javax.jms.XAConnection closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	private void closeQuietly(javax.sql.XAConnection closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	private void closeQuietly(javax.resource.spi.ManagedConnection closeable) {
		if (closeable != null) {
			try {
				closeable.cleanup();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}

			try {
				closeable.destroy();
			} catch (Exception ex) {
				logger.debug(ex.getMessage());
			}
		}
	}

	public int getTransactionTimeout() throws XAException {
		return delegate.getTransactionTimeout();
	}

	public boolean isSameRM(XAResource arg0) throws XAException {
		return delegate.isSameRM(arg0);
	}

	public int prepare(Xid arg0) throws XAException {
		int vote = delegate.prepare(arg0);
		if(vote == XAResource.XA_OK)
		{
			delegate.commit(arg0, false);
		}
		return vote;
	}

	public Xid[] recover(int arg0) throws XAException {
		Xid[] xidArray = delegate.recover(arg0);
		// for (int i = 0; this.recoverXid != null && i < xidArray.length; i++) {
		// Xid xid = xidArray[i];
		// boolean formatIdEquals = xid.getFormatId() == this.recoverXid.getFormatId();
		// boolean globalTransactionIdEquals = Arrays.equals(xid.getGlobalTransactionId(),
		// this.recoverXid.getGlobalTransactionId());
		// boolean branchQualifierEquals = Arrays.equals(xid.getBranchQualifier(), this.recoverXid.getBranchQualifier());
		// if (formatIdEquals && globalTransactionIdEquals && branchQualifierEquals) {
		// this.recved = true;
		// }
		// }
		return xidArray;
	}

	public void rollback(Xid arg0) throws XAException {
		delegate.rollback(arg0);
	}

	public boolean setTransactionTimeout(int arg0) throws XAException {
		return delegate.setTransactionTimeout(arg0);
	}

	public void start(Xid arg0, int arg1) throws XAException {
		logger.info("thread={},xid={},delegate={}", Thread.currentThread().getName(), arg0, delegate);
		try {
			delegate.start(arg0, arg1);
		} catch (XAException e) {
			logger.error("thread={},xid={},delegate={}", Thread.currentThread().getName(), arg0, delegate);
			throw e;
		}
	}

	public XAResource getDelegate() {
		return delegate;
	}

	public void setDelegate(XAResource delegate) {
		this.delegate = delegate;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public Xid getRecoverXid() {
		return recoverXid;
	}

	public void setRecoverXid(Xid recoverXid) {
		this.recoverXid = recoverXid;
	}

	public Object getManaged() {
		return managed;
	}

	public void setManaged(Object managed) {
		this.managed = managed;
	}

}
