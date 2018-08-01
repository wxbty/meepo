
package org.feisoft.jta.supports.resource;

import org.apache.commons.lang3.StringUtils;
import org.feisoft.jta.supports.jdbc.LocalXAResource;
import org.feisoft.jta.supports.jdbc.RecoveredResource;
import org.feisoft.transaction.supports.resource.XAResourceDescriptor;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class LocalXAResourceDescriptor implements XAResourceDescriptor {

	private String identifier;
	private XAResource delegate;

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException {
		try {
			if (RecoveredResource.class.isInstance(this.delegate)) {
				((RecoveredResource) this.delegate).recoverable(xid);
			} else {
				((LocalXAResource) this.delegate).recoverable(xid);
			}
			return true;
		} catch (XAException ex) {
			switch (ex.errorCode) {
			case XAException.XAER_NOTA:
				return false;
			default:
				throw new IllegalStateException(ex);
			}
		}
	}

	public String toString() {
		return String.format("local-xa-resource[%s]", this.identifier);
	}

	public void setTransactionTimeoutQuietly(int timeout) {
		try {
			this.delegate.setTransactionTimeout(timeout);
		} catch (Exception ex) {
			return;
		}
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.commit(arg0, arg1);
	}

	public void end(Xid arg0, int arg1) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.end(arg0, arg1);
	}

	public void forget(Xid arg0) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.forget(arg0);
	}

	public int getTransactionTimeout() throws XAException {
		if (this.delegate == null) {
			return 0;
		}
		return delegate.getTransactionTimeout();
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		if (this.delegate == null) {
			return false;
		}

		if (LocalXAResourceDescriptor.class.isInstance(xares)) {
			LocalXAResourceDescriptor that = (LocalXAResourceDescriptor) xares;
			return StringUtils.equals(this.identifier, that.identifier);
		} else {
			return delegate.isSameRM(xares);
		}

	}

	public int prepare(Xid arg0) throws XAException {
		if (this.delegate == null) {
			return XA_RDONLY;
		}
		return delegate.prepare(arg0);
	}

	public Xid[] recover(int arg0) throws XAException {
		if (this.delegate == null) {
			return new Xid[0];
		}
		return delegate.recover(arg0);
	}

	public void rollback(Xid arg0) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.rollback(arg0);
	}

	public boolean setTransactionTimeout(int arg0) throws XAException {
		if (this.delegate == null) {
			return false;
		}
		return delegate.setTransactionTimeout(arg0);
	}

	public void start(Xid arg0, int arg1) throws XAException {
		if (this.delegate == null) {
			return;
		}
		delegate.start(arg0, arg1);
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public XAResource getDelegate() {
		return delegate;
	}

	public void setDelegate(XAResource delegate) {
		this.delegate = delegate;
	}

}
