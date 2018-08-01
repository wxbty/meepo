
package org.feisoft.jta.supports.resource;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.common.utils.CommonUtils;
import org.feisoft.transaction.supports.resource.XAResourceDescriptor;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class RemoteResourceDescriptor implements XAResourceDescriptor {

	private RemoteCoordinator delegate;

	public void setIdentifier(String identifier) {
	}

	public String getIdentifier() {
		return this.delegate == null ? null : this.delegate.getIdentifier();
	}

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException {
		throw new IllegalStateException();
	}

	public String toString() {
		return String.format("remote-resource[id= %s]", this.getIdentifier());
	}

	public void setTransactionTimeoutQuietly(int timeout) {
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		delegate.commit(arg0, arg1);
	}

	public void end(Xid arg0, int arg1) throws XAException {
		// delegate.end(arg0, arg1);
	}

	public void forget(Xid arg0) throws XAException {
		delegate.forget(arg0);
	}

	public int getTransactionTimeout() throws XAException {
		throw new XAException(XAException.XAER_RMERR);
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		try {
			RemoteResourceDescriptor that = (RemoteResourceDescriptor) xares;
			return CommonUtils.equals(this.getIdentifier(), that.getIdentifier());
		} catch (RuntimeException rex) {
			return false;
		}
	}

	public int prepare(Xid arg0) throws XAException {
		return delegate.prepare(arg0);
	}

	public Xid[] recover(int arg0) throws XAException {
		return delegate.recover(arg0);
	}

	public void rollback(Xid arg0) throws XAException {
		delegate.rollback(arg0);
	}

	public boolean setTransactionTimeout(int arg0) throws XAException {
		return true;
	}

	public void start(Xid arg0, int arg1) throws XAException {
		delegate.start(arg0, arg1);
	}

	public RemoteCoordinator getDelegate() {
		return delegate;
	}

	public void setDelegate(RemoteCoordinator delegate) {
		this.delegate = delegate;
	}

}
