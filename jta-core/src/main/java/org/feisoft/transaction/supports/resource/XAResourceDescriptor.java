
package org.feisoft.transaction.supports.resource;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public interface XAResourceDescriptor extends XAResource {

	public void setIdentifier(String identifier);

	public String getIdentifier();

	public XAResource getDelegate();

	public void setTransactionTimeoutQuietly(int timeout);

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException;

}
