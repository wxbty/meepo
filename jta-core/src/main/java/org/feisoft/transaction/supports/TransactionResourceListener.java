
package org.feisoft.transaction.supports;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public interface TransactionResourceListener {

	public void onEnlistResource(Xid xid, XAResource xares);

	public void onDelistResource(Xid xid, XAResource xares);

}
