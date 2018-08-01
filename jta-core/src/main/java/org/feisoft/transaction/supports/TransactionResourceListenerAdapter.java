
package org.feisoft.transaction.supports;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class TransactionResourceListenerAdapter implements TransactionResourceListener {

	public void onEnlistResource(Xid xid, XAResource xares) {
	}

	public void onDelistResource(Xid xid, XAResource xares) {
	}

}
