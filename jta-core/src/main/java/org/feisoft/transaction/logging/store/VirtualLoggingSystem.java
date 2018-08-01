
package org.feisoft.transaction.logging.store;

import javax.transaction.xa.Xid;

public interface VirtualLoggingSystem {
	public static final int OPERATOR_CREATE = 1;
	public static final int OPERATOR_MOFIFY = 2;
	public static final int OPERATOR_DELETE = 3;

	public void create(Xid xid, byte[] byteArray);

	public void delete(Xid xid);

	public void modify(Xid xid, byte[] byteArray);

	public void traversal(VirtualLoggingListener listener);

}
