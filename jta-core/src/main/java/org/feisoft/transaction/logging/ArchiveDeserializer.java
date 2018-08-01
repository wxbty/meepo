
package org.feisoft.transaction.logging;

import org.feisoft.transaction.xa.TransactionXid;

public interface ArchiveDeserializer {

	public byte[] serialize(TransactionXid xid, Object archive);

	public Object deserialize(TransactionXid xid, byte[] array);

}
