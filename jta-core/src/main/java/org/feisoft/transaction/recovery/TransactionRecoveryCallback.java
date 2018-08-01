
package org.feisoft.transaction.recovery;

import org.feisoft.transaction.archive.TransactionArchive;

public interface TransactionRecoveryCallback {

	public void recover(TransactionArchive archive);

}
