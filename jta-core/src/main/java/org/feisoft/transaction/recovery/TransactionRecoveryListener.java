
package org.feisoft.transaction.recovery;

import org.feisoft.transaction.Transaction;

public interface TransactionRecoveryListener {

	public void onRecovery(Transaction transaction);

}
