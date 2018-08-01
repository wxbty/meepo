
package org.feisoft.transaction.supports;

import org.feisoft.transaction.Transaction;

public interface TransactionTimer {

	public void timingExecution();

	public void stopTiming(Transaction tx);

}
