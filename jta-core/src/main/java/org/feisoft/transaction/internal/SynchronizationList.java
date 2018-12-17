
package org.feisoft.transaction.internal;

import javax.transaction.Synchronization;
import java.util.ArrayList;
import java.util.List;

public class SynchronizationList implements Synchronization {
	private final List<Synchronization> synchronizations = new ArrayList<Synchronization>();

    private boolean beforeCompletionInvoked;

    private boolean finishCompletionInvoked;

	public void registerSynchronizationQuietly(Synchronization sync) {
		SynchronizationImpl synchronization = new SynchronizationImpl(sync);
		this.synchronizations.add(synchronization);
	}

	public void beforeCompletion() {
        if (this.beforeCompletionInvoked == false) {
            int length = this.synchronizations.size();
            for (int i = 0; i < length; i++) {
                Synchronization synchronization = this.synchronizations.get(i);
                try {
                    synchronization.beforeCompletion();
                } catch (RuntimeException error) {
                    // ignore
                }
            } // end-for
            this.beforeCompletionInvoked = true;
        } // end-if (this.beforeCompletionIn
	}

	public void afterCompletion(int status) {
        if (this.finishCompletionInvoked == false) {
            int length = this.synchronizations.size();
            for (int i = 0; i < length; i++) {
                Synchronization synchronization = this.synchronizations.get(i);
                try {
                    synchronization.afterCompletion(status);
                } catch (RuntimeException error) {
                    // ignore
                }
            } // end-for
            this.finishCompletionInvoked = true;
        } // end-if (this.finishCompletionInvoked == false)
	}

}
