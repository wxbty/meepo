
package org.feisoft.transaction.work;

import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkListener;

public class SimpleWork implements Runnable {

	private Object source;
	private Work work;
	private WorkListener workListener;

	public void run() {
		this.workListener.workStarted(new WorkEvent(this.source, WorkEvent.WORK_STARTED, this.work, null));
		this.work.run();
		this.workListener.workCompleted(new WorkEvent(this.source, WorkEvent.WORK_COMPLETED, this.work, null));
	}

	public void setSource(Object source) {
		this.source = source;
	}

	public void setWork(Work work) {
		this.work = work;
	}

	public void setWorkListener(WorkListener workListener) {
		this.workListener = workListener;
	}

}
