
package org.feisoft.transaction.work;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.resource.spi.work.*;
import java.util.concurrent.*;

public class SimpleWorkManager implements WorkManager {
	static final Logger logger = LoggerFactory.getLogger(SimpleWorkManager.class);

	private final ThreadPoolExecutor executor = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());
	// private final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

	public void doWork(Work work) throws WorkException {
		this.doWork(work, 1800 * 1000L, null, null);
	}

	public void doWork(Work work, long startTimeout, ExecutionContext execContext, WorkListener workListener)
			throws WorkException {
		SimpleWorkListener wrappedListener = new SimpleWorkListener(workListener);
		wrappedListener.workAccepted(new WorkEvent(this, WorkEvent.WORK_ACCEPTED, work, null));
		SimpleWork task = new SimpleWork();
		task.setWork(work);
		task.setWorkListener(wrappedListener);
		Future<?> future = this.executor.submit(task);
		try {
			future.get();
		} catch (CancellationException ex) {
			wrappedListener.workCompleted(new WorkEvent(this, WorkEvent.WORK_REJECTED, work, new WorkException(ex)));
		} catch (InterruptedException ex) {
			wrappedListener.workCompleted(new WorkEvent(this, WorkEvent.WORK_COMPLETED, work, new WorkException(ex)));
		} catch (ExecutionException ex) {
			wrappedListener.workCompleted(new WorkEvent(this, WorkEvent.WORK_COMPLETED, work, new WorkException(ex)));
		}
	}

	public long startWork(Work work) throws WorkException {
		return this.startWork(work, 1800 * 1000L, null, null);
	}

	public long startWork(Work work, long startTimeout, ExecutionContext execContext, WorkListener workListener)
			throws WorkException {
		SimpleWorkListener wrappedListener = new SimpleWorkListener(workListener);
		wrappedListener.workAccepted(new WorkEvent(this, WorkEvent.WORK_ACCEPTED, work, null));
		SimpleWork task = new SimpleWork();
		task.setSource(this);
		task.setWork(work);
		task.setWorkListener(wrappedListener);
		this.executor.submit(task);
		return wrappedListener.waitForStart();
	}

	public void scheduleWork(Work work) throws WorkException {
		throw new WorkException("not supported yet!");
	}

	public void scheduleWork(Work work, long startTimeout, ExecutionContext execContext, WorkListener workListener)
			throws WorkException {
		// SimpleWorkListener wrappedListener = new SimpleWorkListener(workListener);
		// wrappedListener.workAccepted(new WorkEvent(this, WorkEvent.WORK_ACCEPTED, work, null));
		// SimpleWork task = new SimpleWork();
		// task.setSource(this);
		// task.setWork(work);
		// task.setWorkListener(wrappedListener);
		// // ScheduledFuture<?> future =
		// this.scheduled.scheduleAtFixedRate(task, 0, 1000, TimeUnit.MILLISECONDS);
		throw new WorkException("not supported yet!");
	}

}
