
package org.feisoft.transaction.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;
import java.util.ArrayList;
import java.util.List;

public class ResourceAdapterImpl implements ResourceAdapter {
	static final Logger logger = LoggerFactory.getLogger(ResourceAdapterImpl.class);

	private List<Work> workList = new ArrayList<Work>();;
	private WorkManager workManager;

	public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {
		this.workManager = ctx.getWorkManager();
		for (int i = 0; this.workList != null && i < this.workList.size(); i++) {
			Work work = this.workList.get(i);
			try {
				this.workManager.startWork(work);
			} catch (WorkException ex) {
				this.stop();
				throw new ResourceAdapterInternalException(ex);
			} catch (RuntimeException ex) {
				this.stop();
				throw new ResourceAdapterInternalException(ex);
			}
		}
	}

	public void stop() {
		for (int i = 0; this.workList != null && i < this.workList.size(); i++) {
			Work work = this.workList.get(i);
			try {
				work.release();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) throws ResourceException {
	}

	public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) {
	}

	public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException {
		return new XAResource[0];
	}

	public List<Work> getWorkList() {
		return workList;
	}

	public void setWorkList(List<Work> workList) {
		this.workList = workList;
	}

}
