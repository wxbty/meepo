
package org.feisoft.transaction.archive;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.List;

public class TransactionArchive {
	private transient String endpoint;
	private Xid xid;
	private int status;
	private int vote;
	private boolean coordinator;
	private Object propagatedBy;
	private final List<XAResourceArchive> nativeResources = new ArrayList<XAResourceArchive>();
	private final List<XAResourceArchive> remoteResources = new ArrayList<XAResourceArchive>();

	private int transactionStrategyType;
	private XAResourceArchive optimizedResource;

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public Xid getXid() {
		return xid;
	}

	public void setXid(Xid xid) {
		this.xid = xid;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getVote() {
		return vote;
	}

	public void setVote(int vote) {
		this.vote = vote;
	}

	public boolean isCoordinator() {
		return coordinator;
	}

	public void setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
	}

	public Object getPropagatedBy() {
		return propagatedBy;
	}

	public void setPropagatedBy(Object propagatedBy) {
		this.propagatedBy = propagatedBy;
	}

	public List<XAResourceArchive> getNativeResources() {
		return nativeResources;
	}

	public List<XAResourceArchive> getRemoteResources() {
		return remoteResources;
	}

	public XAResourceArchive getOptimizedResource() {
		return optimizedResource;
	}

	public void setOptimizedResource(XAResourceArchive optimizedResource) {
		this.optimizedResource = optimizedResource;
	}

	public int getTransactionStrategyType() {
		return transactionStrategyType;
	}

	public void setTransactionStrategyType(int transactionStrategyType) {
		this.transactionStrategyType = transactionStrategyType;
	}

}
