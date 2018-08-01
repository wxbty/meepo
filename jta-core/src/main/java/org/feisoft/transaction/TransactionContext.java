
package org.feisoft.transaction;

import org.feisoft.transaction.xa.TransactionXid;

import java.io.Serializable;

public class TransactionContext implements Serializable, Cloneable {
	private static final long serialVersionUID = 1L;

	protected transient Object propagatedBy;
	protected transient boolean propagated;
	protected transient boolean coordinator;
	protected transient boolean recoveried;

	protected TransactionXid xid;
	protected long createdTime;
	protected long expiredTime;

	public TransactionContext clone() {
		TransactionContext that = new TransactionContext();
		that.xid = this.xid.clone();
		that.createdTime = System.currentTimeMillis();
		that.expiredTime = this.expiredTime;
		return that;
	}

	public boolean isCoordinator() {
		return coordinator;
	}

	public void setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
	}

	public boolean isRecoveried() {
		return recoveried;
	}

	public void setRecoveried(boolean recoveried) {
		this.recoveried = recoveried;
	}

	public TransactionXid getXid() {
		return xid;
	}

	public void setXid(TransactionXid xid) {
		this.xid = xid;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

	public long getExpiredTime() {
		return expiredTime;
	}

	public void setExpiredTime(long expiredTime) {
		this.expiredTime = expiredTime;
	}

	public boolean isPropagated() {
		return propagated;
	}

	public void setPropagated(boolean propagated) {
		this.propagated = propagated;
	}

	public Object getPropagatedBy() {
		return propagatedBy;
	}

	public void setPropagatedBy(Object propagatedBy) {
		this.propagatedBy = propagatedBy;
	}

}
