
package org.feisoft.transaction.xa;

import org.feisoft.common.utils.ByteUtils;

import javax.transaction.xa.Xid;
import java.io.Serializable;
import java.util.Arrays;

public class TransactionXid implements Xid, Cloneable, Serializable {
	private static final long serialVersionUID = 1L;

	private int formatId;
	private byte[] globalTransactionId;
	private byte[] branchQualifier;

	public TransactionXid() {
	}

	public TransactionXid(int formatId, byte[] global) {
		this(formatId, global, new byte[0]);
	}

	public TransactionXid(int formatId, byte[] global, byte[] branch) {
		if (global == null) {
			throw new IllegalArgumentException("globalTransactionId cannot be null.");
		} else if (global.length > MAXGTRIDSIZE) {
			throw new IllegalArgumentException("length of globalTransactionId cannot exceed 64 bytes.");
		}

		if (branch == null) {
			throw new IllegalArgumentException("branchQualifier cannot be null.");
		} else if (branch.length > MAXBQUALSIZE) {
			throw new IllegalArgumentException("length of branchQualifier cannot exceed 64 bytes.");
		}

		this.globalTransactionId = new byte[global.length];
		this.branchQualifier = new byte[branch.length];

		System.arraycopy(global, 0, this.globalTransactionId, 0, global.length);
		System.arraycopy(branch, 0, this.branchQualifier, 0, branch.length);

		this.formatId = formatId;
		this.globalTransactionId = global;
		this.branchQualifier = branch;
	}

	public TransactionXid clone() {
		TransactionXid that = new TransactionXid();
		that.setFormatId(this.formatId);
		byte[] global = new byte[this.globalTransactionId.length];
		byte[] branch = new byte[this.branchQualifier.length];

		System.arraycopy(this.globalTransactionId, 0, global, 0, this.globalTransactionId.length);
		System.arraycopy(this.branchQualifier, 0, branch, 0, this.branchQualifier.length);

		that.setGlobalTransactionId(global);
		that.setBranchQualifier(branch);

		return that;
	}

	public int getFormatId() {
		return this.formatId;
	}

	public int hashCode() {
		int hash = 23;
		hash += 29 * this.getFormatId();
		hash += 31 * Arrays.hashCode(branchQualifier);
		hash += 37 * Arrays.hashCode(globalTransactionId);
		return hash;
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (getClass() != obj.getClass()) {
			return false;
		}
		TransactionXid other = (TransactionXid) obj;
		if (this.formatId != other.formatId) {
			return false;
		} else if (Arrays.equals(branchQualifier, other.branchQualifier) == false) {
			return false;
		} else if (Arrays.equals(globalTransactionId, other.globalTransactionId) == false) {
			return false;
		}
		return true;
	}

	public String toString() {
		String global = this.globalTransactionId == null ? null : ByteUtils.byteArrayToString(this.globalTransactionId);
		String branch = this.branchQualifier == null ? null : ByteUtils.byteArrayToString(this.branchQualifier);
		return String.format("%s-%s-%s", this.getFormatId(), global, branch);
	}

	public byte[] getGlobalTransactionId() {
		return globalTransactionId;
	}

	public void setGlobalTransactionId(byte[] globalTransactionId) {
		this.globalTransactionId = globalTransactionId;
	}

	public byte[] getBranchQualifier() {
		return branchQualifier;
	}

	public void setBranchQualifier(byte[] branchQualifier) {
		this.branchQualifier = branchQualifier;
	}

	public void setFormatId(int formatId) {
		this.formatId = formatId;
	}

}
