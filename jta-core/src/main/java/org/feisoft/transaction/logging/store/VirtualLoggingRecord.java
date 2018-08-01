
package org.feisoft.transaction.logging.store;

import javax.transaction.xa.Xid;

public class VirtualLoggingRecord {

	private Xid identifier;
	private int operator;
	private byte[] value;
	private byte[] content;

	public int getOperator() {
		return operator;
	}

	public void setOperator(int operator) {
		this.operator = operator;
	}

	public Xid getIdentifier() {
		return identifier;
	}

	public void setIdentifier(Xid identifier) {
		this.identifier = identifier;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

}
