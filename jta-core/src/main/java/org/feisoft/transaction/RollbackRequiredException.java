
package org.feisoft.transaction;

import javax.transaction.SystemException;

public class RollbackRequiredException extends SystemException {
	private static final long serialVersionUID = 1L;

	public RollbackRequiredException() {
		super();
	}

	public RollbackRequiredException(String s) {
		super(s);
	}

	public RollbackRequiredException(int errcode) {
		super(errcode);
	}

}
