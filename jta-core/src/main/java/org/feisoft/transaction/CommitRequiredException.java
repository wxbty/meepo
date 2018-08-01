
package org.feisoft.transaction;

import javax.transaction.SystemException;

public class CommitRequiredException extends SystemException {
	private static final long serialVersionUID = 1L;

	public CommitRequiredException() {
		super();
	}

	public CommitRequiredException(String s) {
		super(s);
	}

	public CommitRequiredException(int errcode) {
		super(errcode);
	}

}
