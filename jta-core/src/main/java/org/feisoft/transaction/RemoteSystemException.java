
package org.feisoft.transaction;

import javax.transaction.SystemException;
import java.rmi.RemoteException;

public class RemoteSystemException extends SystemException {
	private static final long serialVersionUID = 1L;

	public RemoteSystemException() {
		super();
	}

	public RemoteSystemException(String s) {
		super(s);
	}

	public RemoteSystemException(int errcode) {
		super(errcode);
	}

	public Throwable initCause(Throwable cause) {
		if (RemoteException.class.isInstance(cause) == false) {
			throw new IllegalArgumentException();
		}
		return super.initCause(cause);
	}

	public RemoteException getRemoteException() {
		Throwable thrown = super.getCause();
		return RemoteException.class.cast(thrown);
	}

}
