
package org.feisoft.jta.supports.rpc;

import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.transaction.TransactionContext;
import org.feisoft.transaction.supports.rpc.TransactionResponse;

public class TransactionResponseImpl implements TransactionResponse {

	private boolean participantStickyRequired = true;
	private boolean participantRollbackOnly;

	private RemoteCoordinator participantCoordinator;
	private TransactionContext transactionContext;

	private transient boolean participantEnlistFlag;
	private transient boolean participantDelistFlag;

	public RemoteCoordinator getSourceTransactionCoordinator() {
		return this.participantCoordinator;
	}

	public void setSourceTransactionCoordinator(RemoteCoordinator remoteCoordinator) {
		this.participantCoordinator = remoteCoordinator;
	}

	public TransactionContext getTransactionContext() {
		return this.transactionContext;
	}

	public void setTransactionContext(TransactionContext transactionContext) {
		this.transactionContext = transactionContext;
	}

	public boolean isParticipantRollbackOnly() {
		return participantRollbackOnly;
	}

	public void setParticipantRollbackOnly(boolean participantRollbackOnly) {
		this.participantRollbackOnly = participantRollbackOnly;
	}

	public boolean isParticipantStickyRequired() {
		return participantStickyRequired;
	}

	public void setParticipantStickyRequired(boolean participantStickyRequired) {
		this.participantStickyRequired = participantStickyRequired;
	}

	public boolean isParticipantDelistFlag() {
		return participantDelistFlag;
	}

	public void setParticipantDelistFlag(boolean participantDelistFlag) {
		this.participantDelistFlag = participantDelistFlag;
	}

	public boolean isParticipantEnlistFlag() {
		return participantEnlistFlag;
	}

	public void setParticipantEnlistFlag(boolean participantEnlistFlag) {
		this.participantEnlistFlag = participantEnlistFlag;
	}

}
