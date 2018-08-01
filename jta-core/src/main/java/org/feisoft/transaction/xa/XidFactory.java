
package org.feisoft.transaction.xa;

public interface XidFactory {
	public static final int JTA_FORMAT_ID = 1207;
	public static final int TCC_FORMAT_ID = 8127;

	public static final int GLOBAL_TRANSACTION_LENGTH = 20;
	public static final int BRANCH_QUALIFIER_LENGTH = 20;

	public TransactionXid createGlobalXid();

	public TransactionXid createGlobalXid(byte[] globalTransactionId);

	public TransactionXid createBranchXid(TransactionXid globalXid);

	public TransactionXid createBranchXid(TransactionXid globalXid, byte[] branchQualifier);
}
