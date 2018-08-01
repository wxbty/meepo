
package org.feisoft.transaction.supports.rpc;

public interface TransactionInterceptor {

	public void beforeSendRequest(TransactionRequest request) throws IllegalStateException;

	public void beforeSendResponse(TransactionResponse response) throws IllegalStateException;

	public void afterReceiveRequest(TransactionRequest request) throws IllegalStateException;

	public void afterReceiveResponse(TransactionResponse response) throws IllegalStateException;

}
