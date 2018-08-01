
package org.feisoft.transaction.aware;

public interface TransactionEndpointAware {
	public static final String ENDPOINT_FIELD_NAME = "endpoint";

	public void setEndpoint(String identifier);
}
