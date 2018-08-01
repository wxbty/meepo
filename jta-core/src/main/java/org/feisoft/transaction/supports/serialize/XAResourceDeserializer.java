
package org.feisoft.transaction.supports.serialize;

import org.feisoft.transaction.supports.resource.XAResourceDescriptor;

public interface XAResourceDeserializer {

	public XAResourceDescriptor deserialize(String identifier);

}
