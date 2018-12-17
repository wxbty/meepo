
package org.feisoft.jta.supports.resource;

import org.feisoft.transaction.supports.resource.XAResourceDescriptor;

import javax.transaction.xa.XAResource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class ManagedXASessionHandler implements InvocationHandler {

	private final Object delegate;
	private String identifier;

	public ManagedXASessionHandler(Object managed) {
		this.delegate = managed;
	}

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();
		Class<?> returningClass = method.getReturnType();

		Object resultObject = method.invoke(this.delegate, args);
		if (resultObject != null && XAResourceDescriptor.class.isInstance(resultObject)) {
			return resultObject;
		}

		CommonResourceDescriptor descriptor = new CommonResourceDescriptor();
		descriptor.setIdentifier(this.identifier);
		if (javax.jms.XASession.class.equals(declaringClass) && XAResource.class.equals(returningClass)) {
			descriptor.setDelegate((XAResource) resultObject);
		}

		return descriptor.getDelegate() != null ? descriptor : resultObject;

	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

}
