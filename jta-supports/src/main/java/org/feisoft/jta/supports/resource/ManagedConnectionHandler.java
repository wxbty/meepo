
package org.feisoft.jta.supports.resource;

import org.feisoft.transaction.supports.resource.XAResourceDescriptor;

import javax.jms.XASession;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ManagedConnectionHandler implements InvocationHandler {

	private final Object delegate;
	private String identifier;

	public ManagedConnectionHandler(Object managed) {
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
		if (javax.sql.XAConnection.class.equals(declaringClass) && XAResource.class.equals(returningClass)) {
			descriptor.setDelegate((XAResource) resultObject);
		} else if (javax.jms.XAConnection.class.equals(declaringClass) && XAResource.class.equals(returningClass)) {
			descriptor.setDelegate((XAResource) resultObject);
		} else if (javax.jms.XAConnection.class.equals(declaringClass) && XASession.class.equals(returningClass)) {
			Class<?> clazz = resultObject.getClass();
			ClassLoader cl = clazz.getClassLoader();
			Class<?>[] interfaces = clazz.getInterfaces();
			boolean containsReturningClass = false;
			for (int i = 0; i < interfaces.length; i++) {
				Class<?> interfaceClass = interfaces[i];
				if (interfaceClass.equals(returningClass)) {
					containsReturningClass = true;
					break;
				}
			} // end-for (int i = 0; i < interfaces.length; i++)

			ManagedXASessionHandler interceptor = new ManagedXASessionHandler(resultObject);
			interceptor.setIdentifier(this.identifier);

			if (containsReturningClass) {
				return Proxy.newProxyInstance(cl, interfaces, interceptor);
			} else {
				Class<?>[] interfaceArray = new Class<?>[interfaces.length + 1];
				System.arraycopy(interfaces, 0, interfaceArray, 0, interfaces.length);
				interfaceArray[interfaces.length] = XASession.class;
				return Proxy.newProxyInstance(cl, interfaceArray, interceptor);
			}
		} else if (ManagedConnection.class.equals(declaringClass) && XAResource.class.equals(returningClass)) {
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
