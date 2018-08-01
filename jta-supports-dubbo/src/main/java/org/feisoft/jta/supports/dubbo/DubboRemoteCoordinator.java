
package org.feisoft.jta.supports.dubbo;

import org.apache.commons.lang3.StringUtils;
import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.jta.supports.wire.RemoteCoordinatorRegistry;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DubboRemoteCoordinator implements InvocationHandler {

	private InvocationContext invocationContext;
	private RemoteCoordinator remoteCoordinator;

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		InvocationContextRegistry registry = InvocationContextRegistry.getInstance();
		try {
			registry.associateInvocationContext(this.invocationContext);
			Class<?> clazz = method.getDeclaringClass();
			String methodName = method.getName();
			if (Object.class.equals(clazz)) {
				return method.invoke(this, args);
			} else if (RemoteCoordinator.class.equals(clazz)) {
				if ("getIdentifier".equals(methodName)) {
					String serverHost = this.invocationContext == null ? null : this.invocationContext.getServerHost();
					String serviceKey = this.invocationContext == null ? null : this.invocationContext.getServiceKey();
					int serverPort = this.invocationContext == null ? 0 : this.invocationContext.getServerPort();
					return this.invocationContext == null ? null
							: String.format("%s:%s:%s", serverHost, serviceKey, serverPort);
				} else if ("getApplication".equals(methodName)) {
					if (this.invocationContext == null) {
						return null;
					} else if (StringUtils.isNotBlank(this.invocationContext.getServiceKey())) {
						return this.invocationContext.getServiceKey();
					} else {
						return this.invokeCoordinator(proxy, method, args);
					}
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else if (XAResource.class.equals(clazz)) {
				String serverHost = this.invocationContext == null ? null : this.invocationContext.getServerHost();
				int serverPort = this.invocationContext == null ? 0 : this.invocationContext.getServerPort();
				String remoteAddr = String.format("%s:%s", serverHost, serverPort);
				if ("start".equals(methodName)) {
					RemoteCoordinatorRegistry coordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
					if (this.invocationContext == null) {
						throw new IllegalAccessException();
					} else if (coordinatorRegistry.containsApplication(remoteAddr)) {
						return null;
					} else {
						return this.invokeCoordinator(proxy, method, args);
					}
				} else if ("prepare".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("commit".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("rollback".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("recover".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("forget".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else {
				throw new IllegalAccessException();
			}
		} finally {
			registry.desociateInvocationContext();
		}
	}

	public Object invokeCoordinator(Object proxy, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(this.remoteCoordinator, args);
		} catch (InvocationTargetException ex) {
			throw ex.getTargetException();
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	public InvocationContext getInvocationContext() {
		return invocationContext;
	}

	public void setInvocationContext(InvocationContext invocationContext) {
		this.invocationContext = invocationContext;
	}

	public RemoteCoordinator getRemoteCoordinator() {
		return remoteCoordinator;
	}

	public void setRemoteCoordinator(RemoteCoordinator remoteCoordinator) {
		this.remoteCoordinator = remoteCoordinator;
	}

}
