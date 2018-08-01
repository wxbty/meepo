
package org.feisoft.jta.supports.dubbo.serialize;


import org.feisoft.jta.supports.dubbo.DubboRemoteCoordinator;
import org.feisoft.jta.supports.dubbo.InvocationContext;
import org.feisoft.jta.supports.dubbo.TransactionBeanRegistry;
import org.feisoft.jta.supports.resource.RemoteResourceDescriptor;
import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.feisoft.jta.supports.wire.RemoteCoordinatorRegistry;
import org.feisoft.transaction.supports.resource.XAResourceDescriptor;
import org.feisoft.transaction.supports.serialize.XAResourceDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.lang.reflect.Proxy;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XAResourceDeserializerImpl implements XAResourceDeserializer, ApplicationContextAware {
	static final Logger logger = LoggerFactory.getLogger(XAResourceDeserializerImpl.class);
	static Pattern pattern = Pattern.compile("^[^:]+\\s*:\\s*[^:]+\\s*:\\s*\\d+$");

	private XAResourceDeserializer resourceDeserializer;
	private ApplicationContext applicationContext;

	public XAResourceDescriptor deserialize(String identifier) {
		XAResourceDescriptor resourceDescriptor = this.resourceDeserializer.deserialize(identifier);
		if (resourceDescriptor != null) {
			return resourceDescriptor;
		}

		Matcher matcher = pattern.matcher(identifier);
		if (matcher.find()) {
			RemoteCoordinatorRegistry registry = RemoteCoordinatorRegistry.getInstance();
			RemoteCoordinator coordinator = registry.getRemoteCoordinator(identifier);
			if (coordinator == null) {
				String[] array = identifier.split("\\:");
				InvocationContext invocationContext = new InvocationContext();
				invocationContext.setServerHost(array[0]);
				invocationContext.setServiceKey(array[1]);
				invocationContext.setServerPort(Integer.valueOf(array[2]));

				TransactionBeanRegistry beanRegistry = TransactionBeanRegistry.getInstance();
				RemoteCoordinator consumeCoordinator = beanRegistry.getConsumeCoordinator();

				DubboRemoteCoordinator dubboCoordinator = new DubboRemoteCoordinator();
				dubboCoordinator.setInvocationContext(invocationContext);
				dubboCoordinator.setRemoteCoordinator(consumeCoordinator);

				coordinator = (RemoteCoordinator) Proxy.newProxyInstance(DubboRemoteCoordinator.class.getClassLoader(),
						new Class[] { RemoteCoordinator.class }, dubboCoordinator);
				registry.putRemoteCoordinator(identifier, coordinator);
			}

			RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
			descriptor.setIdentifier(identifier);
			descriptor.setDelegate(registry.getRemoteCoordinator(identifier));

			return descriptor;
		} else {
			logger.error("can not find a matching xa-resource(identifier= {})!", identifier);
			return null;
		}

	}

	public XAResourceDeserializer getResourceDeserializer() {
		return resourceDeserializer;
	}

	public void setResourceDeserializer(XAResourceDeserializer resourceDeserializer) {
		this.resourceDeserializer = resourceDeserializer;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}
