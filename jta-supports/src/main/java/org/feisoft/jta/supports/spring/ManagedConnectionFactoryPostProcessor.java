
package org.feisoft.jta.supports.spring;

import org.feisoft.jta.supports.resource.ManagedConnectionFactoryHandler;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import javax.jms.XAConnectionFactory;
import javax.resource.spi.ManagedConnectionFactory;
import javax.sql.XADataSource;
import java.lang.reflect.Proxy;

public class ManagedConnectionFactoryPostProcessor implements BeanPostProcessor {

	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

		Class<?> clazz = bean.getClass();
		ClassLoader cl = clazz.getClassLoader();

		Class<?>[] interfaces = clazz.getInterfaces();

		if (XADataSource.class.isInstance(bean)) {
			ManagedConnectionFactoryHandler interceptor = new ManagedConnectionFactoryHandler(bean);
			interceptor.setIdentifier(beanName);
			return Proxy.newProxyInstance(cl, interfaces, interceptor);
        } else if (XAConnectionFactory.class.isInstance(bean)) {
			ManagedConnectionFactoryHandler interceptor = new ManagedConnectionFactoryHandler(bean);
			interceptor.setIdentifier(beanName);
			return Proxy.newProxyInstance(cl, interfaces, interceptor);
		} else if (ManagedConnectionFactory.class.isInstance(bean)) {
			ManagedConnectionFactoryHandler interceptor = new ManagedConnectionFactoryHandler(bean);
			interceptor.setIdentifier(beanName);
			return Proxy.newProxyInstance(cl, interfaces, interceptor);
		} else {
			return bean;
		}
	}

}
