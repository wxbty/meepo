
package org.feisoft.jta.supports.dubbo;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import org.feisoft.common.utils.CommonUtils;
import org.feisoft.transaction.aware.TransactionEndpointAware;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.ArrayList;
import java.util.List;

public class TransactionEndpointPostProcessor implements BeanFactoryPostProcessor {

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();

		BeanDefinition applicationDef = null;
		BeanDefinition protocolDef = null;

		List<BeanDefinition> beanDefList = new ArrayList<BeanDefinition>();
		String[] beanNameArray = beanFactory.getBeanDefinitionNames();
		for (int i = 0; i < beanNameArray.length; i++) {
			String beanName = beanNameArray[i];
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			String beanClassName = beanDef.getBeanClassName();

			Class<?> beanClass = null;
			try {
				beanClass = cl.loadClass(beanClassName);
			} catch (Exception ex) {
				continue;
			}

			if (TransactionEndpointAware.class.isAssignableFrom(beanClass)) {
				beanDefList.add(beanDef);
			} else if (ProtocolConfig.class.isAssignableFrom(beanClass)) {
				if (protocolDef == null) {
					protocolDef = beanDef;
				} else {
					throw new FatalBeanException("There are more than one com.alibaba.dubbo.config.ProtocolConfig was found!");
				}
			} else if (ApplicationConfig.class.isAssignableFrom(beanClass)) {
				if (applicationDef == null) {
					applicationDef = beanDef;
				} else {
					throw new FatalBeanException(
							"There are more than one com.alibaba.dubbo.config.ApplicationConfig was found!");
				}
			}
		}

		if (applicationDef == null) {
			throw new FatalBeanException("No configuration of class com.alibaba.dubbo.config.ApplicationConfig was found.");
		} else if (protocolDef == null) {
			throw new FatalBeanException("No configuration of class com.alibaba.dubbo.config.ProtocolConfig was found.");
		}

		MutablePropertyValues applicationValues = applicationDef.getPropertyValues();
		PropertyValue applicationValue = applicationValues.getPropertyValue("name");
		if (applicationValue == null || applicationValue.getValue() == null) {
			throw new FatalBeanException("Attribute 'name' of <dubbo:application ... /> is null.");
		}

		MutablePropertyValues protocolValues = protocolDef.getPropertyValues();
		PropertyValue protocolValue = protocolValues.getPropertyValue("port");
		if (protocolValue == null || protocolValue.getValue() == null) {
			throw new FatalBeanException("Attribute 'port' of <dubbo:protocol ... /> is null.");
		}

		String host = CommonUtils.getInetAddress();
		String name = String.valueOf(applicationValue.getValue());
		String port = String.valueOf(protocolValue.getValue());
		String identifier = String.format("%s:%s:%s", host, name, port);

		for (int i = 0; i < beanDefList.size(); i++) {
			BeanDefinition beanDef = beanDefList.get(i);
			MutablePropertyValues mpv = beanDef.getPropertyValues();
			mpv.addPropertyValue(TransactionEndpointAware.ENDPOINT_FIELD_NAME, identifier);
		}

	}

}
