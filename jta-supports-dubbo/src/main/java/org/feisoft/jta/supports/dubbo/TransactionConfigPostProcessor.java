
package org.feisoft.jta.supports.dubbo;

import org.apache.commons.lang3.StringUtils;
import org.feisoft.jta.supports.wire.RemoteCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;

public class TransactionConfigPostProcessor implements BeanFactoryPostProcessor {
	static final Logger logger = LoggerFactory.getLogger(TransactionConfigPostProcessor.class);

	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
		String[] beanNameArray = beanFactory.getBeanDefinitionNames();

		String applicationBeanId = null;
		String registryBeanId = null;
		String transactionBeanId = null;

		for (int i = 0; i < beanNameArray.length; i++) {
			String beanName = beanNameArray[i];
			BeanDefinition beanDef = beanFactory.getBeanDefinition(beanName);
			String beanClassName = beanDef.getBeanClassName();
			if (com.alibaba.dubbo.config.ApplicationConfig.class.getName().equals(beanClassName)) {
				if (StringUtils.isBlank(applicationBeanId)) {
					applicationBeanId = beanName;
				} else {
					throw new FatalBeanException("There are more than one application name was found!");
				}
			} else if (org.feisoft.jta.TransactionCoordinator.class.getName().equals(beanClassName)) {
				if (StringUtils.isBlank(transactionBeanId)) {
					transactionBeanId = beanName;
				} else {
					throw new FatalBeanException(
							"There are more than one org.feisoft.jta.TransactionCoordinator was found!");
				}
			} else if (org.feisoft.jta.supports.dubbo.TransactionBeanRegistry.class.getName().equals(beanClassName)) {
				if (StringUtils.isBlank(registryBeanId)) {
					registryBeanId = beanName;
				} else {
					throw new FatalBeanException(
							"There are more than one org.feisoft.jta.supports.dubbo.TransactionBeanRegistry was found!");
				}
			}
		}

		if (StringUtils.isBlank(applicationBeanId)) {
			throw new FatalBeanException("No application name was found!");
		}

		BeanDefinition beanDef = beanFactory.getBeanDefinition(applicationBeanId);
		MutablePropertyValues mpv = beanDef.getPropertyValues();
		PropertyValue pv = mpv.getPropertyValue("name");

		if (pv == null || pv.getValue() == null || StringUtils.isBlank(String.valueOf(pv.getValue()))) {
			throw new FatalBeanException("No application name was found!");
		}

		if (StringUtils.isBlank(transactionBeanId)) {
			throw new FatalBeanException("No configuration of class org.feisoft.jta.TransactionCoordinator was found.");
		} else if (registryBeanId == null) {
			throw new FatalBeanException(
					"No configuration of class org.feisoft.jta.supports.dubbo.TransactionBeanRegistry was found.");
		}

		String application = String.valueOf(pv.getValue());
		this.initializeForProvider(beanFactory, application, transactionBeanId);
		this.initializeForConsumer(beanFactory, application, registryBeanId);
	}

	public void initializeForProvider(ConfigurableListableBeanFactory beanFactory, String application, String refBeanName)
			throws BeansException {
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;

		GenericBeanDefinition beanDef = new GenericBeanDefinition();
		beanDef.setBeanClass(com.alibaba.dubbo.config.spring.ServiceBean.class);

		MutablePropertyValues mpv = beanDef.getPropertyValues();
		mpv.addPropertyValue("interface", RemoteCoordinator.class.getName());
		mpv.addPropertyValue("ref", new RuntimeBeanReference(refBeanName));
		mpv.addPropertyValue("cluster", "failfast");
		mpv.addPropertyValue("loadbalance", "transaction");
		mpv.addPropertyValue("filter", "transaction");
		mpv.addPropertyValue("group", "org.feisoft.jta");
		mpv.addPropertyValue("retries", "0");
//		mpv.addPropertyValue("timeout", "12000");

		String skeletonBeanId = String.format("skeleton@%s", RemoteCoordinator.class.getName());
		registry.registerBeanDefinition(skeletonBeanId, beanDef);
	}

	public void initializeForConsumer(ConfigurableListableBeanFactory beanFactory, String application, String targetBeanName)
			throws BeansException {
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) beanFactory;

		// <dubbo:reference id="yyy"
		// interface="org.feisoft.jta.supports.wire.RemoteCoordinator"
		// timeout="6000" group="org.feisoft.jta" loadbalance="transaction" cluster="failfast" />
		GenericBeanDefinition beanDef = new GenericBeanDefinition();
		beanDef.setBeanClass(com.alibaba.dubbo.config.spring.ReferenceBean.class);

		MutablePropertyValues mpv = beanDef.getPropertyValues();
		mpv.addPropertyValue("interface", RemoteCoordinator.class.getName());
//		mpv.addPropertyValue("timeout", "12000");
		mpv.addPropertyValue("cluster", "failfast");
		mpv.addPropertyValue("loadbalance", "transaction");
		mpv.addPropertyValue("filter", "transaction");
		mpv.addPropertyValue("group", "org.feisoft.jta");
		mpv.addPropertyValue("check", "false");

		String stubBeanId = String.format("stub@%s", RemoteCoordinator.class.getName());
		registry.registerBeanDefinition(stubBeanId, beanDef);

		// <bean id="xxx" class="org.feisoft.jta.supports.dubbo.TransactionBeanRegistry"
		// factory-method="getInstance">
		// <property name="consumeCoordinator" ref="yyy" />
		// </bean>
		BeanDefinition targetBeanDef = beanFactory.getBeanDefinition(targetBeanName);
		MutablePropertyValues targetMpv = targetBeanDef.getPropertyValues();
		targetMpv.addPropertyValue("consumeCoordinator", new RuntimeBeanReference(stubBeanId));
	}

}
