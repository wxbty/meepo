
package org.feisoft.transaction.aware;

import org.feisoft.transaction.TransactionBeanFactory;

public interface TransactionBeanFactoryAware {
	public static final String BEAN_FACTORY_FIELD_NAME = "beanFactory";

	public void setBeanFactory(TransactionBeanFactory tbf);

}
