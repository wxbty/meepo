
package org.feisoft.jta.supports.jdbc;

import org.feisoft.transaction.Transaction;
import org.feisoft.transaction.TransactionContext;
import org.feisoft.transaction.supports.resource.XAResourceDescriptor;
import org.springframework.beans.factory.BeanNameAware;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class LocalXADataSource /* extends TransactionListenerAdapter */
		implements XADataSource, DataSource, DataSourceHolder, BeanNameAware {
	private PrintWriter logWriter;
	private int loginTimeout;

	private DataSource dataSource;
	private String beanName;
	@javax.annotation.Resource
	private TransactionManager transactionManager;

	public Connection getConnection() throws SQLException {
		try {
			Transaction transaction = (Transaction) this.transactionManager.getTransaction();
			if (transaction == null) {
				return this.dataSource.getConnection();
			}

			XAResourceDescriptor descriptor = transaction.getResourceDescriptor(this.beanName);
			LocalXAResource resource = descriptor == null ? null : (LocalXAResource) descriptor.getDelegate();
			LocalXAConnection xacon = resource == null ? null : resource.getManagedConnection();

			if (xacon != null) {
				return xacon.getConnection();
			}

			xacon = this.getXAConnection();
			LogicalConnection connection = xacon.getConnection();
			descriptor = xacon.getXAResource();
			LocalXAResource localXARes = (LocalXAResource) descriptor.getDelegate();
			TransactionContext transactionContext = transaction.getTransactionContext();
			boolean loggingRequired = LocalXACompatible.class.isInstance(transactionContext) //
					? ((LocalXACompatible) transactionContext).compatibleLoggingLRO() : false;
			localXARes.setLoggingRequired(loggingRequired);
			transaction.enlistResource(descriptor);

			return connection;
		} catch (SystemException ex) {
			throw new SQLException(ex);
		} catch (RollbackException ex) {
			throw new SQLException(ex);
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		}

	}

	public Connection getConnection(String username, String password) throws SQLException {
		try {
			Transaction transaction = (Transaction) this.transactionManager.getTransaction();
			if (transaction == null) {
				return this.dataSource.getConnection(username, password);
			}

			XAResourceDescriptor descriptor = transaction.getResourceDescriptor(this.beanName);
			LocalXAResource resource = descriptor == null ? null : (LocalXAResource) descriptor.getDelegate();
			LocalXAConnection xacon = resource == null ? null : resource.getManagedConnection();

			if (xacon != null) {
				return xacon.getConnection();
			}

			xacon = this.getXAConnection(username, password);
			LogicalConnection connection = xacon.getConnection();
			descriptor = xacon.getXAResource();
			LocalXAResource localXARes = (LocalXAResource) descriptor.getDelegate();
			TransactionContext transactionContext = transaction.getTransactionContext();
			boolean loggingRequired = LocalXACompatible.class.isInstance(transactionContext) //
					? ((LocalXACompatible) transactionContext).compatibleLoggingLRO() : false;
			localXARes.setLoggingRequired(loggingRequired);
			transaction.enlistResource(descriptor);

			return connection;
		} catch (SystemException ex) {
			throw new SQLException(ex);
		} catch (RollbackException ex) {
			throw new SQLException(ex);
		} catch (RuntimeException ex) {
			throw new SQLException(ex);
		}

	}

	public boolean isWrapperFor(Class<?> iface) {
		if (iface == null) {
			return false;
		} else if (iface.isInstance(this)) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public <T> T unwrap(Class<T> iface) {
		if (iface == null) {
			return null;
		} else if (iface.isInstance(this)) {
			return (T) this;
		}
		return null;
	}

	public LocalXAConnection getXAConnection() throws SQLException {
		Connection connection = this.dataSource.getConnection();
		LocalXAConnection xacon = new LocalXAConnection(connection);
		xacon.setResourceId(this.beanName);
		return xacon;
	}

	public LocalXAConnection getXAConnection(String user, String passwd) throws SQLException {
		Connection connection = this.dataSource.getConnection(user, passwd);
		LocalXAConnection xacon = new LocalXAConnection(connection);
		xacon.setResourceId(this.beanName);
		return xacon;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBeanName(String name) {
		this.beanName = name;
	}

	public PrintWriter getLogWriter() {
		return logWriter;
	}

	public void setLogWriter(PrintWriter logWriter) {
		this.logWriter = logWriter;
	}

	public int getLoginTimeout() {
		return loginTimeout;
	}

	public void setLoginTimeout(int loginTimeout) {
		this.loginTimeout = loginTimeout;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public TransactionManager getTransactionManager() {
		return transactionManager;
	}

	public void setTransactionManager(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

}
