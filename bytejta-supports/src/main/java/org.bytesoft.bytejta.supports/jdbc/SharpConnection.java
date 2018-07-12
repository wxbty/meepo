package org.bytesoft.bytejta.supports.jdbc;

import javax.sql.XAConnection;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class SharpConnection implements Connection {

    private Connection conn;

    private XAConnection xaConn;

    SharpConnection(XAConnection xaConn) {
        this.xaConn = xaConn;
        try {
            this.conn = xaConn.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Statement createStatement() throws SQLException {

        Statement st = conn.createStatement();
        return st;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement ps = null;
        if (sql.toLowerCase().startsWith("insert")) {
            ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        } else {
            ps =conn.prepareStatement(sql);
        }
        PreparedStatement proxySt = (PreparedStatement) Proxy.newProxyInstance(ClassLoader.getSystemClassLoader(), new Class[]{PreparedStatement.class}, new DynamicPreparedStatementProxyHandler(ps, sql, xaConn));
        return proxySt;
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return conn.prepareCall(sql);
    }

    public String nativeSQL(String sql) throws SQLException {
        return conn.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    public void commit() throws SQLException {
        conn.commit();
    }

    public void rollback() throws SQLException {
        conn.rollback();
    }

    public void close() throws SQLException {
        conn.close();
    }

    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        conn.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException {
        conn.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException {
        conn.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException {
        return conn.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.createStatement(resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return conn.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        conn.setTypeMap(map);
    }

    public void setHoldability(int holdability) throws SQLException {
        conn.setHoldability(holdability);
    }

    public int getHoldability() throws SQLException {
        return conn.getHoldability();
    }

    public Savepoint setSavepoint() throws SQLException {
        return conn.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return conn.setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        conn.rollback();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        conn.releaseSavepoint(savepoint);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return conn.prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return conn.prepareStatement(sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return conn.prepareStatement(sql, columnNames);
    }

    public Clob createClob() throws SQLException {
        return conn.createClob();
    }

    public Blob createBlob() throws SQLException {
        return conn.createBlob();
    }

    public NClob createNClob() throws SQLException {
        return conn.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return conn.createSQLXML();
    }

    public boolean isValid(int timeout) throws SQLException {
        return conn.isValid(timeout);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        conn.setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        conn.setClientInfo(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return conn.getClientInfo(name);
    }

    public Properties getClientInfo() throws SQLException {
        return conn.getClientInfo();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return conn.createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return conn.createStruct(typeName, attributes);
    }

    public void setSchema(String schema) throws SQLException {
        conn.setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return conn.getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        conn.abort(executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        conn.setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return conn.getNetworkTimeout();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return conn.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return conn.isWrapperFor(iface);
    }
}
