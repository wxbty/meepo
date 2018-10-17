/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
 * <p>
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.feisoft.jta.supports.jdbc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.resource.XATerminatorImpl;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.logging.Logger;

public class XADataSourceImpl implements XADataSource, BeanNameAware,InitializingBean {

    static final org.slf4j.Logger logger = LoggerFactory.getLogger(XADataSourceImpl.class);

    private int timeoutSeconds = 5 * 6000;
    private int expireMilliSeconds = 15 * 1000;

    private String identifier;
    private XADataSource xaDataSource;
    public static String user;
    public static String password;
    public static String url;
    public static String className;


    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;

    }

    private String driverClass;

    public XADataSourceImpl() {

    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public PrintWriter getLogWriter() throws SQLException {
        return this.xaDataSource.getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        this.xaDataSource.setLogWriter(out);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        this.xaDataSource.setLoginTimeout(seconds);
    }

    public int getLoginTimeout() throws SQLException {
        return this.xaDataSource.getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.xaDataSource.getParentLogger();
    }

    public XAConnection getXAConnection() throws SQLException {


        XAConnectionImpl managed = new XAConnectionImpl();
        managed.setIdentifier(this.identifier);
        XAConnection delegate = this.xaDataSource.getXAConnection();
        managed.setDelegate(delegate);
        delegate.addConnectionEventListener(managed);
        return managed;
    }

    public XAConnection getXAConnection(String user, String password) throws SQLException {


        XAConnectionImpl managed = new XAConnectionImpl();
        managed.setIdentifier(this.identifier);
        XAConnection delegate = this.xaDataSource.getXAConnection(user, password);
        managed.setDelegate(delegate);
        delegate.addConnectionEventListener(managed);
        return managed;
    }

    public void setBeanName(String name) {
        this.setIdentifier(name);
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public XADataSource getXaDataSource() {
        return xaDataSource;
    }

    public void setXaDataSource(XADataSource xaDataSource) {
        this.xaDataSource = xaDataSource;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            Class cls = Class.forName(className);
            Object obj = cls.newInstance();
            Method setUrl = cls.getMethod("setUrl", String.class);
            setUrl.invoke(obj, url);
            Method setUser = cls.getMethod("setUser", String.class);
            setUser.invoke(obj, user);
            Method setPass = cls.getMethod("setPassword", String.class);
            setPass.invoke(obj, password);
            XADataSource xs = (XADataSource) obj;
            this.xaDataSource = xs;
            XATerminatorImpl.sourceProp.put("url",url);
            XATerminatorImpl.sourceProp.put("user",user);
            XATerminatorImpl.sourceProp.put("password",password);
            XATerminatorImpl.sourceProp.put("className",className);
            rollbackOverTimeImage();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    private void rollbackOverTimeImage() {


        long now = System.currentTimeMillis();
        logger.debug("TransactionManagerImpl.rollbackOverTimeImage,now={}", now);

        ResultSet rs = null;
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(className);
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            String sql = "select u.id, rollback_info,k.create_time from txc_lock k,txc_undo_log u where k.xid=u.xid and k.branch_id=u.branch_id and  k.create_time +" + expireMilliSeconds + "< " + now;
            rs = stmt.executeQuery(sql);
            boolean isExpire = false;
            while (rs.next()) {
                Long nowMillis = System.currentTimeMillis();
                Long txMillis = rs.getLong("create_time");
                if (nowMillis - txMillis < timeoutSeconds) {
                    logger.debug("Not go to expired txc,continue!");
                    continue;
                }

                isExpire = true;
                String imageInfo = rs.getString("rollback_info");
                logger.info("TransactionManagerImpl.ExeBackinfo:{}", imageInfo);

                BackInfo backInfo = JSON.parseObject(imageInfo, new TypeReference<BackInfo>() {});
                backInfo.setId(rs.getLong("id"));
                Connection connS = DriverManager.getConnection(url, user, password);
                Statement stmtS = connS.createStatement();
                backInfo.rollback(stmtS);
                backInfo.updateStatusFinish(stmtS);
                if (stmtS != null)
                    stmtS.close();
            }

            if (isExpire) {
                sql = "delete from txc_lock where  create_time +" + expireMilliSeconds + "< " + now;
                stmt.execute(sql);
            }
        } catch (ClassNotFoundException e) {
            logger.error("Config.classNameNotFound", e);
        } catch (SQLException e) {
            logger.error("SQLException", e);
        } catch (XAException e) {
            logger.error("XAException", e);
        } finally {
            try {
                if (conn != null)
                    conn.close();
                if (stmt != null)
                    stmt.close();
                if (rs == null )
                {
                    logger.info("rs == null ");
                }
                if (rs!=null && !rs.isClosed()) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
