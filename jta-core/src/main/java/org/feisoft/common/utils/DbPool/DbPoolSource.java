package org.feisoft.common.utils.DbPool;

import org.feisoft.common.utils.SqlpraserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DbPoolSource {

    static final Logger logger = LoggerFactory.getLogger(DbPoolSource.class);

    private DataSource dataSource = null;

    public boolean inited;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        setInited(true);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setProxyDataSource(Object proxyDataSource) throws Exception {

        if (proxyDataSource instanceof DataSource) {
            setDataSource((DataSource) proxyDataSource);
        } else if (Proxy.isProxyClass(proxyDataSource.getClass())) {
            Object target = getTarget(proxyDataSource);
            if (target instanceof DataSource) {
                setDataSource((DataSource) target);
            } else if (Proxy.isProxyClass(target.getClass())) {
                setProxyDataSource((XADataSource) target);
            } else {
                setInited(false);
            }
        } else {
            setInited(false);
        }

    }

    public boolean isInited() {
        return inited;
    }

    public void setInited(boolean inited) {
        this.inited = inited;
    }

    public Connection getConnection() throws SQLException {
        if (!this.inited) {
            throw new SQLException("DbPoolSource.NotInited");
        }
        return dataSource.getConnection();
    }

    public void close(Connection con, PreparedStatement pstmt, ResultSet rs, Statement stmt) {
        close(con, pstmt, rs);
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void close(Connection con, PreparedStatement pstmt, ResultSet rs) {

        try {
            if (rs != null) {
                rs.close();
            }
            if (pstmt != null) {
                pstmt.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public int executeUpdate(String sql, Object... params) throws SQLException {
        int result = 0;
        Connection con = getConnection();
        PreparedStatement pstmt = null;
        try {
            if (SqlpraserUtils.assertInsert(sql)) {
                pstmt = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            } else {
                pstmt = con.prepareStatement(sql);
            }
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            result = pstmt.executeUpdate();
            if (SqlpraserUtils.assertInsert(sql)) {
                ResultSet generatedKeys = pstmt.getGeneratedKeys();
                if (generatedKeys != null) {
                    while (generatedKeys.next()) {
                        result = generatedKeys.getInt(1);
                    }
                    if (!generatedKeys.isClosed()) {
                        generatedKeys.close();
                    }
                }
            }

        } catch (Exception e) {
            logger.error("executeUpdate SqlExcepiton", e);
        } finally {
            close(con, pstmt, null);
        }

        return result;
    }

    public boolean exitListQuery(String sql, BoolRowMap rowmap, Object... params) throws SQLException {
        Connection con = getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            pstmt = con.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                //通过RowMap接口创建一个t 来接收读取的值
                boolean result = rowmap.booleanMapping(rs);
                if (!result)
                    return false;
            }
        } catch (Exception e) {
            logger.error("executeQuery SqlExcepiton", e);
        } finally {
            close(con, pstmt, rs);
        }
        return true;
    }

    public <T> List<T> executeQuery(String sql, RowMap<T> rowmap, Object... params) throws SQLException {
        List<T> list = new ArrayList<T>();
        Connection con = getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            pstmt = con.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                //通过RowMap接口创建一个t 来接收读取的值
                T t = rowmap.rowMapping(rs);
                if (t != null)
                    list.add(t);
            }
        } catch (Exception e) {
            logger.error("executeQuery SqlExcepiton", e);
        } finally {
            close(con, pstmt, rs);
        }

        return list;
    }

    public int countList(String sql) throws SQLException {

        //sql格式，select count(0) as total from ..
        //todo sql格式
        int total = 0;
        Connection con = getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            //传入参数
            pstmt = con.prepareStatement(sql);
            rs = pstmt.executeQuery();
            //读出
            while (rs.next()) {
                total = rs.getInt("total");
            }
        } catch (Exception e) {
            logger.error("countList SqlExcepiton", e);
        } finally {
            close(con, pstmt, rs);
        }

        return total;
    }

    /**
     * 获取 目标对象
     *
     * @param proxy 代理对象
     * @return
     * @throws Exception
     */
    public Object getTarget(Object proxy) throws Exception {
        Field field = proxy.getClass().getSuperclass().getDeclaredField("h");
        field.setAccessible(true);
        //获取指定对象中此字段的值
        Object hProxy = field.get(proxy); //获取Proxy对象中的此字段的值
        Field target = hProxy.getClass().getDeclaredField("delegate");
        target.setAccessible(true);
        return target.get(hProxy);
    }

}
