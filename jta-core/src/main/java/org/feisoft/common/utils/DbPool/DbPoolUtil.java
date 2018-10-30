package org.feisoft.common.utils.DbPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DbPoolUtil {

    static final Logger logger = LoggerFactory.getLogger(DbPoolUtil.class);

    private static DataSource dataSource;

    public static boolean inited;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        setInited(true);
    }

    public static boolean isInited() {
        return inited;
    }

    public static void setInited(boolean inited) {
        DbPoolUtil.inited = inited;
    }

    public static Connection getConnection() throws SQLException {
        if (!inited)
        {
            throw new SQLException("DbPoolUtil.NotInited");
        }
        return dataSource.getConnection();
    }

    public static void close(Connection con, PreparedStatement pstmt, ResultSet rs, Statement stmt) {
        close(con, pstmt, rs);
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void close(Connection con, PreparedStatement pstmt, ResultSet rs) {

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

    public static int executeUpdate(String sql, Object... params) throws SQLException {
        int result = 0;
        Connection con = getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            result = pstmt.executeUpdate();
        } catch (Exception e) {
            logger.error("executeUpdate SqlExcepiton", e);
        } finally {
            close(con, pstmt, null);
        }

        return result;
    }

    public static <T> List<T> executeQuery(String sql, RowMap<T> rowmap, Object... params) throws SQLException {
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

    public static int countList(String sql) throws SQLException {

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

}
