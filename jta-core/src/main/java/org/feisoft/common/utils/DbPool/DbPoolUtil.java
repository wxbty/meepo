package org.feisoft.common.utils.DbPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DbPoolUtil {

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private static DataSource dataSource;

    /*
     * 获取数据库连接对象
     */
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    //关闭资源流
    public static void close(Connection con, PreparedStatement pstmt, ResultSet rs) {

        try {
            //添加判断如果参数流参数不为空,则关闭
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    //增加/删除/修改封装
    public static int executeUpdate(String sql, Object... params) throws SQLException {
        int result = 0;
        //建立连接
        Connection con = getConnection();
        //创建命令窗口,输入SQL语句
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement(sql);
            //判断是否传入参数,如果参数不为空的话,使用循环注入参数
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            result = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(con, pstmt, null);
        }

        return result;
    }

    //查询封装,定义一个泛型方法
    public static <T> List<T> executeQuery(String sql, RowMap<T> rowmap, Object... params) throws SQLException {
        //创建一个集合接收
        List<T> list = new ArrayList<T>();
        //建立连接
        Connection con = getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            //传入参数
            pstmt = con.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            //读出
            while (rs.next()) {
                //通过RowMap接口创建一个t 来接收读取的值
                T t = rowmap.rowMapping(rs);
                if (t != null)
                    list.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
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
            e.printStackTrace();
        } finally {
            close(con, pstmt, rs);
        }

        return total;
    }

}
