package org.feisoft.common.utils;

import javax.sql.DataSource;
import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.SQLException;

public class DbPoolUtil {

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private static DataSource dataSource;

    /*
     * 获取数据库连接对象
     */
    public static Connection getConnection() throws XAException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new XAException("DbPoolUtil.getConnection error!");
        }
    }

}
