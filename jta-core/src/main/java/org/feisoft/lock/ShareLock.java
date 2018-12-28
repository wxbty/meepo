package org.feisoft.lock;

import org.feisoft.DbPool.DbPoolSource;
import org.feisoft.common.utils.SpringBeanUtil;

import java.sql.SQLException;

/*
    插入共享锁如果之前
    未锁：初始一把共享锁
    已有互斥锁：获取锁失败
    已有共享锁：共享队列+1，获取锁成功
 */
public class ShareLock extends TxcLock {

    private DbPoolSource dbPoolSource = null;

    {
        if (this.dbPoolSource == null) {
            DbPoolSource dbPoolSource = (DbPoolSource) SpringBeanUtil.getBean("dbPoolSource");
            this.dbPoolSource = dbPoolSource;
        }
    }

    @Override
    public void lock() throws SQLException {
        String sql = "select count(0) as total from  txc_lock where  table_name = '" + tableName + "' and key_value ="
                + keyValue + " and xlock='" + xlock + "'";
        int total = dbPoolSource.countList(sql);
        if (total == 0) {
            insertLock();
            insertShareLock();
        } else if (total == 1) {
            throw new SQLException();
        } else {
            insertShareLock();
        }

        setLock(Boolean.TRUE);

    }

    @Override
    public void unlock() throws SQLException {
        deleteLock();
        setLock(Boolean.FALSE);
    }

    private void insertShareLock() throws SQLException {
        String sql = "insert into txc_lock (table_name,key_value,xid,branch_id,xlock,slock,create_time)values(";
        sql += "'" + tableName + "'," + keyValue + ",'" + xid + "','" + branchId + "','" + xid + "'," + slock + ","
                + createTime;
        sql += ")";
        dbPoolSource.executeUpdate(sql);
    }

    ;
}
