package org.feisoft.jta.lock;

import org.feisoft.common.utils.DbPool.DbPoolUtil;

import java.sql.SQLException;

/*
    插入共享锁如果之前
    未锁：初始一把共享锁
    已有互斥锁：获取锁失败
    已有共享锁：共享队列+1，获取锁成功
 */
public class ShareLock extends TxcLock {

    @Override
    public void lock() throws SQLException {
        String sql = "select count(0) as total from  txc_lock where table_name = '" + tableName + "' and key_value ="
                + keyValue + " and xid='" + xid + "' and branch_id='" + branchId + "'";
        int total = DbPoolUtil.countList(sql);
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
        DbPoolUtil.executeUpdate(sql);
    }

    ;
}
