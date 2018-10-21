package org.feisoft.jta.lock;

import java.sql.SQLException;

public class MutexLock extends TxcLock {



    @Override
    public void lock() throws SQLException {
        //获取互斥锁，锁唯一索引再锁失败
        insertLock();
    }

    @Override
    public void unlock() throws SQLException {
        deleteLock();
    }
}
