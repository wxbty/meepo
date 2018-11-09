package org.feisoft.jta.lock;

import java.sql.SQLException;

public class MutexLock extends TxcLock {

    @Override
    public  void lock() throws SQLException {
        insertLock();
    }

    @Override
    public void unlock() throws SQLException {
        deleteLock();
    }
}
