package org.bytesoft.bytejta.lock;

import java.sql.SQLException;
import java.sql.Statement;

public class ShareLock extends TxcLock {
    @Override
    public void lock(Statement st) throws SQLException {
        insertLock(st);
    }

    @Override
    public void unlock(Statement st) throws SQLException {
        deleteLock(st);
    }
}
