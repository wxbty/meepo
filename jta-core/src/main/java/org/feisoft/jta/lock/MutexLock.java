package org.feisoft.jta.lock;

import java.sql.SQLException;
import java.sql.Statement;

public class MutexLock extends TxcLock {



    @Override
    public void lock(Statement st) throws SQLException {
        //查询加锁记录，根据tableName，keyValue及xlock查询记录R2,分支sql查询记录数R1，查出未加锁的记录R3=R1-R2，并加锁（同一个事务内）
        insertLock(st);
    }

    @Override
    public void unlock(Statement st) throws SQLException {
        deleteLock(st);
    }
}
