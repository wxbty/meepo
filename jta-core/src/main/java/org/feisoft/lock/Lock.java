package org.feisoft.lock;

import java.sql.SQLException;

public interface Lock {

    public  void lock() throws SQLException;

    public  void unlock() throws SQLException;
}
