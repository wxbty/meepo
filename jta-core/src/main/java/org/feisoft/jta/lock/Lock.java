package org.feisoft.jta.lock;

import java.sql.SQLException;
import java.sql.Statement;

public interface Lock {

    public  void lock(Statement st) throws SQLException;

    public  void unlock(Statement st) throws SQLException;
}
