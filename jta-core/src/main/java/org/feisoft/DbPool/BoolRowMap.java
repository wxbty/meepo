package org.feisoft.DbPool;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface BoolRowMap<T>{

    boolean  booleanMapping(ResultSet rs) throws SQLException;
}
