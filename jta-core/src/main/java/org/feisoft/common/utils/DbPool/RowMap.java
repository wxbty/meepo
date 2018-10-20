package org.feisoft.common.utils.DbPool;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowMap<T>{
    public T rowMapping(ResultSet rs) throws SQLException;
}
