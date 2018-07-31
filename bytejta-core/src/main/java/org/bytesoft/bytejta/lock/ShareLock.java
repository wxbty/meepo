package org.bytesoft.bytejta.lock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ShareLock extends TxcLock {
    @Override
    public void lock(Statement st) throws SQLException {
        String sql = "select count(0) as count from  txc_lock where table_name = '"+tableName+"' and key_value ="+keyValue+" and xid='"+xid+"' and branch_id='"+branchId+"'";
        ResultSet rs = st.executeQuery(sql);
        if(rs.next())
        {
           int count =  rs.getInt("count");
           if (count == 0)
           {
               insertLock(st);
               insertShareLock(st);
           }else if(count == 1)
           {
               throw new SQLException();
           }else
           {
               insertShareLock(st);
           }

        }

        setLock(Boolean.TRUE);

    }

    @Override
    public void unlock(Statement st) throws SQLException {
        deleteLock(st);
        setLock(Boolean.FALSE);
    }

    private  void insertShareLock(Statement st) throws SQLException {
        String sql = "insert into txc_lock (table_name,key_value,xid,branch_id,xlock,slock,create_time)values(";
        sql += "'"+tableName+"',"+keyValue+",'"+xid+"','"+branchId+"','"+xid+"',"+slock+","+createTime;
        sql += ")";
        st.executeUpdate(sql);
    };
}
