package org.bytesoft.bytejta.lock;

import java.sql.SQLException;
import java.sql.Statement;

public abstract class TxcLock implements  Lock{

    protected Long id;
    protected String tableName;
    protected String keyValue;

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    protected String xid;
    protected String branchId;
    protected String xlock;

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    protected long createTime;

    private  boolean isLock;

    public boolean isLock() {
        return isLock;
    }

    public void setLock(boolean lock) {
        isLock = lock;
    }

    public Integer getSlock() {
        return slock;
    }

    public void setSlock(Integer slock) {
        this.slock = slock;
    }

    protected Integer slock;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public String getBranchId() {
        return branchId;
    }

    public void setBranchId(String branchId) {
        this.branchId = branchId;
    }

    public String getXlock() {
        return xlock;
    }

    public void setXlock(String xlock) {
        this.xlock = xlock;
    }



    @Override
    public abstract void lock(Statement st) throws SQLException;

    @Override
    public abstract void unlock(Statement st) throws SQLException;

    protected  void insertLock(Statement st) throws SQLException {
        String sql = "insert into txc_lock (table_name,key_value,xid,branch_id,xlock,slock,create_time)values(";
        sql += "'"+tableName+"',"+keyValue+",'"+xid+"','"+branchId+"','"+xlock+"',"+slock+","+createTime;
        sql += ")";
        st.executeUpdate(sql);
    };



    protected  void deleteLock(Statement st) throws SQLException {
        String sql = "delete from txc_lock  where id = "+id;
        st.executeUpdate(sql);
    };

}
