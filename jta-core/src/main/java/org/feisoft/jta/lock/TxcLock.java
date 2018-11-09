package org.feisoft.jta.lock;

import org.feisoft.common.utils.DbPool.DbPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public abstract class TxcLock implements Lock {

    static final Logger logger = LoggerFactory.getLogger(TxcLock.class);

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

    private boolean isLock;

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
    public abstract void lock() throws SQLException;

    @Override
    public abstract void unlock() throws SQLException;

    protected void insertLock() throws SQLException {

        //        synchronized (TxcLock.class) {
        String sql = "select count(0) as total from  txc_lock where table_name = '" + tableName + "' and key_value ="
                + keyValue + " and xlock='" + xlock + "'";
        int total = DbPoolUtil.countList(sql);
        if (total > 0) {
            throw new SQLException();
        }

        String insert_sql = "insert into txc_lock (table_name,key_value,xid,branch_id,xlock,slock,create_time)values(";
        insert_sql +=
                "'" + tableName + "'," + keyValue + ",'" + xid + "','" + branchId + "','" + xlock + "'," + slock + ","
                        + createTime;
        insert_sql += ")";
        try {
            DbPoolUtil.executeUpdate(insert_sql);
        } catch (Exception e) {
            logger.error("插入lock失败，totol_sql ={},insert_sql={},total={}", sql, insert_sql, total, e);
            throw new SQLException();

        }
        //        }
    }

    ;

    protected void deleteLock() throws SQLException {
        String sql = "delete from txc_lock  where id = " + id;
        DbPoolUtil.executeUpdate(sql);
    }

    ;

}
