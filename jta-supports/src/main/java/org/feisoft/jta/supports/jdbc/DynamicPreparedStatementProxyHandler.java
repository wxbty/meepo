package org.feisoft.jta.supports.jdbc;

import com.alibaba.fastjson.JSON;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.lang3.StringUtils;
import org.feisoft.DbPool.DbPoolSource;
import org.feisoft.common.utils.SpringBeanUtil;
import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.jta.TransactionImpl;
import org.feisoft.image.BackInfo;
import org.feisoft.image.resolvers.BaseResolvers;
import org.feisoft.image.resolvers.InsertImageResolvers;
import org.feisoft.lock.MutexLock;
import org.feisoft.lock.ShareLock;
import org.feisoft.lock.TxcLock;
import org.feisoft.transaction.TransactionBeanFactory;
import org.feisoft.transaction.archive.XAResourceArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.sql.XAConnection;
import javax.transaction.Status;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicPreparedStatementProxyHandler implements InvocationHandler {

    private DbPoolSource dbPoolSource = null;

    Logger logger = LoggerFactory.getLogger(DynamicPreparedStatementProxyHandler.class);

    List<String> proxyUpdateMethods = Arrays
            .asList("executeUpdate", "execute", "executeBatch", "executeLargeBatch", "executeLargeUpdate");

    private Object realObject;

    private String sql;

    private XAConnection xaConn;

    public Xid currentXid;

    private String gloableXid;

    private String branchXid;

    private List<Object> params = new ArrayList<>();

    private long timeOut = 2 * 1000;

    public DynamicPreparedStatementProxyHandler(Object realObject, String sql, XAConnection conn) {
        this.realObject = realObject;
        this.sql = sql;
        this.xaConn = conn;
        if (this.dbPoolSource == null) {
            DbPoolSource dbPoolSource = (DbPoolSource) SpringBeanUtil.getBean("dbPoolSource");
            this.dbPoolSource = dbPoolSource;
        }
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        TransactionBeanFactory jtaBeanFactory = (TransactionBeanFactory) SpringBeanUtil.getBean("jtaBeanFactory");
        TransactionImpl transaction = (TransactionImpl) jtaBeanFactory.getTransactionManager().getTransaction();

        if (transaction == null || "close".equals(method.getName())) {
            return method.invoke(realObject, args);
        }

        if (method.getName().startsWith("set") && args != null && args.length == 2) {
            params.add(args[1]);
        }
        if (Status.STATUS_ACTIVE != transaction.getStatus()) {
            throw new SQLException("Operation is disabled during the inactive phase of the transaction!");
        } else if (!transaction.isTiming()) {
            throw new SQLException(
                    "Lock operation is disabled during the inactive phase of the transaction when transaction is stoped!");
        }

        if (isQueryMethod(method) || isUpdateMethod(method.getName())) {
            if (currentXid == null) {
                List<XAResourceArchive> xaResourceArchives = transaction.getNativeParticipantList();
                if (xaResourceArchives.size() > 0) {
                    currentXid = xaResourceArchives.get(0).getXid();
                }
            }
            if (currentXid != null) {
                gloableXid = partGloableXid(currentXid);
                branchXid = partBranchXid(currentXid);
            }

        }

        if (isUpdateMethod(method.getName())) {
            if (currentXid == null) {
                //                    logger.error("method.getName()={},args={}-----没有xid-----", method.getName(), args);
                throw new SQLException("No xid");
            }
            return invokUpdate(method, args, transaction);
        }

        if (isQueryMethod(method)) {

            try {
                if (args == null || StringUtils.isEmpty(args[0].toString())) {
                    //select x
                    return method.invoke(realObject, args);
                }
                sql = args[0].toString();
                if (StringUtils.deleteWhitespace(sql.toLowerCase()).endsWith("lockinsharemode")) {
                    return LockSharedMode(method, args);
                } else {
                    net.sf.jsqlparser.statement.Statement statement;
                    try {
                        statement = CCJSqlParserUtil.parse(sql);
                    } catch (JSQLParserException e) {
                        logger.error("jsqlparser.praseFailed,sql=" + sql);
                        return method.invoke(realObject, args);
                    }
                    Select select = (Select) statement;
                    SelectBody selectBody = select.getSelectBody();
                    if (selectBody instanceof PlainSelect) {
                        PlainSelect ps = (PlainSelect) selectBody;
                        if (ps.isForUpdate()) {
                            // update
                            return lockForUpdate(method, args);
                        }
                    }

                }
            } finally {
                xaCommit(method, args);
            }
        }
        return method.invoke(realObject, args);

    }

    private boolean isQueryMethod(Method method) {
        return "executeQuery".equals(method.getName());
    }

    private boolean isUpdateMethod(String methodName) {
        return proxyUpdateMethods.contains(methodName);
    }

    private Object lockForUpdate(Method method, Object[] args)
            throws SQLException, JSQLParserException, IllegalAccessException, InvocationTargetException {
        BackInfo backInfo = new BackInfo();
        try {

            BaseResolvers resolver = BaseResolvers.newInstance(sql, backInfo);
            backInfo.setBeforeImage(resolver.genBeforeImage());
            getXlock(resolver, gloableXid, branchXid, null);
        } finally {
            xaCommit(method, args);
        }

        return method.invoke(realObject, args);
    }

    private Object LockSharedMode(Method method, Object[] args)
            throws SQLException, JSQLParserException, IllegalAccessException, InvocationTargetException {
        BackInfo backInfo = new BackInfo();

        Object obj;
        try {
            BaseResolvers resolver = BaseResolvers
                    .newInstance(sql.substring(0, sql.toLowerCase().indexOf("lock")), backInfo);
            backInfo.setBeforeImage(resolver.genBeforeImage());

            getSlock(resolver, gloableXid, branchXid);

            obj = method.invoke(realObject, args);
        } finally {
            xaCommit(method, args);
        }

        return obj;
    }

    private Object invokUpdate(Method method, Object[] args, TransactionImpl transaction)
            throws SQLException, JSQLParserException, IllegalAccessException, InvocationTargetException {
        BackInfo backInfo = new BackInfo();
        Object obj = null;
        Object pkVal = null;

        if (realObject instanceof PreparedStatement) {
            sql = printRealSql(sql, params);
        } else if (realObject instanceof Statement) {

            sql = args[0].toString();
        }
        //事务数据源从对应数据库获取前置对象

        BaseResolvers resolver;
        List<TxcLock> lockedList;
        try {
            resolver = BaseResolvers.newInstance(sql, backInfo);
            backInfo.setBeforeImage(resolver.genBeforeImage());
            lockedList = getXlock(resolver, gloableXid, branchXid, transaction);
            if (SqlpraserUtils.assertInsert(sql)) {
                ResultSet generatedKeys = null;
                if (realObject instanceof PreparedStatement) {
                    if (transaction.isTiming()) {
                        obj = method.invoke(realObject, args);
                    } else {
                        if (lockedList.size() > 0) {
                            releaseLock(lockedList);
                        }
                        throw new SQLException(
                                "Lock operation is disabled during the inactive phase of the transaction when transaction is stoped!");
                    }
                    PreparedStatement preparedStatement = (PreparedStatement) realObject;
                    generatedKeys = preparedStatement.getGeneratedKeys();
                } else if (realObject instanceof Statement) {
                    Statement realSt = (Statement) realObject;
                    if (transaction.isTiming()) {
                        obj = realSt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                    } else {
                        if (lockedList.size() > 0) {
                            releaseLock(lockedList);
                        }
                        throw new SQLException(
                                "Lock operation is disabled during the inactive phase of the transaction when transaction is stoped!");
                    }
                    generatedKeys = realSt.getGeneratedKeys();
                }
                if (generatedKeys != null) {
                    while (generatedKeys.next()) {
                        pkVal = generatedKeys.getObject(1);
                    }
                    if (!generatedKeys.isClosed()) {
                        generatedKeys.close();
                    }
                }

                if (pkVal == null) {
                    String pkKey = resolver.getMetaPrimaryKey(SqlpraserUtils.name_insert_table(sql));
                    List<String> colums = SqlpraserUtils.name_insert_column(sql);
                    List<String> values = SqlpraserUtils.name_insert_values(sql);
                    if (colums.contains(pkKey)) {
                        pkVal = values.get(colums.indexOf(pkKey));
                    }
                }
            } else {
                if (transaction.isTiming()) {
                    obj = method.invoke(realObject, args);
                } else {
                    releaseLock(lockedList);
                    throw new SQLException("Lock operation is disabled during the inactive phase of the transaction!");
                }
            }
        } catch (SQLException e) {
            //            logger.error("unhandle Exception", e);
            throw e;
        } finally {
            xaCommit(method, args);

        }

        //插入时需要获取主键的value
        if (SqlpraserUtils.assertInsert(sql)) {
            InsertImageResolvers inResolver = (InsertImageResolvers) resolver;
            inResolver.setPkVal(pkVal);
            backInfo.setAfterImage(inResolver.genAfterImage());
        } else {
            backInfo.setAfterImage(resolver.genAfterImage());
        }
        String backSqlJson = JSON.toJSONString(backInfo);
        String logSql = "INSERT INTO txc_undo_log (gmt_create,gmt_modified,xid,branch_id,rollback_info,status,server) VALUES(now(),now(),?,?,?,?,?)";
        if (obj != null) {
            if (!transaction.isTiming()) {
                logger.error("transaction is end,but insert logsql");
                if (lockedList.size() > 0) {
                    logger.info("releaseLock log,sql={},args={}", sql, args);
                    releaseLock(lockedList);
                }
                try {
                    backInfo.rollback();
                } catch (SQLException e) {
                    logger.error("BackInfo.rollback(),error", e);
                    throw e;
                }
            } else {
                logger.info("before executeUpdate logsql,transaction isTiming=" + transaction.isTiming());
                dbPoolSource.executeUpdate(logSql, gloableXid, branchXid, backSqlJson, 0, getHost());
                logger.info("logsql exe time={}", System.currentTimeMillis());
            }
        }
        //事务数据源从对应数据库获取后置对象
        return obj;
    }

    private void xaCommit(Method method, Object[] args) {
        XAResource resource;
        try {
            //本地直接提交
            resource = xaConn.getXAResource();
            try {
                resource.end(currentXid, XAResource.TMSUCCESS);
            } catch (XAException e) {
            }
            try {
                resource.prepare(currentXid);
            } catch (XAException e) {
            }
            resource.commit(currentXid, false);
        } catch (Exception ex) {
            logger.info("Local multi sqls exe!method={},args={}", method.getName(), args);
        }
    }

    private String getHost() throws SQLException {
        Connection conn = dbPoolSource.getConnection();
        DatabaseMetaData md = conn.getMetaData();
        String url = md.getURL();
        String host = "";
        Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
        Matcher matcher = p.matcher(url);
        if (matcher.find()) {
            host = matcher.group();
        }
        dbPoolSource.close(conn, null, null);
        return host;
    }

    private List<TxcLock> getXlock(BaseResolvers resolver, String gloableXid, String branchXid,
                                   TransactionImpl transaction) throws JSQLParserException, SQLException {

        Assert.notNull(transaction, "Transaction must not be null");
        long atime = System.currentTimeMillis();
        long btime = atime;
        while ((btime - atime <= this.timeOut) && transaction.isTiming()) {
            btime = System.currentTimeMillis();
            List<TxcLock> lockList = this.getMutexLock(gloableXid, branchXid, resolver);
            if (this.lockCurrent(lockList)) {
                return lockList;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        throw new SQLException("Proxy.getLockTimeout");

    }

    private boolean releaseLock(List<TxcLock> lockedList) throws SQLException {
        return this.releaseCurrent(lockedList);
    }

    private void getSlock(BaseResolvers resolver, String gloableXid, String branchXid)
            throws JSQLParserException, SQLException {

        long atime = System.currentTimeMillis();

        long btime;
        do {
            btime = System.currentTimeMillis();
            List<TxcLock> lockList = getShareLock(gloableXid, branchXid, resolver, sql);
            if (lockCurrent(lockList))
                return;

        } while (btime - atime <= this.timeOut);
        throw new SQLException("Proxy.getLockTimeout");
    }

    private boolean lockCurrent(List<TxcLock> lockList) {
        if (lockList.size() > 0) {
            for (TxcLock lock : lockList) {
                try {
                    if (!lock.isLock()) {
                        lock.lock();
                    }
                } catch (Exception e) {
                    logger.info("getXlock -- Data locked by other,retry");
                    return false;
                }
                lock.setLock(true);
            }
        }
        return true;
    }

    private boolean releaseCurrent(List<TxcLock> lockList) throws SQLException {
        if (lockList.size() > 0) {
            for (TxcLock lock : lockList) {
                try {
                    if (lock.isLock()) {
                        lock.unlock();
                    }
                } catch (SQLException e) {
                    logger.error("releaseCurrent error", e);
                    throw e;
                }
                lock.setLock(false);
            }
        }
        return true;
    }

    private List<TxcLock> getMutexLock(String gloableXid, String branchXid, BaseResolvers resolver)
            throws JSQLParserException, SQLException {

        String beforeLockSql = resolver.getLockedSet();
        String primaryKey = resolver.getMetaPrimaryKey(resolver.getTable());

        List<String> allList = dbPoolSource
                .executeQuery(beforeLockSql, rs -> rs.getObject(primaryKey).toString(), null);
        List<TxcLock> lockList = new ArrayList<>();

        if (allList.size() == 0) {
            return lockList;
        }
        String lockSql = "select key_value from txc_lock where xid='" + gloableXid + "'  and branch_id ='" + branchXid
                + "' and table_name = '" + resolver.getTable() + "' and key_value in(" + resolver.transList(allList)
                + ")";

        List<String> lockedList = dbPoolSource.executeQuery(lockSql, rs -> rs.getObject("key_value").toString(), null);

        allList.removeAll(lockedList);
        for (String unlockRecord : allList) {

            MutexLock lock = new MutexLock();
            lock.setLock(Boolean.FALSE);
            lock.setXid(gloableXid);
            lock.setBranchId(branchXid);
            lock.setTableName(resolver.getTable());
            lock.setXlock("1");
            lock.setSlock(0);
            lock.setKeyValue(unlockRecord);
            lock.setCreateTime(System.currentTimeMillis());
            lockList.add(lock);
        }

        return lockList;
    }

    private List<TxcLock> getShareLock(String gloableXid, String branchXid, BaseResolvers resolver, String sql)
            throws JSQLParserException, SQLException {

        String beforeLockSql = resolver.getLockedSet();

        String primaryKey = resolver.getMetaPrimaryKey(resolver.getTable());
        List<String> allList = dbPoolSource
                .executeQuery(beforeLockSql, rs -> rs.getObject(primaryKey).toString(), null);

        List<TxcLock> lockList = new ArrayList<>();

        String lockSql =
                "select key_value,count(*) as count from txc_lock where xid='" + gloableXid + "'  and branch_id ='"
                        + branchXid + "' and table_name = '" + resolver.getTable() + "' and key_value in(" + resolver
                        .transList(allList) + ") group by key_value";
        List<String> lockedList = dbPoolSource.executeQuery(lockSql, rs -> {
            String tmp = null;
            if (rs.getInt("count") > 1)
                tmp = rs.getString("key_value");
            return tmp;
        }, null);

        allList.removeAll(lockedList);
        for (String r3str : allList) {

            ShareLock lock = new ShareLock();
            lock.setLock(Boolean.FALSE);
            lock.setXid(gloableXid);
            lock.setBranchId(branchXid);
            lock.setTableName(resolver.getTable());
            lock.setXlock("1");
            lock.setSlock(1);
            lock.setKeyValue(r3str);
            lock.setCreateTime(System.currentTimeMillis());
            lockList.add(lock);
        }

        return lockList;
    }

    /**
     * @param sql    SQL 语句，可以带有 ? 的占位符
     * @param params 插入到 SQL 中的参数，可单个可多个可不填
     * @return 实际 sql 语句
     */
    private String printRealSql(String sql, List<Object> params) {
        if (params == null || params.size() == 0) {
            return sql;
        }

        if (!match(sql, params)) {
            logger.error("SQL 语句中的占位符与参数个数不匹配。SQL：" + sql);
            return null;
        }

        int cols = params.size();
        Object[] values = new Object[cols];
        params.toArray(values);
        for (int i = 0; i < cols; i++) {
            Object value = values[i];
            if (value instanceof Date || value instanceof Timestamp || value instanceof String
                    || value instanceof Blob) {
                values[i] = "'" + value + "'";
            } else if (value instanceof Boolean) {
                values[i] = (Boolean) value ? 1 : 0;
            }
        }

        String statement = String.format(sql.replaceAll("\\?", "%s"), values);
        return statement;
    }

    /**
     * ? 和参数的实际个数是否匹配
     *
     * @param sql    SQL 语句，可以带有 ? 的占位符
     * @param params 插入到 SQL 中的参数，可单个可多个可不填
     * @return true 表示为 ? 和参数的实际个数匹配
     */
    private boolean match(String sql, List<Object> params) {
        if (params == null || params.size() == 0)
            return true; // 没有参数，完整输出

        Matcher m = Pattern.compile("(\\?)").matcher(sql);
        int count = 0;
        while (m.find()) {
            count++;
        }

        return count == params.size();
    }

    private String partGloableXid(Xid xid) {

        byte[] gtrid = xid.getGlobalTransactionId();

        StringBuilder builder = new StringBuilder();

        if (gtrid != null) {
            appendAsHex(builder, gtrid);
        }

        return builder.toString();
    }

    private String partBranchXid(Xid xid) {

        byte[] btrid = xid.getBranchQualifier();

        StringBuilder builder = new StringBuilder();

        if (btrid != null) {
            appendAsHex(builder, btrid);
        }

        return builder.toString();
    }

    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

    private static void appendAsHex(StringBuilder builder, byte[] bytes) {
        builder.append("0x");
        for (byte b : bytes) {
            builder.append(HEX_DIGITS[(b >>> 4) & 0xF]).append(HEX_DIGITS[b & 0xF]);
        }
    }

}
