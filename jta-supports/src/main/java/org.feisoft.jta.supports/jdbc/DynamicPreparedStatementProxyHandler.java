package org.feisoft.jta.supports.jdbc;

import com.alibaba.fastjson.JSON;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.apache.commons.lang3.StringUtils;
import org.feisoft.common.utils.DbPoolUtil;
import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.jta.TransactionImpl;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.image.Resolvers.BaseResolvers;
import org.feisoft.jta.image.Resolvers.ImageUtil;
import org.feisoft.jta.image.Resolvers.InsertImageResolvers;
import org.feisoft.jta.lock.MutexLock;
import org.feisoft.jta.lock.ShareLock;
import org.feisoft.jta.lock.TxcLock;
import org.feisoft.jta.supports.spring.SpringBeanUtil;
import org.feisoft.transaction.Transaction;
import org.feisoft.transaction.TransactionBeanFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XAConnection;
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

    Logger logger = LoggerFactory.getLogger(DynamicPreparedStatementProxyHandler.class);

    private Object realObject;

    private String sql;

    private XAConnection xaConn;

    private List<Object> params = new ArrayList<>();

    private int timeOut = 50 * 1000;

    public DynamicPreparedStatementProxyHandler(Object realObject, String sql, XAConnection conn) {
        this.realObject = realObject;
        this.sql = sql;
        this.xaConn = conn;

    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        TransactionBeanFactory jtaBeanFactory = (TransactionBeanFactory) SpringBeanUtil.getBean("jtaBeanFactory");
        Transaction transaction = jtaBeanFactory.getTransactionManager().getTransaction();

        if (transaction == null) {
            return method.invoke(realObject, args);
        }

        System.out.println("invoke method=" + method.getName());
        if (method.getName().startsWith("set") && args != null && args.length == 2) {
//            Integer seq = (Integer) args[0];
            params.add(args[1]);
        }
        List<String> proxyMethods = Arrays
                .asList("executeUpdate", "execute", "executeBatch", "executeLargeBatch", "executeLargeUpdate");
        if (proxyMethods.contains(method.getName())) {
            return invokUpdate(method, args);
        }

        if ("executeQuery".equals(method.getName())) {

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
                        //select for update
                        return lockForUpdate(method, args);
                    }
                }

            }
        }
        return method.invoke(realObject, args);
    }

    private Object lockForUpdate(Method method, Object[] args)
            throws SQLException, XAException, JSQLParserException, IllegalAccessException, InvocationTargetException {
        Connection conn = xaConn.getConnection();
        Statement st = conn.createStatement();
        BackInfo backInfo = new BackInfo();
        BaseResolvers resolver = ImageUtil.getImageResolvers(sql, backInfo, conn, st);
        backInfo.setBeforeImage(resolver.genBeforeImage());
        Xid currentXid = TransactionImpl.currentXid.get();
        String GloableXid = partGloableXid(currentXid);
        String branchXid = partBranchXid(currentXid);
        getXlock(conn, st, resolver, GloableXid, branchXid);

        return method.invoke(realObject, args);
    }

    private Object LockSharedMode(Method method, Object[] args)
            throws ClassNotFoundException, SQLException, XAException, JSQLParserException, IllegalAccessException,
            InvocationTargetException {
        Connection conn = null;
        Statement st = null;
        Xid currentXid;
        try {
            Class.forName(XADataSourceImpl.className);
            conn = DriverManager.getConnection(XADataSourceImpl.url, XADataSourceImpl.user, XADataSourceImpl.password);
            st = conn.createStatement();

            BackInfo backInfo = new BackInfo();
            BaseResolvers resolver = ImageUtil
                    .getImageResolvers(sql.substring(0, sql.toLowerCase().indexOf("lock")), backInfo, conn, st);
            backInfo.setBeforeImage(resolver.genBeforeImage());
            currentXid = TransactionImpl.currentXid.get();
            String GloableXid = partGloableXid(currentXid);
            String branchXid = partBranchXid(currentXid);
            getSlock(conn, st, resolver, GloableXid, branchXid);
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (st != null) {
                st.close();
            }
        }
        Object obj = method.invoke(realObject, args);
        //本地直接提交
        xaConn.getXAResource().end(currentXid, XAResource.TMSUCCESS);
        xaConn.getXAResource().prepare(currentXid);
        return obj;
    }

    private Object invokUpdate(Method method, Object[] args)
            throws ClassNotFoundException, SQLException, XAException, JSQLParserException, IllegalAccessException,
            InvocationTargetException {
        BackInfo backInfo = new BackInfo();
        PreparedStatement ps = null;
        Object obj = null;
        Object pkVal = null;

        if (realObject instanceof PreparedStatement) {
            sql = printRealSql(sql, params);
        } else if (realObject instanceof Statement) {

            sql = args[0].toString();
        }
        Xid currentXid = TransactionImpl.currentXid.get();
        //事务数据源从对应数据库获取前置对象
        Connection conn = DbPoolUtil.getConnection();
        Statement st = conn.createStatement();

        BaseResolvers resolver = ImageUtil.getImageResolvers(sql, backInfo, conn, st);
        backInfo.setBeforeImage(resolver.genBeforeImage());
        String GloableXid = partGloableXid(currentXid);
        String branchXid = partBranchXid(currentXid);
        getXlock(conn, st, resolver, GloableXid, branchXid);
        String logSql = "INSERT INTO txc_undo_log (gmt_create,gmt_modified,xid,branch_id,rollback_info,status,server) VALUES(now(),now(),?,?,?,?,?)";
        ps = conn.prepareStatement(logSql);
        ps.setString(1, GloableXid);
        ps.setString(2, branchXid);
        ps.setInt(4, 0);
        ps.setString(5, getHost(conn));

        if (SqlpraserUtils.assertInsert(sql)) {
            ResultSet generatedKeys = null;
            if (realObject instanceof PreparedStatement) {
                obj = method.invoke(realObject, args);
                PreparedStatement preparedStatement = (PreparedStatement) realObject;
                generatedKeys = preparedStatement.getGeneratedKeys();
            } else if (realObject instanceof Statement) {
                Statement realSt = (Statement) realObject;
                obj = realSt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
                generatedKeys = realSt.getGeneratedKeys();
            }
            while (generatedKeys.next()) {
                pkVal = generatedKeys.getObject(1);
            }
            if (!generatedKeys.isClosed()) {
                generatedKeys.close();
            }

            if (pkVal == null) {
                String pkKey = resolver.getMetaPrimaryKey(conn, SqlpraserUtils.name_insert_table(sql));
                List<String> colums = SqlpraserUtils.name_insert_column(sql);
                List<String> values = SqlpraserUtils.name_insert_values(sql);
                if (colums.contains(pkKey)) {
                    pkVal = values.get(colums.indexOf(pkKey));
                }
            }
        } else {
            obj = method.invoke(realObject, args);
        }

        //本地直接提交
        XAResource resource = xaConn.getXAResource();

        try {
            resource.end(currentXid, XAResource.TMSUCCESS);
            resource.prepare(currentXid);
            resource.commit(currentXid, false);
        } catch (Exception ex) {
            logger.info("Local multi sqls exe!");
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
        ps.setString(3, backSqlJson);
        ps.executeUpdate();         //执行sql语句

        if (st != null) {
            st.close();
        }
        if (conn != null) {
            conn.close();
        }
        //事务数据源从对应数据库获取后置对象
        return obj;
    }

    private String getHost(Connection conn) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        String url = md.getURL();
        String host = "";
        Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
        Matcher matcher = p.matcher(url);
        if (matcher.find()) {
            host = matcher.group();
        }
        return host;
    }

    private void getXlock(Connection conn, Statement st, BaseResolvers resolver, String gloableXid, String branchXid)
            throws XAException, JSQLParserException, SQLException {

        long atime = System.currentTimeMillis();

        do {
            long btime = System.currentTimeMillis();
            List<TxcLock> lockList = getMutexLock(gloableXid, branchXid, resolver, sql, conn, st);
            if (lockCurrent(st, lockList))
                return;
            if (btime - atime > timeOut) {
                throw new XAException("Proxy.getLockTimeout");
            }
        } while (true);

    }

    private void getSlock(Connection conn, Statement st, BaseResolvers resolver, String gloableXid, String branchXid)
            throws XAException, JSQLParserException, SQLException {

        long atime = System.currentTimeMillis();

        do {
            long btime = System.currentTimeMillis();
            List<TxcLock> lockList = getShareLock(gloableXid, branchXid, resolver, sql, conn, st);
            if (lockCurrent(st, lockList))
                return;
            if (btime - atime > timeOut) {
                throw new XAException("Proxy.getLockTimeout");
            }
        } while (true);

    }

    private boolean lockCurrent(Statement st, List<TxcLock> lockList) {
        if (lockList.size() > 0) {
            for (TxcLock lock : lockList) {
                try {
                    lock.lock(st);
                } catch (SQLException e) {
                    logger.info("getXlock -- Data locked by other,retry");
                    return false;
                }
            }
        }
        return true;
    }

    private List<TxcLock> getMutexLock(String gloableXid, String branchXid, BaseResolvers resolver, String sql,
                                       Connection conn, Statement st)
            throws XAException, JSQLParserException, SQLException {

        String beforeLockSql = resolver.getLockedSet();
        ResultSet result = st.executeQuery(beforeLockSql);
        String pk = resolver.getMetaPrimaryKey(conn, resolver.getTable());
        List<TxcLock> lockList = new ArrayList<>();
        List<String> R1list = new ArrayList<>();
        while (result.next()) {
            R1list.add(result.getObject(pk).toString());
        }
        if (!result.isClosed()) {
            result.close();
        }

        if (R1list.size() == 0) {
            return lockList;
        }

        String lockSql = "select key_value from txc_lock where xid='" + gloableXid + "'  and branch_id ='" + branchXid
                + "' and table_name = '" + resolver.getTable() + "'";
        if (R1list.size() > 0)
            lockSql += " and key_value in(" + resolver.transList(R1list) + ")";
        List<String> R2list = new ArrayList<>();
        ResultSet lockResult = st.executeQuery(lockSql);
        while (lockResult.next()) {
            R2list.add(lockResult.getString("key_value"));
        }
        if (!lockResult.isClosed()) {
            lockResult.close();
        }

        R1list.removeAll(R2list);
        for (String r3str : R1list) {

            MutexLock lock = new MutexLock();
            lock.setLock(Boolean.FALSE);
            lock.setXid(gloableXid);
            lock.setBranchId(branchXid);
            lock.setTableName(resolver.getTable());
            lock.setXlock("1");
            lock.setSlock(0);
            lock.setKeyValue(r3str);
            lock.setCreateTime(System.currentTimeMillis());
            lockList.add(lock);
        }

        return lockList;
    }

    private List<TxcLock> getShareLock(String gloableXid, String branchXid, BaseResolvers resolver, String sql,
                                       Connection conn, Statement st)
            throws XAException, JSQLParserException, SQLException {

        String beforeLockSql = resolver.getLockedSet();
        ResultSet result = st.executeQuery(beforeLockSql);
        String pk = resolver.getMetaPrimaryKey(conn, resolver.getTable());
        List<TxcLock> lockList = new ArrayList<>();
        List<String> R1list = new ArrayList<>();
        while (result.next()) {
            R1list.add(result.getObject(pk).toString());
        }
        if (!result.isClosed()) {
            result.close();
        }

        String lockSql =
                "select key_value,count(*) as count from txc_lock where xid='" + gloableXid + "'  and branch_id ='"
                        + branchXid + "' and table_name = '" + resolver.getTable() + "' and key_value in(" + resolver
                        .transList(R1list) + ") group by key_value";
        List<String> R2list = new ArrayList<>();
        ResultSet lockResult = st.executeQuery(lockSql);
        while (lockResult.next()) {
            if (lockResult.getInt("count") > 1)
                R2list.add(lockResult.getString("key_value"));
        }
        if (!lockResult.isClosed()) {
            lockResult.close();
        }

        R1list.removeAll(R2list);
        for (String r3str : R1list) {

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
     * 在开发过程，SQL语句有可能写错，如果能把运行时出错的 SQL 语句直接打印出来，那对排错非常方便，因为其可以直接拷贝到数据库客户端进行调试。
     *
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
        //        System.arraycopy(params, 0, values, 0, cols);
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
