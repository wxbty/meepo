package org.bytesoft.bytejta.supports.jdbc;

import com.alibaba.fastjson.JSON;
import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.common.utils.SqlpraserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicPreparedStatementProxyHandler implements InvocationHandler {

     Logger logger = LoggerFactory.getLogger(DynamicPreparedStatementProxyHandler.class);

    private Object realObject;

    private String sql;

    private XAConnection xaConn;

    private Object[] params = new Object[2];

    public static ThreadLocal<BackInfo> backinfo = new ThreadLocal<BackInfo>(){
        public BackInfo initialValue(){
            return new BackInfo();
        }
    };


    public DynamicPreparedStatementProxyHandler(Object realObject, String sql, XAConnection conn) {
        this.realObject = realObject;
        this.sql = sql;
        this.xaConn = conn;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        if (method.getName().startsWith("set") && args != null && args.length == 2) {
            int seq = (int) args[0];
            params[seq - 1] = args[1];
        }
        //代理扩展逻辑
        if ("executeUpdate".equals(method.getName())) {

            sql = printRealSql(sql,params);
            CommonResourceDescriptor resource = (CommonResourceDescriptor) xaConn.getXAResource();
            Xid currentXid = TransactionImpl.currentXid.get();

            //事务数据源从对应数据库获取前置对象
            List<String> waitBackSqlList = new ArrayList<>();
            waitBackSqlList.add(sql);
            Connection conn = xaConn.getConnection();
            Statement st = conn.createStatement();

            BackInfo  backInfo = backinfo.get();
            backInfo.setBeforeImage(SqlpraserUtils.handleRollBack(backInfo,waitBackSqlList, conn, st));


            PreparedStatement ps = null;
            String GloableXid = SqlpraserUtils.partGloableXid(currentXid);
            String branchXid = SqlpraserUtils.partBranchXid(currentXid);
            String logSql = "INSERT INTO txc_undo_log (gmt_create,gmt_modified,xid,branch_id,rollback_info,status,server) VALUES(now(),now(),?,?,?,?,?)";
            ps = conn.prepareStatement(logSql);
            ps.setString(1, GloableXid);
            ps.setString(2, branchXid);
            ps.setInt(4, 0);
            ps.setString(5, "127.0.0.1");


            logger.info("proxy sql = " + sql);
            Object obj = method.invoke(realObject, args);

            //本地直接提交
            xaConn.getXAResource().end(currentXid, XAResource.TMSUCCESS);
            xaConn.getXAResource().prepare(currentXid);

            backInfo.setAfterImage(SqlpraserUtils.handleRollBack(backInfo, waitBackSqlList, conn, st));
            String backSqlJson = JSON.toJSONString(backInfo);
            ps.setString(3, backSqlJson);
            ps.executeUpdate();         //执行sql语句

            st.close();
            //事务数据源从对应数据库获取后置对象
            return obj;

        }

        return method.invoke(realObject, args);
    }


    /**
     * 在开发过程，SQL语句有可能写错，如果能把运行时出错的 SQL 语句直接打印出来，那对排错非常方便，因为其可以直接拷贝到数据库客户端进行调试。
     *
     * @param sql
     *            SQL 语句，可以带有 ? 的占位符
     * @param params
     *            插入到 SQL 中的参数，可单个可多个可不填
     * @return 实际 sql 语句
     */
    public static String printRealSql(String sql, Object[] params) {
        if(params == null || params.length == 0) {
            System.out.println("The SQL is------------>\n" + sql);
            return sql;
        }

        if (!match(sql, params)) {
            System.out.println("SQL 语句中的占位符与参数个数不匹配。SQL：" + sql);
            return null;
        }

        int cols = params.length;
        Object[] values = new Object[cols];
        System.arraycopy(params, 0, values, 0, cols);

        for (int i = 0; i < cols; i++) {
            Object value = values[i];
            if (value instanceof Date) {
                values[i] = "'" + value + "'";
            } else if (value instanceof String) {
                values[i] = "'" + value + "'";
            } else if (value instanceof Boolean) {
                values[i] = (Boolean) value ? 1 : 0;
            }
        }

        String statement = String.format(sql.replaceAll("\\?", "%s"), values);

        System.out.println("The SQL is------------>\n" + statement);

        return statement;
    }

    /**
     * ? 和参数的实际个数是否匹配
     *
     * @param sql
     *            SQL 语句，可以带有 ? 的占位符
     * @param params
     *            插入到 SQL 中的参数，可单个可多个可不填
     * @return true 表示为 ? 和参数的实际个数匹配
     */
    private static boolean match(String sql, Object[] params) {
        if(params == null || params.length == 0) return true; // 没有参数，完整输出

        Matcher m = Pattern.compile("(\\?)").matcher(sql);
        int count = 0;
        while (m.find()) {
            count++;
        }

        return count == params.length;
    }

}
