package org.bytesoft.bytejta.supports.jdbc;

import com.alibaba.fastjson.JSON;
import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.image.Resolvers.BaseResolvers;
import org.bytesoft.bytejta.image.Resolvers.ImageUtil;
import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.common.utils.SqlpraserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicPreparedStatementProxyHandler implements InvocationHandler {

    Logger logger = LoggerFactory.getLogger(DynamicPreparedStatementProxyHandler.class);

    private Object realObject;

    private String sql;

    private XAConnection xaConn;

    private Object[] params = new Object[2];

    public DynamicPreparedStatementProxyHandler(Object realObject, String sql, XAConnection conn) {
        this.realObject = realObject;
        this.sql = sql;
        this.xaConn = conn;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        if (method.getName().startsWith("set") && args != null && args.length == 2) {
            Integer seq = (Integer) args[0];
            params[seq - 1] = args[1];
        }
        if ("executeUpdate".equals(method.getName())) {
            BackInfo backInfo = new BackInfo();
            sql = printRealSql(sql, params);
            CommonResourceDescriptor resource = (CommonResourceDescriptor) xaConn.getXAResource();
            Xid currentXid = TransactionImpl.currentXid.get();
            //事务数据源从对应数据库获取前置对象
            Connection conn = xaConn.getConnection();
            Statement st = conn.createStatement();


            BaseResolvers resolver = ImageUtil.getImageResolvers(sql);
            backInfo.setBeforeImage(resolver.genBeforeImage(backInfo, sql, conn, st));
            PreparedStatement ps = null;
            String GloableXid = partGloableXid(currentXid);
            String branchXid = partBranchXid(currentXid);
            String logSql = "INSERT INTO txc_undo_log (gmt_create,gmt_modified,xid,branch_id,rollback_info,status,server) VALUES(now(),now(),?,?,?,?,?)";
            ps = conn.prepareStatement(logSql);
            ps.setString(1, GloableXid);
            ps.setString(2, branchXid);
            ps.setInt(4, 0);
            ps.setString(5, "127.0.0.1");

            logger.info("proxy sql = " + sql);
            Object obj = method.invoke(realObject, args);
            Object pkVal = null;
            if (sql.toLowerCase().startsWith("insert")) {
                PreparedStatement preparedStatement = (PreparedStatement) realObject;
                ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                while (generatedKeys.next()) {
                    pkVal = generatedKeys.getObject(1);
                }
                if (pkVal == null) {
                    String pkKey = resolver.getMetaPrimaryKey(conn, SqlpraserUtils.name_insert_table(sql));
                    List<String> colums = SqlpraserUtils.name_insert_column(sql);
                    List<String> values = SqlpraserUtils.name_insert_values(sql);
                    if (colums.contains(pkKey)) {
                        pkVal = values.get(colums.indexOf(pkKey));
                    }
                }

            }
            //本地直接提交
            xaConn.getXAResource().end(currentXid, XAResource.TMSUCCESS);
            xaConn.getXAResource().prepare(currentXid);
            //插入时需要获取主键的value

            backInfo.setAfterImage(resolver.genAfterImage(backInfo, sql, conn, st, pkVal));
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
     * @param sql    SQL 语句，可以带有 ? 的占位符
     * @param params 插入到 SQL 中的参数，可单个可多个可不填
     * @return 实际 sql 语句
     */
    private String printRealSql(String sql, Object[] params) {
        if (params == null || params.length == 0) {
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
     * @param sql    SQL 语句，可以带有 ? 的占位符
     * @param params 插入到 SQL 中的参数，可单个可多个可不填
     * @return true 表示为 ? 和参数的实际个数匹配
     */
    private boolean match(String sql, Object[] params) {
        if (params == null || params.length == 0) return true; // 没有参数，完整输出

        Matcher m = Pattern.compile("(\\?)").matcher(sql);
        int count = 0;
        while (m.find()) {
            count++;
        }

        return count == params.length;
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


    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static void appendAsHex(StringBuilder builder, byte[] bytes) {
        builder.append("0x");
        for (byte b : bytes) {
            builder.append(HEX_DIGITS[(b >>> 4) & 0xF]).append(HEX_DIGITS[b & 0xF]);
        }
    }


    private static void appendAsHex(StringBuilder builder, int value) {
        if (value == 0) {
            builder.append("0x0");
            return;
        }

        int shift = 32;
        byte nibble;
        boolean nonZeroFound = false;

        builder.append("0x");
        do {
            shift -= 4;
            nibble = (byte) ((value >>> shift) & 0xF);
            if (nonZeroFound) {
                builder.append(HEX_DIGITS[nibble]);
            } else if (nibble != 0) {
                builder.append(HEX_DIGITS[nibble]);
                nonZeroFound = true;
            }
        } while (shift != 0);
    }

}
