package org.feisoft.jta.image.resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.lang3.StringUtils;
import org.feisoft.common.utils.DbPool.DbPoolSource;
import org.feisoft.common.utils.SpringBeanUtil;
import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.image.Field;
import org.feisoft.jta.image.Image;
import org.feisoft.jta.image.LineFileds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NamedThreadLocal;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BaseResolvers implements ImageResolvers {

    static final Logger logger = LoggerFactory.getLogger(BaseResolvers.class);

    private DbPoolSource dbPoolSource;

    private static final ThreadLocal<Map<Object, Object>> resources = new NamedThreadLocal<>("Datasource meta");

    private static ConcurrentHashMap<String, String> tbNameToPks = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, Map<String, Object>> tbNameToCols = new ConcurrentHashMap<>();

    protected BackInfo backInfo;

    protected String schema;

    protected String tableName;

    protected String primaryKey;

    protected String allColumns;

    protected String sqlWhere;

    protected List<String> column_list;

    protected String orginSql;

    protected String beforeImageSql;

    BaseResolvers() {
        DbPoolSource dbPoolSource = (DbPoolSource) SpringBeanUtil.getBean("dbPoolSource");
        Assert.notNull(dbPoolSource, "No DataSource specified");
        this.dbPoolSource = dbPoolSource;
    }

    protected Image genImage() throws SQLException, JSQLParserException {

        tableName = getTable();
        primaryKey = getPrimaryKey();
        sqlWhere = getSqlWhere();
        column_list = getColumnList();
        allColumns = getAllColumns();
        schema = getschema();

        Image image = new Image();
        List<Map<String, Object>> key_value_list;

        getBeforeImageSql();

        key_value_list = dbPoolSource.executeQuery(beforeImageSql, rs -> {
            Map<String, Object> keyObjMap = new HashMap<>();
            try {
                for (String col : column_list) {
                    keyObjMap.put(col, rs.getObject(col));
                }
                keyObjMap.put(primaryKey, rs.getObject(primaryKey));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return keyObjMap;
        }, null);

        List<LineFileds> line = new ArrayList<LineFileds>();

        for (Map<String, Object> peMap : key_value_list) {
            LineFileds lf = new LineFileds();
            List<org.feisoft.jta.image.Field> fileds = new ArrayList<org.feisoft.jta.image.Field>();
            for (String col : peMap.keySet()) {
                Field field = new org.feisoft.jta.image.Field();
                field.setName(col);
                field.setType(peMap.get(col).getClass().getName());
                field.setValue(peMap.get(col));
                fileds.add(field);
            }
            lf.setFields(fileds);
            line.add(lf);
        }
        image.setLine(line);
        image.setTableName(tableName);
        image.setSchemaName(schema);

        backInfo.setSelectBody("select " + primaryKey + "," + allColumns + " from " + tableName);
        backInfo.setSelectWhere(" where 1=1 and " + sqlWhere);
        backInfo.setChangeType(orginSql.trim().substring(0, 6));
        backInfo.setChangeSql(orginSql);
        backInfo.setPk(primaryKey);

        return image;
    }

    public abstract String getTable() throws JSQLParserException, SQLException;

    protected void getBeforeImageSql() {
        StringBuffer sqlJoint = new StringBuffer("select ");
        sqlJoint.append(primaryKey + "," + allColumns);
        sqlJoint.append(" from ");
        sqlJoint.append(tableName);
        if (StringUtils.isNotBlank(sqlWhere)) {
            sqlJoint.append(" where ");
            sqlJoint.append(sqlWhere);
        }
        beforeImageSql = sqlJoint.toString();
    }

    public String getschema() throws SQLException {

        if (hasSchema(dbPoolSource.getDataSource())) {
            return doGetResource(dbPoolSource.getDataSource()).toString();
        }

        Connection conn = dbPoolSource.getConnection();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet schemasResultSet = dbMetaData.getSchemas(conn.getCatalog(), null);
        String schemasName = null;
        while (schemasResultSet.next()) {
            schemasName = schemasResultSet.getString("TABLE_SCHEM");
        }
        if (StringUtils.isNotBlank(schemasName)) {
            bindResource(dbPoolSource.getDataSource(), schemasName);
        }
        dbPoolSource.close(conn, null, schemasResultSet);
        return schemasName;
    }

    public String getPrimaryKey() throws SQLException {
        return getMetaPrimaryKey(tableName);
    }

    public String getAllColumns() {
        return transList(column_list);
    }

    public abstract String getSqlWhere() throws JSQLParserException;

    public List<String> getColumnList() throws JSQLParserException, SQLException {
        Map<String, Object> metaColumn = getColumns();
        return new ArrayList<>(metaColumn.keySet());
    }

    ;

    public String getMetaPrimaryKey(String tableName) throws SQLException {
        if (BaseResolvers.tbNameToPks.keySet().contains(tableName)) {
            return BaseResolvers.tbNameToPks.get(tableName);
        }
        Connection conn = dbPoolSource.getConnection();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet primaryKeyResultSet = dbMetaData.getPrimaryKeys(conn.getCatalog(), null, tableName);
        String primaryKeyColumnName = null;
        while (primaryKeyResultSet.next()) {
            primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
        }
        dbPoolSource.close(conn, null, primaryKeyResultSet);
        if (StringUtils.isNotBlank(primaryKeyColumnName)) {
            BaseResolvers.tbNameToPks.put(tableName, primaryKeyColumnName);
        }
        return primaryKeyColumnName;
    }

    protected Map<String, Object> getColumns() throws SQLException {
        if (tbNameToCols.keySet().contains(tableName)) {
            return tbNameToCols.get(tableName);
        }
        Connection conn = dbPoolSource.getConnection();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet columResultSet = dbMetaData.getColumns(null, "%", tableName, "%");
        Map<String, Object> colums = new HashMap<>();
        while (columResultSet.next()) {
            colums.put(columResultSet.getString("COLUMN_NAME"), columResultSet.getObject("TYPE_NAME"));
        }
        dbPoolSource.close(conn, null, columResultSet);
        if (colums.size() > 0) {
            tbNameToCols.put(tableName, colums);
        }
        return colums;
    }

    public String transList(List<String> list) {
        StringBuffer buf = new StringBuffer();
        for (String str : list) {
            buf.append(str).append(",");
        }
        while (buf.charAt(buf.length() - 1) == ',') {
            buf.deleteCharAt(buf.length() - 1);
        }
        return buf.toString();
    }

    abstract public String getLockedSet() throws JSQLParserException, SQLException;

    public static BaseResolvers newInstance(String sql, BackInfo backInfo) throws SQLException {
        if (SqlpraserUtils.assertInsert(sql))
            return new InsertImageResolvers(sql, backInfo);
        else if (SqlpraserUtils.assertUpdate(sql)) {
            return new UpdateImageResolvers(sql, backInfo);
        } else if (SqlpraserUtils.assertDelete(sql)) {
            return new DeleteImageResolvers(sql, backInfo);
        } else if (SqlpraserUtils.assertSelect(sql)) {
            return new SelectImageResolvers(sql, backInfo);
        } else {
            throw new SQLException("BaseResolvers.UnsupportSqlType");
        }
    }

    public void bindResource(Object actualKey, Object value) throws IllegalStateException {
        Assert.notNull(value, "Value must not be null");
        Map<Object, Object> map = resources.get();
        // set ThreadLocal Map if none found
        if (map == null) {
            map = new HashMap<>();
            resources.set(map);
        }
        Object oldValue = map.put(actualKey.toString(), value);
        // Transparently suppress a ResourceHolder that was marked as void...

        if (oldValue != null) {
            throw new IllegalStateException(
                    "Already value [" + oldValue + "] for key [" + actualKey + "] bound to thread [" + Thread
                            .currentThread().getName() + "]");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Bound value [" + value + "] for key [" + actualKey + "] to thread [" + Thread.currentThread()
                    .getName() + "]");
        }
    }

    public Object doGetResource(Object actualKey) {
        Map<Object, Object> map = resources.get();
        if (map == null) {
            return null;
        }
        return map.get(actualKey);
    }

    public boolean hasSchema(DataSource dataSource) {
        return doGetResource(dataSource) != null;
    }
}
