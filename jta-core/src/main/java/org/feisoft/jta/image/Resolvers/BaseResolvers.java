package org.feisoft.jta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.lang3.StringUtils;
import org.feisoft.common.utils.DbPool.DbPoolUtil;
import org.feisoft.common.utils.DbPool.RowMap;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.image.Image;
import org.feisoft.jta.image.LineFileds;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseResolvers implements ImageResolvers {

    protected BackInfo backInfo;

    protected String schema;

    protected String tableName;

    protected String primaryKey;

    protected String allColumns;

    protected String sqlWhere;

    protected List<String> column_list;

    protected String orginSql;

    protected String beforeImageSql;

    protected Image genImage() throws SQLException, JSQLParserException, XAException {

        tableName = getTable();
        primaryKey = getPrimaryKey();
        sqlWhere = getSqlWhere();
        column_list = getColumnList();
        allColumns = getAllColumns();
        schema = getMetaSchema();

        Image image = new Image();
        List<Map<String, Object>> key_value_list;

        getBeforeImageSql();

        key_value_list = DbPoolUtil.executeQuery(beforeImageSql, new RowMap<Map<String, Object>>() {

            @Override
            public Map<String, Object> rowMapping(ResultSet rs) {
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
            }

            @Override
            public boolean booleanMapping(ResultSet rs) {
                return false;
            }
        }, null);

        List<LineFileds> line = new ArrayList<LineFileds>();

        for (Map<String, Object> peMap : key_value_list) {
            LineFileds lf = new LineFileds();
            List<org.feisoft.jta.image.Field> fileds = new ArrayList<org.feisoft.jta.image.Field>();
            for (String col : peMap.keySet()) {
                org.feisoft.jta.image.Field field = new org.feisoft.jta.image.Field();
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

    @Override
    public abstract Image genBeforeImage() throws SQLException, JSQLParserException, XAException;

    @Override
    public abstract Image genAfterImage() throws SQLException, XAException, JSQLParserException;

    public abstract String getTable() throws JSQLParserException, XAException;

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

    protected String getschema() throws SQLException {
        return getMetaSchema();
    }

    protected String getPrimaryKey() throws SQLException {
        return getMetaPrimaryKey(tableName);
    }

    protected String getAllColumns() {
        return transList(column_list);
    }

    protected abstract String getSqlWhere() throws JSQLParserException;

    protected List<String> getColumnList() throws JSQLParserException, SQLException {
        Map<String, Object> metaColumn = getColumns();
        return new ArrayList<>(metaColumn.keySet());
    }

    ;

    public String getMetaPrimaryKey(String tableName) throws SQLException {
        Connection conn = DbPoolUtil.getConnection();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet primaryKeyResultSet = dbMetaData.getPrimaryKeys(conn.getCatalog(), null, tableName);
        String primaryKeyColumnName = "";
        while (primaryKeyResultSet.next()) {
            primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
        }
        DbPoolUtil.close(conn, null, primaryKeyResultSet);

        return primaryKeyColumnName;
    }

    public String getMetaSchema() throws SQLException {
        Connection conn = DbPoolUtil.getConnection();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet schemasResultSet = dbMetaData.getSchemas(conn.getCatalog(), null);
        String schemasName = "";
        while (schemasResultSet.next()) {
            schemasName = schemasResultSet.getString("TABLE_SCHEM");
        }
        DbPoolUtil.close(conn, null, schemasResultSet);
        return schemasName;
    }

    protected Map<String, Object> getColumns() throws SQLException {
        Connection conn = DbPoolUtil.getConnection();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet columResultSet = dbMetaData.getColumns(null, "%", tableName, "%");
        Map<String, Object> colums = new HashMap<>();
        while (columResultSet.next()) {
            colums.put(columResultSet.getString("COLUMN_NAME"), columResultSet.getObject("TYPE_NAME"));
        }
        DbPoolUtil.close(conn, null, columResultSet);
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

    abstract public String getLockedSet() throws JSQLParserException, SQLException, XAException;

}
