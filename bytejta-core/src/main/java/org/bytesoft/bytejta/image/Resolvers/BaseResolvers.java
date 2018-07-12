package org.bytesoft.bytejta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.image.Image;
import org.bytesoft.bytejta.image.LineFileds;
import org.bytesoft.common.utils.SqlpraserUtils;

import javax.transaction.xa.XAException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseResolvers implements ImageResolvers {

    protected String schema;
    protected String tableName;
    protected String primaryKey;
    protected String allColumns;
    protected String sqlWhere;
    protected List<String> column_list;

    protected Image genImage(BackInfo backInfo, String sql, Connection conn, Statement stmt) throws SQLException, JSQLParserException, XAException {

        tableName = getTable(sql);
        primaryKey = getPrimaryKey(conn, tableName);
        sqlWhere = getSqlWhere(sql);
        column_list = getColumnList(sql, conn, tableName);
        allColumns = getAllColumns();

        Image Image = new Image();
        List<Map<String, Object>> key_value_list = new ArrayList<Map<String, Object>>();
        StringBuffer sqlJoint = new StringBuffer("select ");
        sqlJoint.append(allColumns);
        sqlJoint.append(" from ");
        sqlJoint.append(tableName);
        if (StringUtils.isNotBlank(sqlWhere)) {
            sqlJoint.append(" where ");
            sqlJoint.append(sqlWhere);
        }
        String beforeImageSql = sqlJoint.toString();
        ResultSet rs = stmt.executeQuery(beforeImageSql);
        while (rs.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (String col : column_list) {
                map.put(col, rs.getObject(col));
            }
            map.put(primaryKey, rs.getObject(primaryKey));
            key_value_list.add(map);
        }
        List<LineFileds> line = new ArrayList<LineFileds>();

        for (Map<String, Object> peMap : key_value_list) {
            LineFileds lf = new LineFileds();
            List<org.bytesoft.bytejta.image.Field> fileds = new ArrayList<org.bytesoft.bytejta.image.Field>();
            StringBuffer backSql = new StringBuffer();
            for (String col : peMap.keySet()) {
                org.bytesoft.bytejta.image.Field field = new org.bytesoft.bytejta.image.Field();
                field.setName(col);
                field.setType(peMap.get(col).getClass().getName());
                field.setValue(peMap.get(col));
                fileds.add(field);
            }
            lf.setFields(fileds);
            line.add(lf);
        }
        Image.setLine(line);
        Image.setTableName(tableName);
        Image.setSchemaName(schema);

        backInfo.setSelectBody("select " + allColumns + " from " + tableName);
        backInfo.setSelectWhere(" where 1=1 and " + sqlWhere);
        backInfo.setChangeType(sql.trim().substring(0,6));
        backInfo.setChangeSql(sql);
        backInfo.setPk(primaryKey);

        return Image;
    }

    @Override
    public abstract Image genBeforeImage(BackInfo backInfo, String sql, Connection conn, Statement stmt) throws SQLException, JSQLParserException, XAException;

    @Override
    public abstract Image genAfterImage(BackInfo backInfo, String sql, Connection conn, Statement stmt, Object pkVal) throws SQLException, XAException, JSQLParserException;


    protected abstract String getTable(String sql) throws JSQLParserException, XAException;

    protected String getschema(Connection con, String tableName) throws SQLException {
        return getMetaSchema(con, tableName);
    }

    protected String getPrimaryKey(Connection con, String tableName) throws SQLException {
        return getMetaPrimaryKey(con, tableName);
    }

    protected String getAllColumns() {
        return  transList(column_list);
    }

    protected abstract String getSqlWhere(String sql) throws JSQLParserException;

    protected List<String> getColumnList(String sql, Connection con, String tableName) throws JSQLParserException, SQLException {
        Map<String, Object> metaColumn = getColumns(con, tableName);
        return new ArrayList<>(metaColumn.keySet());
    }

    ;

    public String getMetaPrimaryKey(Connection con, String tableName) throws SQLException {
        DatabaseMetaData dbMetaData = con.getMetaData();
        ResultSet primaryKeyResultSet = dbMetaData.getPrimaryKeys(con.getCatalog(), null, tableName);
        String primaryKeyColumnName = "";
        while (primaryKeyResultSet.next()) {
            primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
        }
        return primaryKeyColumnName;
    }

    public String getMetaSchema(Connection con, String tableName) throws SQLException {
        DatabaseMetaData dbMetaData = con.getMetaData();
        ResultSet schemasResultSet = dbMetaData.getSchemas(con.getCatalog(), null);
        String schemasName = "";
        while (schemasResultSet.next()) {
            schemasName = schemasResultSet.getString("TABLE_SCHEM");
        }
        return schemasName;
    }

    protected Map<String, Object> getColumns(Connection con, String tableName) throws SQLException {
        DatabaseMetaData dbMetaData = con.getMetaData();
        ResultSet columResultSet = dbMetaData.getColumns(null, "%", tableName, "%");
        Map<String, Object> colums = new HashMap<>();
        while (columResultSet.next()) {
            colums.put(columResultSet.getString("COLUMN_NAME"), columResultSet.getObject("TYPE_NAME"));
        }
        return colums;
    }

    private String transList(List<String> list) {
        StringBuffer buf = new StringBuffer();
        for (String str : list) {
            buf.append(str).append(",");
        }
        while (buf.charAt(buf.length() - 1) == ',') {
            buf.deleteCharAt(buf.length() - 1);
        }
        return buf.toString();
    }

}
