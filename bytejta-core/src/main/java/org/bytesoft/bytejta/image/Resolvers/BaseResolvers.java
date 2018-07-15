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

    protected BackInfo backInfo;
    protected Connection conn;
    protected Statement stmt;

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
        schema = getMetaSchema(conn,tableName);


        Image image = new Image();
        List<Map<String, Object>> key_value_list = new ArrayList<Map<String, Object>>();

        getBeforeImageSql();
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

    protected void getBeforeImageSql() throws SQLException {
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
        return getMetaSchema(conn, tableName);
    }

    protected String getPrimaryKey() throws SQLException {
        return getMetaPrimaryKey(conn, tableName);
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

    protected Map<String, Object> getColumns() throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        ResultSet columResultSet = dbMetaData.getColumns(null, "%", tableName, "%");
        Map<String, Object> colums = new HashMap<>();
        while (columResultSet.next()) {
            colums.put(columResultSet.getString("COLUMN_NAME"), columResultSet.getObject("TYPE_NAME"));
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

    abstract public String getLockedSet() throws JSQLParserException, SQLException, XAException;



}
