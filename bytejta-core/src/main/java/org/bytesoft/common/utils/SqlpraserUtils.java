/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlpraserUtils {

	public static String handleRollBack(List<String> list, Connection connection, Statement stmt,String identifier) {
		List<String> backSql = new ArrayList<String>();
		for (String rollsql : list) {
			if (rollsql.startsWith("insert")) {
				backSql.addAll(handleInsert(rollsql));
			} else if (rollsql.startsWith("update")) {
				backSql.addAll(handleUpdate(rollsql, stmt, connection,identifier));
			} else if (rollsql.startsWith("delete")) {
				backSql.addAll(handleDelete(rollsql, connection));
			}
		}
		String backSqlJson = JSON.toJSONString(encode(backSql));
		return backSqlJson;

	}

	static final Logger logger = LoggerFactory.getLogger(SqlpraserUtils.class);
	public static List<String> handleInsert(String insertSql) {
		//todo 此处会有bug，改成有主键，按主键回滚，没有主键，放弃异步回滚
		try {
			String table = name_insert_table(insertSql);
			net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(insertSql);
			Insert insertStatement = (Insert) statement;
			StringBuilder whereSql = new StringBuilder();
			for (int i = 0; i < insertStatement.getColumns().size(); i++) {
				whereSql.append(" and " + insertStatement.getColumns().get(i).getColumnName() + "=" + ((ExpressionList) insertStatement.getItemsList()).getExpressions().get(i).toString());
			}
			final String backSql = "delete from " + table + " where 1=1 " + whereSql.toString();
			return new ArrayList<String>() {{
				add(backSql);
			}};
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();

	}

	public static List<String> handleUpdate(String updateSql, Statement stmt, Connection conn,String identifier) {

		List<String> updateBackSql = new ArrayList<String>();
		List<Map<String, Object>> key_value_list = new ArrayList<Map<String, Object>>();
		try {

			List<String> column_list = name_update_column(updateSql);
			String columns = transList(column_list);
			List<String> tables = name_update_table(updateSql);
			if (tables.size() > 1) {
				logger.error("Unsupport multi tables for update");
				return null;
			}
			String pkey = getPrimaryKey(conn, tables.get(0),identifier)[0];
			columns = pkey + "," + columns;
			String table = tables.get(0);
			String whereSql = name_update_where(updateSql);
			String selectSql = "select " + columns + " from " + table + " where " + whereSql;
			logger.info(String.format("Save old data for update,data query sql =%s", selectSql));
			ResultSet rs = stmt.executeQuery(selectSql);
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (String col : column_list) {
					map.put(col, rs.getObject(col));
				}
				map.put(pkey, rs.getObject(pkey));
				key_value_list.add(map);
			}

			for (Map<String, Object> peMap : key_value_list) {
				StringBuffer backSql = new StringBuffer();
				backSql.append("update ");
				backSql.append(table);
				backSql.append(" set ");
				for (String col : peMap.keySet()) {
					if (!col.equals(pkey)) {
						backSql.append(col + " = ");
						Object obj = peMap.get(col);
						if (obj instanceof String) {
							backSql.append("'" + peMap.get(col) + "',");
						} else {
							backSql.append(peMap.get(col) + ",");
						}
					}
				}
				while (backSql.charAt(backSql.length() - 1) == ',') {
					backSql.deleteCharAt(backSql.length() - 1);
				}
				backSql.append(" where ");
				Object obj = peMap.get(pkey);
				if (obj instanceof String) {
					backSql.append(pkey + "= '" + peMap.get(pkey)+"'");
				} else {
					backSql.append(pkey + "=" + peMap.get(pkey));
				}
				updateBackSql.add(backSql.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return updateBackSql;

	}

	public static List<String> handleDelete(String deleteSql, Connection conn) {
		List<String> deleteBackList = new ArrayList<String>();
		List<Map<String, Object>> key_value_list = new ArrayList<Map<String, Object>>();
		try {
			List<String> column_list = new ArrayList<String>();
			List<String> tables = name_delete_table(deleteSql);
			if (tables.size() > 1) {
				throw new IllegalArgumentException("Unsupport multi tables for delete");
			}
			String table = tables.get(0);
			String whereSql = name_delete_where(deleteSql);
			String selectSql = "select * from " + table + " where " + whereSql;
			logger.info(String.format("Save old data for update,data query sql =%s", selectSql));
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(selectSql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int count = rsmd.getColumnCount();
			String[] name = new String[count];
			for (int i = 0; i < count; i++) {
				column_list.add(rsmd.getColumnName(i + 1));
			}
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (String col : column_list) {
					map.put(col, rs.getObject(col));
				}
				key_value_list.add(map);
			}

			for (Map<String, Object> peMap : key_value_list) {
				StringBuffer backSql = new StringBuffer();
				backSql.append("insert into ");
				backSql.append(table);
				backSql.append(" ( ");
				backSql.append(transList(column_list) + " )values(");
				for (String col : column_list) {
					Object obj = peMap.get(col);
					if (obj instanceof String) {
						backSql.append("'" + obj + "',");
					} else {
						backSql.append(obj + ",");
					}
				}
				while (backSql.charAt(backSql.length() - 1) == ',') {
					backSql.deleteCharAt(backSql.length() - 1);
				}
				backSql.append(" ) ");
				deleteBackList.add(backSql.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return deleteBackList;
	}


	public static String[] getPrimaryKey(Connection con, String tableName,String identifier) throws Exception
	{
		DatabaseMetaData dbMetaData=con.getMetaData();
		ResultSet primaryKeyResultSet = dbMetaData.getPrimaryKeys(con.getCatalog(),null,tableName);
		String primaryKeyColumnName = "";
		while (primaryKeyResultSet.next())  {
			primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
		}
		return  new String[]{primaryKeyColumnName};
	}

	public static List<String> encode(List<String> list) {
		//base64加密
		List<String> code_list = new ArrayList<String>();
		for (String str : list) {
			code_list.add(Base64Util.encode(str.getBytes()));
		}
		return code_list;
	}

	public static List<String> decode(List<String> list) {
		//base64加密
		List<String> code_list = new ArrayList<String>();
		for (String str : list) {
			try {
				code_list.add(new String(Base64Util.decode(str)));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return code_list;
	}


	// ****insert table
	public static String name_insert_table(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Insert insertStatement = (Insert) statement;
		String string_tablename = insertStatement.getTable().getName();
		return string_tablename;
	}

	// ********* insert table column
	public static List<String> name_insert_column(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Insert insertStatement = (Insert) statement;
		List<Column> table_column = insertStatement.getColumns();
		List<String> str_column = new ArrayList<String>();
		for (int i = 0; i < table_column.size(); i++) {
			str_column.add(table_column.get(i).toString());
		}
		return str_column;
	}


	// ********* Insert values ExpressionList
	public static List<String> name_insert_values(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Insert insertStatement = (Insert) statement;
		List<Expression> insert_values_expression = ((ExpressionList) insertStatement
				.getItemsList()).getExpressions();
		List<String> str_values = new ArrayList<String>();
		for (int i = 0; i < insert_values_expression.size(); i++) {
			str_values.add(insert_values_expression.get(i).toString());
		}
		return str_values;
	}

	// *********update table name
	public static List<String> name_update_table(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Update updateStatement = (Update) statement;
		List<Table> update_table = updateStatement.getTables();
		List<String> str_table = new ArrayList<String>();
		if (update_table != null) {
			for (int i = 0; i < update_table.size(); i++) {
				str_table.add(update_table.get(i).toString());
			}
		}
		return str_table;

	}

	public static List<String> name_delete_table(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Delete updateStatement = (Delete) statement;
		Table update_table = updateStatement.getTable();
		List<String> str_table = new ArrayList<String>();
		if (update_table != null) {
			str_table.add(update_table.toString());
		}
		return str_table;

	}

	// *********update column
	public static List<String> name_update_column(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Update updateStatement = (Update) statement;
		List<Column> update_column = updateStatement.getColumns();
		List<String> str_column = new ArrayList<String>();
		if (update_column != null) {
			for (int i = 0; i < update_column.size(); i++) {
				str_column.add(update_column.get(i).toString());
			}
		}
		return str_column;

	}


	// *******update where
	public static String name_update_where(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Update updateStatement = (Update) statement;
		Expression where_expression = updateStatement.getWhere();
		String str = where_expression.toString();
		return str;
	}

	// *******update where
	public static String name_delete_where(String sql)
			throws JSQLParserException {
		net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
		Delete updateStatement = (Delete) statement;
		Expression where_expression = updateStatement.getWhere();
		String str = where_expression.toString();
		return str;
	}

	public static String transList(List<String> list) {
		StringBuffer buf = new StringBuffer();
		for (String str : list) {
			buf.append(str).append(",");
		}
		while (buf.charAt(buf.length() - 1) == ',') {
			buf.deleteCharAt(buf.length() - 1);
		}
		return buf.toString();
	}


	public static List<String> decodeRollBackSql(List<String> list) {
		List<String> backSql = new ArrayList<String>();
		for (String rollsql : list) {
			System.out.println("begin decodeRollBackSql rollsql");
			List<String> tmpsql = new ArrayList<String>();
			tmpsql = JSONObject.parseArray(rollsql, String.class);
			backSql.addAll(tmpsql);
		}
		backSql = decode(backSql);
		return backSql;

	}


	public static boolean rollback(List<String> list, Connection connection, Statement stmt) {
		for (String rollsql : list) {
			try {
				System.out.println("exec back sql=" + rollsql);
				stmt.execute(rollsql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	public static String partGloableXid(Xid xid) {

		byte[] gtrid = xid.getGlobalTransactionId();

		StringBuilder builder = new StringBuilder();

		if (gtrid != null) {
			appendAsHex(builder, gtrid);
		}

		return builder.toString();
	}

	public static String partBranchXid(Xid xid) {

		byte[] btrid = xid.getBranchQualifier();

		StringBuilder builder = new StringBuilder();

		if (btrid != null) {
			appendAsHex(builder, btrid);
		}

		return builder.toString();
	}


	private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

	public static void appendAsHex(StringBuilder builder, byte[] bytes) {
		builder.append("0x");
		for (byte b : bytes) {
			builder.append(HEX_DIGITS[(b >>> 4) & 0xF]).append(HEX_DIGITS[b & 0xF]);
		}
	}


	public static void appendAsHex(StringBuilder builder, int value) {
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
