package org.feisoft.common.utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class SqlpraserUtils {


    static final Logger logger = LoggerFactory.getLogger(SqlpraserUtils.class);

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
        System.out.println("sql="+sql);
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

    public static List<String> name_select_table(String sql)
            throws JSQLParserException {
        Statement statement = (Statement) CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        List<String> tableList = tablesNamesFinder
                .getTableList(selectStatement);
        return tableList;
    }

    // *******select where
    public static String name_select_where(String sql)
            throws JSQLParserException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Select select = (Select) parserManager.parse(new StringReader(sql));
        PlainSelect plain = (PlainSelect) select.getSelectBody();
        Expression where_expression = plain.getWhere();
        if (where_expression == null)
            return "";
        String str = where_expression.toString();
        return str;
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
        if (where_expression == null)
            return "";
        String str = where_expression.toString();
        return str;
    }

    // *******update where
    public static String name_delete_where(String sql)
            throws JSQLParserException {
        net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(sql);
        Delete updateStatement = (Delete) statement;
        Expression where_expression = updateStatement.getWhere();
        if (where_expression == null)
            return "";
        String str = where_expression.toString();
        return str;
    }


    public static boolean assertInsert(String sql) {
        return sql.toLowerCase().trim().startsWith("insert");
    }

    public static boolean assertUpdate(String sql) {
        return sql.toLowerCase().trim().startsWith("update");
    }

    public static boolean assertDelete(String sql) {
        return sql.toLowerCase().trim().startsWith("delete");
    }

    public static boolean assertSelect(String sql) {
        return sql.toLowerCase().trim().startsWith("select");
    }


}
