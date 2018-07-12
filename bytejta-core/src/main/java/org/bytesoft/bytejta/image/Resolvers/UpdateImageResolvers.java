package org.bytesoft.bytejta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.image.Image;
import org.bytesoft.common.utils.SqlpraserUtils;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class UpdateImageResolvers extends BaseResolvers {


    @Override
    public Image genBeforeImage(BackInfo backInfo, String sql, Connection conn, Statement stmt) throws SQLException, JSQLParserException, XAException {
        return genImage(backInfo,sql,conn,stmt);
    }

    @Override
    public Image genAfterImage(BackInfo backInfo, String sql, Connection conn, Statement stmt, Object pkVal) throws SQLException, XAException, JSQLParserException {
        return genImage(backInfo,sql,conn,stmt);
    }

    @Override
    protected String getTable(String sql) throws JSQLParserException, XAException {

        List<String> tables = SqlpraserUtils.name_update_table(sql);
        if (tables.size() > 1) {
            throw new XAException("Update.UnsupportMultiTables");
        }
        return tables.get(0);
    }

    @Override
    protected String getSqlWhere(String sql) throws JSQLParserException {
        return SqlpraserUtils.name_update_where(sql);
    }


    @Override
    protected List<String> getColumnList(String sql,Connection con, String tableName) throws JSQLParserException {
        return SqlpraserUtils.name_update_column(sql);
    }
}
