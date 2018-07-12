package org.bytesoft.bytejta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.image.Image;
import org.bytesoft.bytejta.image.LineFileds;
import org.bytesoft.common.utils.SqlpraserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeleteImageResolvers extends BaseResolvers {

    static final Logger logger = LoggerFactory.getLogger(DeleteImageResolvers.class);
    private Object pkVal;

    @Override
    public Image genBeforeImage(BackInfo backInfo, String deleteSql, Connection conn, Statement stmt) throws JSQLParserException, SQLException, XAException {
        return genImage(backInfo, deleteSql, conn, stmt);

    }

    @Override
    public Image genAfterImage(BackInfo backInfo, String sql, Connection connection, Statement stmt, Object pkVal)  {
        Image image = new Image();
        image.setSchemaName(schema);
        image.setTableName(tableName);
        image.setLine(new ArrayList<>());
        return image;
    }

    @Override
    protected String getTable(String sql) throws JSQLParserException, XAException {

        List<String> tables = SqlpraserUtils.name_delete_table(sql);
        if (tables.size() > 1) {
            throw new XAException("Delete.UnsupportMultiTables");
        }
        return tables.get(0);
    }


    @Override
    protected String getSqlWhere(String sql) throws JSQLParserException {
        return SqlpraserUtils.name_delete_where(sql);
    }



}
