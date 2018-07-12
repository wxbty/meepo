package org.bytesoft.bytejta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.image.Image;
import org.bytesoft.bytejta.image.LineFileds;
import org.bytesoft.bytejta.resource.XATerminatorImpl;
import org.bytesoft.common.utils.SqlpraserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class InsertImageResolvers extends BaseResolvers {

    static final Logger logger = LoggerFactory.getLogger(InsertImageResolvers.class);
    private Object pkVal;

    @Override
    public Image genBeforeImage(BackInfo backInfo, String insertSql, Connection conn, Statement stmt) {
        Image image = new Image();
        image.setSchemaName(schema);
        image.setTableName(tableName);
        image.setLine(new ArrayList<>());
        return image;
    }

    @Override
    public Image genAfterImage(BackInfo backInfo, String sql, Connection connection, Statement stmt, Object pkVal) throws XAException, SQLException, JSQLParserException {
        if (pkVal == null) {
            logger.error("Insert genAfterImage Can not be null!");
            throw new XAException("Insert.pkValNull");
        }

        this.pkVal = pkVal;
        return genImage(backInfo, sql, connection, stmt);
    }

    @Override
    protected String getTable(String sql) throws JSQLParserException {
        return SqlpraserUtils.name_insert_table(sql);
    }


    @Override
    protected String getSqlWhere(String sql) {
            return primaryKey + "= " + pkVal;
    }


    @Override
    protected List<String> getColumnList(String sql,Connection con, String tableName) throws JSQLParserException {
        return SqlpraserUtils.name_insert_column(sql);
    }
}
