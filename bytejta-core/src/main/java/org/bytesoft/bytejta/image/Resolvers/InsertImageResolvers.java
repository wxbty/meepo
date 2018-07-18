package org.bytesoft.bytejta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.image.Image;
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

    public void setPkVal(Object pkVal) {
        this.pkVal = pkVal;
    }

    private Object pkVal;


    InsertImageResolvers(String orginSql, BackInfo backInfo, Connection conn, Statement stmt) {
        this.orginSql = orginSql;
        this.backInfo = backInfo;
        this.conn = conn;
        this.stmt = stmt;
    }

    @Override
    public Image genBeforeImage() {
        Image image = new Image();
        image.setSchemaName(schema);
        image.setTableName(tableName);
        image.setLine(new ArrayList<>());
        return image;
    }

    @Override
    public Image genAfterImage() throws XAException, SQLException, JSQLParserException {
        if (pkVal == null) {
            logger.error("Insert genAfterImage Can not be null!");
            throw new XAException("Insert.pkValNull");
        }
        return genImage();
    }

    @Override
    public String getTable() throws JSQLParserException {
        return SqlpraserUtils.name_insert_table(orginSql);
    }


    @Override
    protected String getSqlWhere() {
        return primaryKey + "= " + pkVal;
    }


    @Override
    public List<String> getColumnList() throws JSQLParserException {
        return SqlpraserUtils.name_insert_column(orginSql);
    }

    @Override
    public String getLockedSet() throws JSQLParserException, SQLException {
        List<String> insertCols = SqlpraserUtils.name_insert_column(orginSql);
        String pk = getMetaPrimaryKey(conn, getTable());
        StringBuffer sqlJoint = new StringBuffer("select ");
        sqlJoint.append(transList(insertCols));
        sqlJoint.append(" from ");
        sqlJoint.append(getTable());
        sqlJoint.append(" where  1=1 ");
        List<String> values = SqlpraserUtils.name_insert_values(orginSql);
        if (insertCols.contains(pk)) {

            pkVal = values.get(insertCols.indexOf(primaryKey));
            sqlJoint.append("and "+primaryKey + "=" + pkVal);
            return  sqlJoint.toString();
        }else
        {
            for (String col:insertCols)
            {
                sqlJoint.append("and "+col+"="+values.get(insertCols.indexOf(col)));
            }
            return  sqlJoint.toString();
        }
    }


}
