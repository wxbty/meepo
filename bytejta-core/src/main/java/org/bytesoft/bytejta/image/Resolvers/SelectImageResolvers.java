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
import java.util.List;

public class SelectImageResolvers extends BaseResolvers {

    static final Logger logger = LoggerFactory.getLogger(SelectImageResolvers.class);


    SelectImageResolvers(String orginSql, BackInfo backInfo, Connection conn, Statement st)
    {
        this.orginSql =orginSql;
        this.backInfo = backInfo;
        this.conn = conn;
        this.stmt = st;
    }

    @Override
    public Image genBeforeImage() throws JSQLParserException, SQLException, XAException {
        return genImage();

    }

    @Override
    public Image genAfterImage()  {
       return null;
    }

    @Override
    public String getTable() throws JSQLParserException, XAException {

        List<String> tables = SqlpraserUtils.name_select_table(orginSql);
        if (tables.size() > 1) {
            throw new XAException("Select.UnsupportMultiTables");
        }
        return tables.get(0);
    }


    @Override
    protected String getSqlWhere() throws JSQLParserException {
        return SqlpraserUtils.name_select_where(orginSql);
    }

    @Override
    public String getLockedSet() throws JSQLParserException {
        return beforeImageSql;
    }


}
