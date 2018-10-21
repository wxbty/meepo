package org.feisoft.jta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.image.Image;
import org.feisoft.common.utils.SqlpraserUtils;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class UpdateImageResolvers extends BaseResolvers {

    UpdateImageResolvers(String orginSql, BackInfo backInfo)
    {
        this.orginSql =orginSql;
        this.backInfo = backInfo;
    }


    @Override
    public Image genBeforeImage() throws SQLException, JSQLParserException, XAException {
        return genImage();
    }

    @Override
    public Image genAfterImage() throws SQLException, XAException, JSQLParserException {
        return genImage();
    }

    @Override
    public String getTable() throws JSQLParserException, XAException {

        List<String> tables = SqlpraserUtils.name_update_table(orginSql);
        if (tables.size() > 1) {
            throw new XAException("Update.UnsupportMultiTables");
        }
        return tables.get(0);
    }

    @Override
    protected String getSqlWhere() throws JSQLParserException {
        return SqlpraserUtils.name_update_where(orginSql);
    }


    @Override
    protected List<String> getColumnList() throws JSQLParserException {
        return SqlpraserUtils.name_update_column(orginSql);
    }

    @Override
    public String getLockedSet() {

        return beforeImageSql;
    }
}
