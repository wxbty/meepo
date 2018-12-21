package org.feisoft.jta.image.resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.image.Image;

import java.sql.SQLException;
import java.util.List;

public class SelectImageResolvers extends BaseResolvers {

    SelectImageResolvers(String orginSql, BackInfo backInfo) {
        this.orginSql = orginSql;
        this.backInfo = backInfo;
    }

    @Override
    public Image genBeforeImage() throws JSQLParserException, SQLException {
        return genImage();

    }

    @Override
    public Image genAfterImage() {
        return null;
    }

    @Override
    public String getTable() throws JSQLParserException, SQLException {

        List<String> tables = SqlpraserUtils.name_select_table(orginSql);
        if (tables.size() != 1) {
            throw new SQLException("Select.UnsupportMultiTables");
        }
        return tables.get(0).toUpperCase();
    }

    @Override
    public String getSqlWhere() throws JSQLParserException {
        return SqlpraserUtils.name_select_where(orginSql);
    }

    @Override
    public String getLockedSet() throws JSQLParserException {
        return beforeImageSql;
    }

}
