package org.feisoft.image.resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.image.Image;
import org.feisoft.image.BackInfo;

import java.sql.SQLException;
import java.util.List;

public class UpdateImageResolvers extends BaseResolvers {

    UpdateImageResolvers(String orginSql, BackInfo backInfo)
    {
        this.orginSql =orginSql;
        this.backInfo = backInfo;
    }


    @Override
    public Image genBeforeImage() throws SQLException, JSQLParserException {
        return genImage();
    }

    @Override
    public Image genAfterImage() throws SQLException, JSQLParserException {
        return genImage();
    }

    @Override
    public String getTable() throws JSQLParserException, SQLException {

        List<String> tables = SqlpraserUtils.name_update_table(orginSql);
        if (tables.size() != 1) {
            throw new SQLException("Update.UnsupportMultiTables");
        }
        return tables.get(0).toUpperCase();
    }

    @Override
    public String getSqlWhere() throws JSQLParserException {
        return SqlpraserUtils.name_update_where(orginSql);
    }


    @Override
    public List<String> getColumnList() throws JSQLParserException {
        return SqlpraserUtils.name_update_column(orginSql);
    }

    @Override
    public String getLockedSet() {

        return beforeImageSql;
    }

    public static void main(String[] args) {
        String sql = "UPDATE video SET vaalidate=0 WHERE id=8002";
        UpdateImageResolvers resolvers = new UpdateImageResolvers(sql,null);
        try {
            System.out.printf(resolvers.getSqlWhere());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

    }
}
