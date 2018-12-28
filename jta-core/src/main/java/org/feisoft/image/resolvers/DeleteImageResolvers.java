package org.feisoft.image.resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.image.Image;
import org.feisoft.image.BackInfo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DeleteImageResolvers extends BaseResolvers {

    DeleteImageResolvers(String orginSql, BackInfo backInfo) {
        this.orginSql = orginSql;
        this.backInfo = backInfo;
    }

    @Override
    public Image genBeforeImage() throws JSQLParserException, SQLException {
        return genImage();

    }

    @Override
    public Image genAfterImage() {
        Image image = new Image();
        image.setSchemaName(schema);
        image.setTableName(tableName);
        image.setLine(new ArrayList<>());
        return image;
    }

    @Override
    public String getTable() throws JSQLParserException, SQLException {

        List<String> tables = SqlpraserUtils.name_delete_table(orginSql);
        if (tables.size() != 1) {
            throw new SQLException("Delete.UnsupportMultiTables");
        }
        return tables.get(0).toUpperCase();
    }

    @Override
    public String getSqlWhere() throws JSQLParserException {
        return SqlpraserUtils.name_delete_where(orginSql);
    }

    @Override
    public String getLockedSet() {
        return beforeImageSql;
    }


}
