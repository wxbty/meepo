package org.feisoft.jta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.jta.image.BackInfo;
import org.feisoft.jta.image.Image;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import java.sql.SQLException;
import java.util.List;

public class ShareLockImageResolvers extends BaseResolvers {

    static final Logger logger = LoggerFactory.getLogger(ShareLockImageResolvers.class);

    ShareLockImageResolvers(String orginSql, BackInfo backInfo) {
        //SqlpraserUtils解析不了lock
        this.orginSql = orginSql.substring(0, orginSql.toLowerCase().indexOf("lock"));
        this.backInfo = backInfo;
    }

    @Override
    public Image genBeforeImage() throws JSQLParserException, SQLException, XAException {
        return genImage();

    }

    @Override
    public Image genAfterImage() {
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
    public String getSqlWhere() throws JSQLParserException {
        return SqlpraserUtils.name_select_where(orginSql);
    }

    @Override
    public String getLockedSet() throws JSQLParserException {
        return beforeImageSql;
    }

}
