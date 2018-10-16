package org.feisoft.jta.image.Resolvers;

import org.feisoft.jta.image.BackInfo;
import org.feisoft.common.utils.SqlpraserUtils;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.Statement;

public class ImageUtil {


    public static BaseResolvers getImageResolvers(String sql, BackInfo backInfo, Connection conn, Statement st) throws XAException {
        if (SqlpraserUtils.assertInsert(sql))
            return new InsertImageResolvers(sql,backInfo,conn,st);
        else if (SqlpraserUtils.assertUpdate(sql))
        {
            return new UpdateImageResolvers(sql,backInfo,conn,st);
        }else if (SqlpraserUtils.assertDelete(sql))
        {
            return new DeleteImageResolvers(sql,backInfo,conn,st);
        }else if (SqlpraserUtils.assertSelect(sql))
        {
            return new SelectImageResolvers(sql,backInfo,conn,st);
        }else
        {
            throw new XAException("ImageUtil.UnsupportSqlType");
        }
    };
}
