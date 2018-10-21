package org.feisoft.jta.image.Resolvers;

import org.feisoft.common.utils.SqlpraserUtils;
import org.feisoft.jta.image.BackInfo;

import javax.transaction.xa.XAException;

public class ImageUtil {


    public static BaseResolvers getImageResolvers(String sql, BackInfo backInfo) throws XAException {
        if (SqlpraserUtils.assertInsert(sql))
            return new InsertImageResolvers(sql,backInfo);
        else if (SqlpraserUtils.assertUpdate(sql))
        {
            return new UpdateImageResolvers(sql,backInfo);
        }else if (SqlpraserUtils.assertDelete(sql))
        {
            return new DeleteImageResolvers(sql,backInfo);
        }else if (SqlpraserUtils.assertSelect(sql))
        {
            return new SelectImageResolvers(sql,backInfo);
        }else
        {
            throw new XAException("ImageUtil.UnsupportSqlType");
        }
    };
}
