package org.bytesoft.bytejta.image.Resolvers;

import javax.transaction.xa.XAException;

public class ImageUtil {


    public static BaseResolvers getImageResolvers(String sql) throws XAException {
        if (sql.toLowerCase().trim().startsWith("insert"))
            return new InsertImageResolvers();
        else if (sql.toLowerCase().trim().startsWith("update"))
        {
            return new UpdateImageResolvers();
        }else if (sql.toLowerCase().trim().startsWith("delete"))
        {
            return new DeleteImageResolvers();
        }else
        {
            throw new XAException("Unsupport sql type");
        }
    };
}
