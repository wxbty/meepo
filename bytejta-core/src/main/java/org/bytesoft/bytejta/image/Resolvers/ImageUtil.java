package org.bytesoft.bytejta.image.Resolvers;

import org.bytesoft.bytejta.image.BackInfo;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.Statement;

public class ImageUtil {


    public static BaseResolvers getImageResolvers(String sql, BackInfo backInfo, Connection conn, Statement st) throws XAException {
        if (sql.toLowerCase().trim().startsWith("insert"))
            return new InsertImageResolvers(sql,backInfo,conn,st);
        else if (sql.toLowerCase().trim().startsWith("update"))
        {
            return new UpdateImageResolvers(sql,backInfo,conn,st);
        }else if (sql.toLowerCase().trim().startsWith("delete"))
        {
            return new DeleteImageResolvers(sql,backInfo,conn,st);
        }else if (sql.toLowerCase().trim().startsWith("select"))
        {
            return new SelectImageResolvers(sql,backInfo,conn,st);
        }else
        {
            throw new XAException("Unsupport sql type");
        }
    };
}
