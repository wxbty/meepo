package org.bytesoft.bytejta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.bytesoft.bytejta.image.BackInfo;
import org.bytesoft.bytejta.image.Image;

import javax.transaction.xa.XAException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public interface ImageResolvers {

    public Image genBeforeImage(BackInfo backInfo, String sql, Connection connection, Statement stmt) throws SQLException, JSQLParserException, XAException;

    public  Image genAfterImage(BackInfo backInfo, String sql, Connection connection, Statement stmt,Object pkVal) throws SQLException, XAException, JSQLParserException;
}
