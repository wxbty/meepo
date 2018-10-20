package org.feisoft.jta.image.Resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.feisoft.jta.image.Image;

import javax.transaction.xa.XAException;
import java.sql.SQLException;

public interface ImageResolvers {

    Image genBeforeImage() throws SQLException, JSQLParserException, XAException;

    Image genAfterImage() throws SQLException, XAException, JSQLParserException;
}
