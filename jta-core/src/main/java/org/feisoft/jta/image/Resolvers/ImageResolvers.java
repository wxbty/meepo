package org.feisoft.jta.image.resolvers;

import net.sf.jsqlparser.JSQLParserException;
import org.feisoft.jta.image.Image;

import javax.transaction.xa.XAException;
import java.sql.SQLException;
import java.util.List;

public interface ImageResolvers {

    Image genBeforeImage() throws SQLException, JSQLParserException;

    Image genAfterImage() throws SQLException, JSQLParserException;

    String getTable() throws JSQLParserException, XAException, SQLException;

    String getPrimaryKey() throws SQLException;

    String getSqlWhere() throws JSQLParserException;

    List<String> getColumnList() throws JSQLParserException, SQLException;

    String getAllColumns();

    String getschema() throws SQLException;
}
