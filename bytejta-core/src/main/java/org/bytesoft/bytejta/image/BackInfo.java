package org.bytesoft.bytejta.image;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BackInfo {

    static final Logger logger = LoggerFactory.getLogger(BackInfo.class);

    public Long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "BackInfo{" +
                "id=" + id +
                ", beforeImage=" + beforeImage +
                ", afterImage=" + afterImage +
                ", selectBody='" + selectBody + '\'' +
                ", selectWhere='" + selectWhere + '\'' +
                ", changeType='" + changeType + '\'' +
                ", changeSql='" + changeSql + '\'' +
                ", pk='" + pk + '\'' +
                '}';
    }

    public void setId(Long id) {
        this.id = id;
    }

    private Long id;

    private Image beforeImage;

    private Image afterImage;

    private String selectBody;

    private String selectWhere;

    private String changeType;

    private String changeSql;

    private String pk;

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public Image getBeforeImage() {
        return beforeImage;
    }

    public void setBeforeImage(Image beforeImage) {
        this.beforeImage = beforeImage;
    }

    public Image getAfterImage() {
        return afterImage;
    }

    public void setAfterImage(Image afterImage) {
        this.afterImage = afterImage;
    }

    public String getSelectBody() {
        return selectBody;
    }

    public void setSelectBody(String selectBody) {
        this.selectBody = selectBody;
    }

    public String getSelectWhere() {
        return selectWhere;
    }

    public void setSelectWhere(String selectWhere) {
        this.selectWhere = selectWhere;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
    }

    public String getChangeSql() {
        return changeSql;
    }

    public void setChangeSql(String changeSql) {
        this.changeSql = changeSql;
    }

    public boolean isInsert() {
        return "insert".equalsIgnoreCase(getChangeType());
    }

    public boolean isUpdate() {
        return "update".equalsIgnoreCase(getChangeType());
    }

    public boolean isDelete() {
        return "delete".equalsIgnoreCase(getChangeType());
    }

    public void rollback(Statement stmt) throws XAException, SQLException {
        if (validAfterImage(stmt)) {
            rollbackBeforeImage(stmt);
        }else
        {
            logger.error("Rollback unnecessary or failed");
            System.out.println("backinfo="+toString());
        }
    }



    private boolean validAfterImage(Statement stmt) throws XAException, SQLException {

        if (beforeImage == null || afterImage == null || StringUtils.isEmpty(pk) || StringUtils.isEmpty(changeSql) ||
                StringUtils.isEmpty(changeType) || StringUtils.isEmpty(selectBody) || StringUtils.isEmpty(selectWhere)) {
            throw new XAException("validImageParameterNUll");
        }
        if (isDelete()) {
            ResultSet rs = stmt.executeQuery(selectBody + selectWhere);
            return !rs.next();
        } else if (isUpdate() || isInsert()) {
            List<LineFileds> line = afterImage.getLine();
            //找出所有的后像记录，查看和当前记录是否一直，如果不一致，报错人工处理

            for (LineFileds lf : line) {
                List<org.bytesoft.bytejta.image.Field> fields = lf.getFields();
                Map<String, Object> fds = new HashMap<String, Object>();
                for (org.bytesoft.bytejta.image.Field field : fields) {
                    fds.put(field.getName(), field.getValue());
                }
                String andPkEquals = "";
                Object pkVal = fds.get(pk);
                if (pkVal == null) {
                    throw new XAException("ValidAfterImage.Unsupport null premaryKey");
                }
                if (pkVal instanceof String) {
                    andPkEquals = " and " + pk + "='" + pkVal + "'";
                } else {
                    andPkEquals = " and " + pk + "=" + pkVal;
                }

                String selectSql = getSelectBody() + getSelectWhere() + andPkEquals;
                ResultSet rs = stmt.executeQuery(selectSql);
                while (rs.next()) {
                    for (Map.Entry<String, Object> entry : fds.entrySet()) {
                        Object nowVal = rs.getObject(entry.getKey());
                        if (nowVal instanceof java.sql.Timestamp)
                            continue;
                        if (nowVal instanceof Integer && entry.getValue() instanceof Integer) {
                            return nowVal.equals(entry.getValue());
                        }
                        if (!nowVal.toString().equals(entry.getValue().toString())) {
                            logger.error("Rollback sql error,because now resultData is not equals afterImage,change to manual operation!");
                            System.out.println("Rollback sql error,because now resultData is not equals afterImage,change to manual operation!");
                            System.out.println("nowVal="+nowVal+",entry.getValue()="+entry.getValue());

                            return false;
                        }
                    }

                }

            }
            return true;
        }
        return false;
    }

    private void rollbackBeforeImage(Statement stmt) throws XAException, SQLException {

        if (isInsert()) {
            String lastSql = "delete from " + afterImage.getTableName() + " " + selectWhere;
            System.out.println("exe delete backInsert sql="+lastSql);
            stmt.execute(lastSql);
        } else if (isUpdate()) {
            List<LineFileds> line = beforeImage.getLine();
            for (LineFileds lf : line) {
                List<org.bytesoft.bytejta.image.Field> fields = lf.getFields();
                Map<String, Object> fds = new HashMap<String, Object>();
                StringBuffer setSql = new StringBuffer();
                for (org.bytesoft.bytejta.image.Field field : fields) {
                    fds.put(field.getName(), field.getValue());
                    if (field.getValue() instanceof String) {
                        setSql.append(" " + field.getName() + "='" + field.getValue() + "',");
                    } else {
                        setSql.append(" " + field.getName() + "=" + field.getValue() + ",");
                    }
                }
                while (setSql.charAt(setSql.length() - 1) == ',') {
                    setSql.deleteCharAt(setSql.length() - 1);
                }

                String whereSql = "";
                Object pkVal = fds.get(pk);
                if (pkVal == null) {
                    throw new XAException(XAException.XA_RBROLLBACK);
                }
                if (pkVal instanceof String) {
                    whereSql = " where " + pk + "='" + pkVal + "'";
                } else {
                    whereSql = " where " + pk + "=" + pkVal;
                }

                String lastSql = "update " + beforeImage.getTableName() + " set " + setSql + whereSql;
                stmt.execute(lastSql);

            }
        } else if (isDelete()) {
            List<LineFileds> line = beforeImage.getLine();
            for (LineFileds lf : line) {
                List<org.bytesoft.bytejta.image.Field> fields = lf.getFields();
                StringBuffer colSql = new StringBuffer();
                StringBuffer valSql = new StringBuffer();
                for (org.bytesoft.bytejta.image.Field field : fields) {

                    colSql.append(" " + field.getName() + ",");

                    if (field.getValue() instanceof String) {
                        valSql.append(" '" + field.getValue() + "',");
                    } else {
                        valSql.append(" " + field.getValue() + ",");
                    }
                }
                while (colSql.charAt(colSql.length() - 1) == ',') {
                    colSql.deleteCharAt(colSql.length() - 1);
                }
                while (valSql.charAt(valSql.length() - 1) == ',') {
                    valSql.deleteCharAt(valSql.length() - 1);
                }


                String lastSql = "insert into " + beforeImage.getTableName() + " (" + colSql.toString() + ")values(" + valSql.toString() + ")";
                stmt.execute(lastSql);
            }
        }
    }

    public void updateStatusFinish(Statement stmt) {
        String sql = "update txc_undo_log set status =1 where id = "+id;
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            logger.error("update backinfo status failed",e);
            e.printStackTrace();
        }
    }
}
