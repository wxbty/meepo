package org.feisoft.jta.image;

import org.apache.commons.lang3.StringUtils;
import org.feisoft.common.utils.DbPool.DbPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import java.sql.Connection;
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
        return "BackInfo{" + "id=" + id + ", beforeImage=" + beforeImage + ", afterImage=" + afterImage
                + ", selectBody='" + selectBody + '\'' + ", selectWhere='" + selectWhere + '\'' + ", changeType='"
                + changeType + '\'' + ", changeSql='" + changeSql + '\'' + ", pk='" + pk + '\'' + '}';
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

    public void rollback() throws XAException, SQLException {
        if (validAfterImage()) {
            rollbackBeforeImage();
        } else {
            logger.error("Rollback unnecessary or failed,backinfp={}", toString());
        }
    }

    private boolean validAfterImage() throws XAException, SQLException {

        if (beforeImage == null || afterImage == null || StringUtils.isEmpty(pk) || StringUtils.isEmpty(changeSql)
                || StringUtils.isEmpty(changeType) || StringUtils.isEmpty(selectBody) || StringUtils
                .isEmpty(selectWhere)) {
            throw new XAException("validImageParameterNUll");
        }
        if (isDelete()) {
            Connection conn = DbPoolUtil.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(selectBody + selectWhere);
            boolean exist = !rs.next();
            DbPoolUtil.close(conn, null, rs, stmt);
            return exist;
        } else if (isUpdate() || isInsert()) {
            List<LineFileds> line = afterImage.getLine();
            //找出所有的后像记录，查看和当前记录是否一直，如果不一致，报错人工处理

            for (LineFileds lf : line) {
                List<org.feisoft.jta.image.Field> fields = lf.getFields();
                Map<String, Object> fds = new HashMap<String, Object>();
                for (org.feisoft.jta.image.Field field : fields) {
                    fds.put(field.getName(), field.getValue());
                }
                String andPkEquals = "";
                Object pkVal = fds.get(pk);
                if (pkVal == null) {
                    throw new XAException("ValidAfterImage.Unsupport null premaryKey");
                }
                if (pkVal instanceof String) {
                    andPkEquals = "  " + pk + "='" + pkVal + "'";
                } else {
                    andPkEquals = "  " + pk + "=" + pkVal;
                }
                String selectSql;
                if (getSelectWhere().toLowerCase().trim().endsWith("and"))
                    selectSql = getSelectBody() + getSelectWhere() + andPkEquals;
                else
                    selectSql = getSelectBody() + getSelectWhere() + " and " + andPkEquals;
                List<Boolean> result = DbPoolUtil.executeQuery(selectSql, rs -> {
                    boolean isSucess = true;
                    for (Map.Entry<String, Object> entry : fds.entrySet()) {
                        Object nowVal = rs.getObject(entry.getKey());
                        if (nowVal instanceof java.sql.Timestamp)
                            continue;
                        if (nowVal instanceof Integer && entry.getValue() instanceof Integer) {
                            isSucess = nowVal.equals(entry.getValue());
                            if (!isSucess)
                                break;
                        }
                        if (!nowVal.toString().equals(entry.getValue().toString())) {
                            logger.error(
                                    "Rollback sql error,because now resultData is not equals afterImage,change to manual operation!");
                            logger.error("nowVal=" + nowVal + ",entry.getValue()=" + entry.getValue());

                            isSucess = false;
                            break;
                        }
                    }
                    return isSucess;
                }, null);

                for (boolean sucess : result) {
                    if (!sucess)
                        return false;
                }

            }
            return true;
        }
        return false;
    }

    private void rollbackBeforeImage() throws XAException, SQLException {

        if (isInsert()) {
            String lastSql = "delete from " + afterImage.getTableName() + " " + selectWhere;
            DbPoolUtil.executeUpdate(lastSql);
        } else if (isUpdate()) {
            List<LineFileds> line = beforeImage.getLine();
            for (LineFileds lf : line) {
                List<org.feisoft.jta.image.Field> fields = lf.getFields();
                Map<String, Object> fds = new HashMap<String, Object>();
                StringBuffer setSql = new StringBuffer();
                for (org.feisoft.jta.image.Field field : fields) {
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
                DbPoolUtil.executeUpdate(lastSql);

            }
        } else if (isDelete()) {
            List<LineFileds> line = beforeImage.getLine();
            for (LineFileds lf : line) {
                List<org.feisoft.jta.image.Field> fields = lf.getFields();
                StringBuffer colSql = new StringBuffer();
                StringBuffer valSql = new StringBuffer();
                for (org.feisoft.jta.image.Field field : fields) {

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

                String lastSql =
                        "insert into " + beforeImage.getTableName() + " (" + colSql.toString() + ")values(" + valSql
                                .toString() + ")";
                DbPoolUtil.executeUpdate(lastSql);
            }
        }
    }

    public void updateStatusFinish() throws SQLException {
        String sql = "update txc_undo_log set status =1 where id = " + id;
        DbPoolUtil.executeUpdate(sql);

    }
}

