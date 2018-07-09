package org.bytesoft.bytejta.image;

public class BackInfo {

    private Image beforeImage;

    private Image afterImage;

    private String selectBody;

    private String selectWhere;

    private String changeType;

    private String changeSql;

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




}
