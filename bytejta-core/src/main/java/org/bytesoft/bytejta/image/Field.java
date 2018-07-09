package org.bytesoft.bytejta.image;

public class Field {

    private String name;
    private String type;
    private Object value;

    public Field(String name, String type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Object getValue() {
        return value;
    }

}
