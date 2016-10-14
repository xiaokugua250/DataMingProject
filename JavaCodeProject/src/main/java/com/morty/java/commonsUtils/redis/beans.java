package com.morty.java.commonsUtils.redis;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;

/**
 * Created by duliang on 2016/5/15.
 */
public class Beans<T extends Object> implements Serializable {
    private static final long serialVersionUID = -4459808615555206832L;
    private int               id;
    private String            name;
    private T                 key;
    private T                 value;

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(id).append(name).append(key).append(value).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE).append("ID", id)
                                                                        .append("NAME", name)
                                                                        .append("KEY", key)
                                                                        .append("VALUE", value)
                                                                        .toString();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public T getKey() {
        return key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
