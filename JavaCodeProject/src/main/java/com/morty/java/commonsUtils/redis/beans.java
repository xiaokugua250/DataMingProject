package com.morty.java.commonsUtils.redis;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;

/**
 * Created by duliang on 2016/5/15.
 */
public class beans <T extends  Object> implements Serializable {


    private static final long serialVersionUID = -4459808615555206832L;

    private  int id;
    private String name;
    private  T key;
    private  T value;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getKey() {
        return key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("ID",id)
                .append("NAME",name)
                .append("KEY",key)
                .append("VALUE",value)
                .toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(id)
                .append(name)
                .append(key)
                .append(value)
                .toHashCode();
    }


}
