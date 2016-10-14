package com.morty.java.beans;

import java.io.Serializable;

/**
 * Created by morty on 2016/05/17.
 */
public class User implements Serializable {
    private static final long serialVersionUID = -407588917216956565L;
    private int               id;
    private String            name;
    private int               age;
    private Double            income;
    private String            describe;
    private String            province;
    private String            city;
    private String            street;
    private Long              postCode;
    private String            job;
    private String            shcool;
    private String            qqCount;
    private String            webChat;
    private String            email;
    private String            company;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getDescribe() {
        return describe;
    }

    public void setDescribe(String describe) {
        this.describe = describe;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Double getIncome() {
        return income;
    }

    public void setIncome(Double income) {
        this.income = income;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getPostCode() {
        return postCode;
    }

    public void setPostCode(Long postCode) {
        this.postCode = postCode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getQqCount() {
        return qqCount;
    }

    public void setQqCount(String qqCount) {
        this.qqCount = qqCount;
    }

    public String getShcool() {
        return shcool;
    }

    public void setShcool(String shcool) {
        this.shcool = shcool;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getWebChat() {
        return webChat;
    }

    public void setWebChat(String webChat) {
        this.webChat = webChat;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
