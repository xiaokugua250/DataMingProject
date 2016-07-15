package com.morty.java.dmp.dmpConfig;

/**
 * Created by morty on 2016/07/15.
 */
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//
public class Namespace {
    public static final String CONF_ROOT = "/konzig";
    public static final String CONF_PARAM = "parameters";
    public static final String CONF_SERV = "services";
    public static final String PARAM_SERV_URL = "serv.url";
    public static final String SERV_PROP_START_TIME = "startTime";
    private static final String CONF_PATH = "/konzig/%s/%s/%s";

    public Namespace() {
    }

    public static String getParamConfPath(String org, String app) {
        return String.format("/konzig/%s/%s/%s", new Object[]{org, app, "parameters"});
    }

    public static String getServConfPath(String org, String app, String servName) {
        return String.format("/konzig/%s/%s/%s/%s", new Object[]{org, app, "services", servName});
    }
}

