package com.morty.java.dmp.dmpConfig;

/**
 * Created by morty on 2016/07/15.
 */

import com.google.gson.Gson;

import java.text.SimpleDateFormat;

/*
 * 封装所有的常量
 * @author phovan
 */
public class DmpConsts {

    // ---------------------------------- 可在ZK或者dmp-config.xml文件配置 ----------------------------------
    /**
     * 可在zk中配置参数的组织名
     */
    public static final String ORG = "midea";
    /**
     * 可在zk中配置参数的项目名，隶属于某个组织
     */
    public static final String APP = "dmp";
    /**
     * 可在zk中配置参数：DMP使用到的hive的连接方式
     * 示例：jdbc:hive2://host:port/database_name|username
     */
    public static final String STR_HIVE_DMP_URL = "hive.dmp.url";
    /**
     * 可在zk中配置参数：DMP使用到的hive的数据在HDFS里面的路径，具体到库名
     * 示例：/user/hive/warehouse/database_name
     */
    public static final String STR_HIVE_DMP_DATA_LOCATION = "hive.dmp.data.location";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase的MR程序，设置reduce的个数
     */
    public static final String STR_REDUCE_TASKS_NUM_USERINFO = "reduce.tasks.num.userinfo";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase的MR程序，设置reduce的个数
     */
    public static final String STR_REDUCE_TASKS_NUM_RECOMMEND = "reduce.tasks.num.recommend";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase，检测一个用户对应的所有info记录数累加不能超过该值
     */
    public static final String STR_USER_MAX_INFO = "user.max.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase，检测一个用户对应的buyernickInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_BUYERNICK_INFO = "user.max.buyernick.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的addrInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_ADDR_INFOS = "user.max.addr.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的qqInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_QQ_INFOS = "user.max.qq.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的wechatInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_WECHAT_INFOS = "user.max.wechat.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的emailInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_EMAIL_INFOS = "user.max.email.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的payInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_PAY_INFOS = "user.max.pay.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的microBlogInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_MICRO_BLOG_INFOS = "user.max.microblog.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的websiteInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_WEBSITE_INFOS = "user.max.website.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的relationInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_RELATION_INFOS = "user.max.relation.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的labelInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_LABEL_INFOS = "user.max.label.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的memInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_MEM_INFOS = "user.max.mem.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的phoneInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_PHONE_INFOS = "user.max.phone.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的baseInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_BASE_INFOS = "user.max.base.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的productInfo记录数不能超过该值
     */
    public static final String STR_USER_MAX_PRODUCT_INFOS = "user.max.product.infos";
    /**
     * 可在zk中配置参数：从HDFS写数据到HBase时，检测一个用户对应的recommend记录数不能超过该值
     */
    public static final String STR_USER_MAX_RECOMMEND_INFOS = "user.max.recommend.infos";

    // ---------------------------------- 程序内部配置相关常量 ----------------------------------
    /**
     * 用于在程序内部配置configuration
     */
    public static final String STR_USERINFO_QUALIFIER_NAME = "userinfo.qualifier.name";
    /**
     * 用于在程序内部配置configuration
     */
    public static final String STR_USERINFO_HTABLE_NAME = "userinfo.htable.name";
    /**
     * 用于在程序内部配置configuration
     */
    public static final String STR_USERPRODUCT_HTABLE_NAME = "userproduct.htable.name";
    /**
     * 用于在程序内部配置configuration
     */
    public static final String STR_USERINFO_IS_GET_FIRST = "userinfo.is.get.first";
    /**
     * 用于在程序内部配置configuration
     */
    public static final String STR_DW_TABLE_NAME = "dw.table.name";

    // ---------------------------------- 时间相关常量 ----------------------------------
    /**
     * 时间规整化常量，yyyy-MM-dd HH:mm:ss
     */
    public static final SimpleDateFormat SF_YYYY_MM_DD_HH_MM_SS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /**
     * 时间规整化常量，yyyy-MM-dd
     */
    public static final SimpleDateFormat SF_YYYY_MM_DD = new SimpleDateFormat("yyyy-MM-dd");
    /**
     * 时间规整化常量，yyyyMMdd
     */
    public static final SimpleDateFormat SF_YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

    /**
     * GSON常量
     */
    public static final Gson GSON = new Gson();

    // ---------------------------------- 分隔符常量 ----------------------------------
    /**
     * 分隔符常量，与hive数据有关
     */
    public static final String SEPARATOR_HIVE_DATA = String.valueOf('\036');
    /**
     * 分隔符常量，逗号
     */
    public static final String SEPARATOR_COMMA = String.valueOf(',');
    /**
     * 分隔符常量，制表符
     */
    public static final String SEPARATOR_TABLE = String.valueOf('\t');
    /**
     * 分隔符常量，从hive导出到HDFS上的数据的分隔符
     */
    public static final String SEPARATOR_HIVE_HDFS_DATA = "\001";
    /**
     * 分隔符常量
     */
    public static final String SEPARATOR_HDFS_ES_KAY = "\002";
    /**
     * 分隔符常量
     */
    public static final String SEPARATOR_VERTICAL_LINE = "\\|";
    /**
     * 分隔符常量
     */
    public static final String SEPARATOR_DOLLAR = "\\$";

    // ---------------------------------- 其他常量 ----------------------------------
    /**
     * 空值常量，与hive数据有关
     */
    public static final String HIVE_TO_HDFS_NULL_VALUE = "\\N";
    /**
     * 简单常量，null
     */
    public static final String STR_NULL = "null";
    /**
     * 简单常量，all
     */
    public static final String STR_ALL = "all";


    //jdbc
    public static final String JDBC_URL_MYSQL_SERVER = "dmp.jdbc.mysql.server";
    public static final String JDBC_URL_MYSQL_PROT = "dmp.jdbc.mysql.port";
    public static final String JDBC_URL_MYSQL_USER = "dmp.jdbc.mysql.user";
    public static final String JDBC_URL_MYSQL_PASSWD = "dmp.jdbc.mysql.passwd";

    /**
     * 行为数据源类型
     *
     * @author phovan
     */
    public static enum BusinessSourceType {
        BUYINFO("buyinfo"), CCS_INSTALL("ccs_install"), CCS_COMPLAINT("ccs_complaint"),
        CCS_CONSULT("ccs_consult"), CCS_SERVICE("ccs_service"), CCS_UPKEEP("ccs_upkeep");
        private final String value;

        private BusinessSourceType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 仓库层表名称
     *
     * @author phovan
     */
    public static enum DwTableName {
        BUYINFO("dw_user_buyinfo"), CCS_INSTALL("dw_user_ccs_install"), CCS_COMPLAINT("dw_user_ccs_complaint"),
        CCS_CONSULT("dw_user_ccs_consult"), CCS_SERVICE("dw_user_ccs_service"), CCS_UPKEEP("dw_user_ccs_upkeep"),
        BASE_INFO("dw_user_base_info"), ADDR_INFO("dw_user_addr_info"), LABEL_REL("dw_user_label_rel"),
        PHONE_INFO("dw_user_phone_info"), PRODUCT_INFO("dw_user_product_info"), WECHAT_INFO("dw_user_wechat_info"),
        QQ_INFO("dw_user_qq_info"), MEM_INFO("dw_user_mem_info"), BUYERNICK_INFO("dw_user_buyernick_info"),
        PAY_INFO("dw_user_pay_info"), EMAIL_INFO("dw_user_email_info"), WEBSITE_INFO("dw_user_website_info"),
        RELATION_INFO("dw_user_relation_info"), MICROBLOG_INFO("dw_user_microblog_info");
        private final String value;

        private DwTableName(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

}

