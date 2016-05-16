--数量校验

--内部融合表：数量
--ecm

select count(*) from ecm.t_order_main;

select count(*) from temp.ecm_aggregation_table_with_id;

select count(*) from temp.temp_ecm_id_integration_result;

select count(distinct(temp_id)) from temp.ecm_aggregation_table_with_id;

select count(distinct(user_id)) from temp.temp_ecm_id_integration_result;

--dbs

select count(*) from dbs.HSCHM_RETAILORDER_H_3YA;

select count(*) from temp.dbs_aggregation_table_with_ID;

select count(*) from temp.temp_dbs_id_integration_result;

select count(distinct(uuid)) from temp.dbs_aggregation_table_with_ID;

select count(distinct(user_id)) from temp.temp_dbs_id_integration_result;

--css

select count(*) from css.t_cs_customer;

select count(*) from temp.dw_ccs_merge_customer;

select count(*) from temp.temp_css_id_integration_result;

select count(distinct(temp_id)) from temp.dw_ccs_merge_customer;

select count(distinct(user_id)) from temp.temp_css_id_integration_result;

--外部融合表：数量

select count(*) from dw_dmp.dw_user_base_info;
select count(distinct(user_id)) from dw_dmp.dw_user_base_info;

select count(*) from dw_dmp.dw_user_phone_info;
select count(distinct(user_id)) from dw_dmp.dw_user_phone_info;

select count(*) from dw_dmp.dw_user_addr_info;
select count(distinct(user_id)) from dw_dmp.dw_user_addr_info;

select count(*) from dw_dmp.dw_user_pay_info;
select count(distinct(user_id)) from dw_dmp.dw_user_pay_info;

select count(*) from dw_dmp.dw_user_email_info;
select count(distinct(user_id)) from dw_dmp.dw_user_email_info;



--一对多关系校验：

--内部融合表：一个用户对应多个手机号

select 
    count(*)
from(
    select 
        count(*) as count
    from(
        select 
            temp_id, receiver_mobile, count(*) as count 
        from 
            temp.ecm_aggregation_table_with_id
        group by 
            temp_id, receiver_mobile
    ) temp
    group by 
        temp.temp_id
    having 
        count(*) > 5
) temp1;

select 
    count(*)
from(
    select 
        count(*) as count
    from(
        select 
            uuid, receivermobile, count(*) as count 
        from 
            temp.dbs_aggregation_table_with_id
        group by 
            uuid, receivermobile
    )temp
    group by 
        temp.uuid
    having 
        count(*) > 5
) temp1;

select 
    count(*)
from(
    select 
        count(*) as count
    from(
        select 
            temp_id, stelephone3, count(*) as count 
        from 
            temp.dw_ccs_merge_customer
        group by 
            temp_id, stelephone3
    )temp
    group by
        temp.temp_id
    having 
        count(*) > 5
) temp1;

--内部融合表：多个手机号的时间跨度
select 
    count(*)
from (    
    select 
        temp2.temp_id,max(unix_timestamp(temp2.update_time))-min(unix_timestamp(temp2.update_time)) difftime,max(temp2.update_time),min(temp2.update_time)
    from
        temp.ecm_aggregation_table_with_id temp2
    where
        temp2.temp_id in(
            select 
                temp1.temp_id 
            from(
                select 
                    temp_id, receiver_mobile, count(*) as count 
                from 
                    temp.ecm_aggregation_table_with_id
                group by 
                    temp_id, receiver_mobile
            ) temp1
            group by 
                temp1.temp_id
            having 
                count(*) > 5
        )
    group by 
        temp2.temp_id
)temp3
where 
    temp3.difftime > 2592000*3;

select 
    count(*)
from (    
    select 
        temp2.uuid,max(unix_timestamp(temp2.created))-min(unix_timestamp(temp2.created)) difftime,max(temp2.created),min(temp2.created)
    from
        temp.dbs_aggregation_table_with_id temp2
    where
        temp2.uuid in(
            select 
                temp1.uuid 
            from(
                select 
                    uuid, receivermobile, count(*) as count 
                from 
                    temp.dbs_aggregation_table_with_id
                group by 
                    uuid, receivermobile
            ) temp1
            group by 
                temp1.uuid
            having 
                count(*) > 5
        )
    group by 
        temp2.uuid
)temp3
where 
    temp3.difftime > 2592000*3;

select 
    count(*)
from (    
    select 
        temp2.temp_id,max(unix_timestamp(temp2.pub_create_date))-min(unix_timestamp(temp2.pub_create_date)) difftime,max(temp2.pub_create_date),min(temp2.pub_create_date)
    from
        temp.dw_ccs_merge_customer temp2
    where
        temp2.temp_id in(
            select 
                temp1.temp_id 
            from(
                select 
                    temp_id, stelephone3, count(*) as count 
                from 
                    temp.dw_ccs_merge_customer
                group by 
                    temp_id, stelephone3
            ) temp1
            group by 
                temp1.temp_id
            having 
                count(*) > 5
        )
    group by 
        temp2.temp_id
)temp3
where 
    temp3.difftime > 2592000*3;

--内部融合表：是否存在一个手机号对应多个用户

select 
    count(*)
from(
    select 
        count(*) as count
    from(
        select 
            temp_id, receiver_mobile, count(*) as count 
        from 
            temp.ecm_aggregation_table_with_id
        group by 
            temp_id, receiver_mobile
    )temp
    group by 
        temp.receiver_mobile
    having 
        count(*) > 1
) temp1;

select 
    count(*)
from(
    select 
        count(*) as count
    from(
        select 
            uuid, receivermobile, count(*) as count 
        from 
            temp.dbs_aggregation_table_with_id
        group by 
            uuid, receivermobile
    )temp
    group by 
        temp.receivermobile
    having 
        count(*) > 1
) temp1;

select 
    count(*)
from(
    select 
        count(*) as count
    from(
        select 
            temp_id, stelephone3, count(*) as count 
        from 
            temp.dw_ccs_merge_customer
        group by 
            temp_id, stelephone3
    )temp
    group by
        temp.stelephone3
    having 
        count(*) > 1
) temp1;

--外部融合表：一个用户对应多个手机号、地址、支付号、邮箱

select 
    count(*) 
from(
    select 
        count(*) as count 
    from 
        dw_dmp.dw_user_phone_info
    group by 
        user_id
    having 
        count(*) > 20
) temp;

select 
    count(*) 
from(
    select 
        count(*) as count 
    from 
        dw_dmp.dw_user_addr_info
    group by 
        user_id
    having 
        count(*) > 20
) temp;

select 
    count(*) 
from(
    select 
        count(*) as count 
    from 
        dw_dmp.dw_user_pay_info
    group by 
        user_id
    having 
        count(*) > 20
) temp;

select 
    count(*) 
from(
    select 
        *
    from 
        dw_dmp.dw_user_email_info
    group by 
        user_id
    having 
        count(*) > 20
) temp;

--外部融合表：是否存在一个手机号对应多个用户

表关系一对多，user_id是phone的外键，不可能存在。

--质量校验

--内部融合表：字段质量校验

select count(*) from temp.ecm_aggregation_table_with_id where temp_id is null;
select count(*) from temp.ecm_aggregation_table_with_id where receiver_mobile is null;

select count(*) from temp.dbs_aggregation_table_with_ID where uuid is null;
select count(*) from temp.dbs_aggregation_table_with_ID where receivermobile is null;

select count(*) from temp.dw_ccs_merge_customer where temp_id is null;
select count(*) from temp.dw_ccs_merge_customer where stelephone3 is null;

--外部融合表：字段质量校验

select count(*) from dw_dmp.dw_user_base_info where createtime is null;
select count(*) from dw_dmp.dw_user_base_info where unix_timestamp(createtime,'yyyy-MM-dd HH:mm:ss.S') == 0;

select count(*) from dw_dmp.dw_user_base_info where updatetime is null;
select count(*) from dw_dmp.dw_user_base_info where unix_timestamp(updatetime,'yyyy-MM-dd HH:mm:ss.S') == 0;

select count(*) from dw_dmp.dw_user_phone_info where mobile is null;
select count(*) from dw_dmp.dw_user_phone_info where mobile not regexp "^1\\d{10}$";

select count(*) from dw_dmp.dw_user_phone_info where mobile_source is null;
select count(*) from dw_dmp.dw_user_phone_info where mobile_source not regexp "^[1-9]$";

select count(*) from dw_dmp.dw_user_phone_info where starttime is null;
select count(*) from dw_dmp.dw_user_phone_info where unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss.S') == 0;

select count(*) from dw_dmp.dw_user_addr_info where address is null;

select count(*) from dw_dmp.dw_user_addr_info where address_source is null;
select count(*) from dw_dmp.dw_user_addr_info where address_source not regexp "^[1-9]$";

select count(*) from dw_dmp.dw_user_addr_info where createtime is null;
select count(*) from dw_dmp.dw_user_addr_info where unix_timestamp(createtime,'yyyy-MM-dd HH:mm:ss.S') == 0;

select count(*) from dw_dmp.dw_user_pay_info where pay_no is null;

select count(*) from dw_dmp.dw_user_pay_info where pay_no_source is null;
select count(*) from dw_dmp.dw_user_pay_info where pay_no_source not regexp "^[1-9]$";

select count(*) from dw_dmp.dw_user_pay_info where starttime is null;
select count(*) from dw_dmp.dw_user_pay_info where unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss.S') == 0;

select count(*) from dw_dmp.dw_user_email_info where email is null;
select count(*) from dw_dmp.dw_user_email_info where email not regexp "^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+$";

select count(*) from dw_dmp.dw_user_email_info where email_source is null;
select count(*) from dw_dmp.dw_user_email_info where email_source not regexp "^[1-9]$";

select count(*) from dw_dmp.dw_user_email_info where starttime is null;
select count(*) from dw_dmp.dw_user_email_info where unix_timestamp(starttime,'yyyy-MM-dd HH:mm:ss.S') == 0;

--外部融合表：联合字段质量校验

select 
    count(*) 
from 
    dw_dmp.dw_user_base_info 
where 
    usrname is not null and usrname_sourece is null;

select 
    count(*) 
from 
    dw_dmp.dw_user_base_info 
where 
    cardid is not null and cardid_source is null;

select 
    count(*) 
from 
    dw_dmp.dw_user_base_info 
where 
    sex is not null and sex_source is null;

select 
    count(*) 
from 
    dw_dmp.dw_user_base_info 
where 
    birthdate is not null and birthdate_source is null;


select 
    count(*) 
from 
    dw_dmp.dw_user_phone_info 
where 
    mobile is not null and mobile_source is null;

select 
    count(*) 
from 
    dw_dmp.dw_user_phone_info 
where 
    telephone is not null and telephone_source is null;


select 
    count(*) 
from 
    dw_dmp.dw_user_addr_info 
where 
    address is not null and address_source is null;


select 
    count(*) 
from 
    dw_dmp.dw_user_pay_info 
where 
    pay_no is not null and pay_no_source is null;

select 
    count(*) 
from 
    dw_dmp.dw_user_pay_info 
where 
    buyernick is not null and buyernick_source is null;


select 
    count(*) 
from 
    dw_dmp.dw_user_email_info 
where 
    email is not null and email_source is null;



--融合效果验证

select 
    temp_id, count(*) as count
from
    temp.ecm_aggregation_table_with_id
group by 
    temp_id
having 
    count(*) > 10
limit 10;

select 
    receiver_name_ori, *
from
    temp.ecm_aggregation_table_with_id
where
    temp_id = ;


select 
    uuid, count(*) as count
from 
    temp.dbs_aggregation_table_with_id
group by 
    uuid
having 
    count(*) > 10
limit 10;

select 
    *
from
    temp.dbs_aggregation_table_with_id
where
    uuid = ;



select 
    temp_id, count(*) as count
from 
    temp.dw_ccs_merge_customer 
group by
    temp_id
having 
    count(*) > 10
limit 10;

select 
    *
from
    temp.dw_ccs_merge_customer 
where
    temp_id = ;




