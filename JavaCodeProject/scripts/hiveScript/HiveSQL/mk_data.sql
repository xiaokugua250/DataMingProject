-- ml数据集操作

CREATE DATABASE ${DATABASE} IF NOT EXISTS ;

create  table du_u_data(
userid INT COMMENT '用户id',
itemid INT COMMENT '电影ID',
ratings DOUBLE COMMENT '评分',
dataTime STRING  COMMENT '时间'
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  STORED AS TEXTFILE;



CREATE TABLE IF NOT EXISTS  du_u_item(
movieid INT COMMENT'电影ID',
movietitle STRING COMMENT '电影名称',
releasedate String COMMENT'发行时间',
videoreleasedae String COMMENT '光碟发行时间',
IMDbURL   STRING COMMENT 'IMBD_URL',
unknown_type           STRING COMMENT '未知',
Action_type      STRING COMMENT '动作',
Adventure       STRING COMMENT '探险',
Animation     STRING COMMENT '动物',
Childrens      STRING COMMENT '',
Comedy      STRING COMMENT '喜剧',
Crime       STRING COMMENT '',
Documentary       STRING COMMENT '',
Drama       STRING COMMENT '歌剧',
Fantasy                   STRING COMMENT '',
Film_Noir       STRING COMMENT '',
Horror      STRING COMMENT '',
Musical       STRING COMMENT '音乐片',
Mystery       STRING COMMENT '悬疑片',
Romance       STRING COMMENT '爱情片',
Sci_Fi                    STRING COMMENT '',
Thriller      STRING COMMENT '',
War       STRING COMMENT '战争片',
Western               STRING COMMENT '西部片'
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'  STORED AS TEXTFILE;
load data  local inpath '/user/zeus/duliang/warehouse/hive_db/ml_data/u.item' overwrite into table du_u_item;