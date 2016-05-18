# 目录：
## Hive文档资料
- [Hivewiki](https://cwiki.apache.org/confluence/display/Hive/Home) 官方参考
- [csdn博客](http://blog.csdn.net/u010376788/article/category/6053411)
- [HIVE教程](http://www.phperz.com/special/20.html)
- [hive自定义函数](https://github.com/delongwu/techdoc/wiki/HIVE-UDF%E5%87%BD%E6%95%B0%E7%BC%96%E5%86%99)
- [hive api](https://cwiki.apache.org/confluence/display/Hive/Hive+APIs+Overview)
## Hive函数大全
### 一、关系运算：
1. 等值比较: =
2. 等值比较:<=>
3. 不等值比较: <>和!=
4. 小于比较: <
5. 小于等于比较: <=
6. 大于比较: >
7. 大于等于比较: >=
8. 区间比较
9. 空值判断: IS NULL
10. 非空判断: IS NOT NULL
10. LIKE比较: LIKE
11. JAVA的LIKE操作: RLIKE
12. REGEXP操作: REGEXP
### 二、数学运算：
1. 加法操作: +
2. 减法操作: –
3. 乘法操作: *
4. 除法操作: /
5. 取余操作: %
6. 位与操作: &
7. 位或操作: |
8. 位异或操作: ^
9．位取反操作: ~
### 三、逻辑运算：
1. 逻辑与操作: AND 、&&
2. 逻辑或操作: OR 、||
3. 逻辑非操作: NOT、!
### 四、复合类型构造函数
1. map结构
2. struct结构
3. named_struct结构
4. array结构
5. create_union
### 五、复合类型操作符
1. 获取array中的元素
2. 获取map中的元素
3. 获取struct中的元素
### 六、数值计算函数
1. 取整函数: round
2. 指定精度取整函数: round
3. 向下取整函数: floor
4. 向上取整函数: ceil
5. 向上取整函数: ceiling
6. 取随机数函数: rand
7. 自然指数函数: exp
8. 以10为底对数函数: log10
9. 以2为底对数函数: log2
10. 对数函数: log
11. 幂运算函数: pow
12. 幂运算函数: power
13. 开平方函数: sqrt
14. 二进制函数: bin
15. 十六进制函数: hex
16. 反转十六进制函数: unhex
17. 进制转换函数: conv
18. 绝对值函数: abs
19. 正取余函数: pmod
20. 正弦函数: sin
21. 反正弦函数: asin
22. 余弦函数: cos
23. 反余弦函数: acos
24. positive函数: positive
25. negative函数: negative
### 七、集合操作函数
1. map类型大小：size
2. array类型大小：size
3. 判断元素数组是否包含元素：array_contains
4. 获取map中所有value集合
5. 获取map中所有key集合
6. 数组排序
### 八、类型转换函数
1. 二进制转换：binary
2. 基础类型之间强制转换：cast
### 九、日期函数
1. UNIX时间戳转日期函数: from_unixtime
2. 获取当前UNIX时间戳函数: unix_timestamp
3. 日期转UNIX时间戳函数: unix_timestamp
4. 指定格式日期转UNIX时间戳函数: unix_timestamp
5. 日期时间转日期函数: to_date
6. 日期转年函数: year
7. 日期转月函数: month
8. 日期转天函数: day
9. 日期转小时函数: hour
10. 日期转分钟函数: minute
11. 日期转秒函数: second
12. 日期转周函数: weekofyear
13. 日期比较函数: datediff
14. 日期增加函数: date_add
15. 日期减少函数: date_sub
### 十、条件函数
1. If函数: if
2. 非空查找函数: COALESCE
3. 条件判断函数：CASE
4. 条件判断函数：CASE
### 十一、字符串函数
1.    字符ascii码函数：ascii
2.    base64字符串
3. 字符串连接函数：concat
4.    带分隔符字符串连接函数：concat_ws
5. 数组转换成字符串的函数：concat_ws
6. 小数位格式化成字符串函数：format_number
7. 字符串截取函数：substr,substring
8. 字符串截取函数：substr,substring
9. 字符串查找函数：instr
10. 字符串长度函数：length
11. 字符串查找函数：locate
12. 字符串格式化函数：printf
13. 字符串转换成map函数：str_to_map
14. base64解码函数：unbase64(string str)
15. 字符串转大写函数：upper,ucase
16. 字符串转小写函数：lower,lcase
17. 去空格函数：trim
18. 左边去空格函数：ltrim
19. 右边去空格函数：rtrim
20. 正则表达式替换函数：regexp_replace
21. 正则表达式解析函数：regexp_extract
22. URL解析函数：parse_url
23. json解析函数：get_json_object
24. 空格字符串函数：space
25. 重复字符串函数：repeat
26. 左补足函数：lpad
27. 右补足函数：rpad
28. 分割字符串函数: split
29. 集合查找函数: find_in_set
30.    分词函数：sentences
31. 分词后统计一起出现频次最高的TOP-K
32. 分词后统计与指定单词一起出现频次最高的TOP-K
### 十二、混合函数
1. 调用Java函数：java_method
2. 调用Java函数：reflect
3. 字符串的hash值：hash
### 十三、XPath解析XML函数
1. xpath
2. xpath_string
3. xpath_boolean
4. xpath_short, xpath_int, xpath_long
5. xpath_float, xpath_double, xpath_number
### 十四、汇总统计函数（UDAF）
1. 个数统计函数: count
2. 总和统计函数: sum
3. 平均值统计函数: avg
4. 最小值统计函数: min
5. 最大值统计函数: max
6. 非空集合总体变量函数: var_pop
7. 非空集合样本变量函数: var_samp
8. 总体标准偏离函数: stddev_pop
9. 样本标准偏离函数: stddev_samp
10. 中位数函数: percentile
11. 中位数函数: percentile
12. 近似中位数函数: percentile_approx
13. 近似中位数函数: percentile_approx
14. 直方图: histogram_numeric
15. 集合去重数：collect_set
16. 集合不去重函数：collect_list
### 十五、表格生成函数Table-Generating Functions (UDTF)
1. 数组拆分成多行：explode
2. Map拆分成多行：explode