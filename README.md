# Hive: YouTubeAnalysis

# 需求

统计Youtube的常规指标，各种 TopN 指标：

-- 统计视频观看数 Top10

-- 统计视频类别热度 Top10

-- 统计出视频观看数最高的 20 个视频的所属类别以及类别包含 Top20 视频的个数

-- 统计视频观看数 Top50 所关联视频的所属类别排序

-- 统计每个类别中的视频热度 Top10,以 Music 为例

-- 统计每个类别视频观看数 Top10

-- 统计上传视频最多的用户 Top10 以及他们上传的视频观看次数在前 20 的视频

# 表结构

## 视频表

| 字段      | 备注                         | 详细描述                   |
| --------- | ---------------------------- | -------------------------- |
| videoId   | 视频唯一 id（String）        | 11 位字符串                |
| uploader  | 视频上传者（String）         | 上传视频的用户名  String   |
| age       | 视频年龄（int）              | 视频在平台上的整数天       |
| category  | 视频类别（Array<String>）    | 上传视频指定的视频分类     |
| length    | 视频长度（Int）              | 整形数字标识的视频长度     |
| views     | 观看次数（Int）              | 视频被浏览的次数           |
| rate      | 视频评分（Double）           | 满分 5 分                  |
| Ratings   | 流量（Int）                  | 视频的流量，整型数字       |
| conments  | 评论数（Int）                | 一个视频的整数评论数       |
| relatedId | 相关视频 id（Array<String>） | 相关视频的  id，最多 20 个 |

## 用户表

| **字段** | **备注**     | **字段类型** |
| -------- | ------------ | ------------ |
| uploader | 上传者用户名 | string       |
| videos   | 上传视频数   | int          |
| friends  | 朋友数量     | int          |

## 数据信息和虚拟机配置

### 数据信息

<<<<<<< HEAD
| **表格名**         | **数据量** | 内存大小 | **存储格式** | **备注**                    |
| ------------------ | ---------- | -------- | ------------ | --------------------------- |
| gulivideo_ori      | 743569     | 202.4M   | TextFile     | 原始数据                    |
| gulivideo_orc      | 743569     | 91.0M    | Orc          | 从原始数据导入，Orc存储格式 |
| gulivideo_user_ori | 2139109    | 34.8M    | TextFile     | 原始数据                    |
| gulivideo_user_orc | 2139109    | 17.3M    | Orc          | 从原始数据导入，Orc存储格式 |
=======
| **表格名**            | **数据量** | 内存大小 | **存储格式** | **备注**                    |
| --------------------- | ---------- | -------- | ------------ | --------------------------- |
| youtubevideo_ori      | 743569     | 202.4M   | TextFile     | 原始数据                    |
| youtubevideo_orc      | 743569     | 91.0M    | Orc          | 从原始数据导入，Orc存储格式 |
| youtubevideo_user_ori | 2139109    | 34.8M    | TextFile     | 原始数据                    |
| youtubevideo_user_orc | 2139109    | 17.3M    | Orc          | 从原始数据导入，Orc存储格式 |
>>>>>>> 4ee2bed (update readme)

### 虚拟机配置

|          | hadoop1                              | hadoop2                                                | hadoop3                              |
| -------- | ------------------------------------ | ------------------------------------------------------ | ------------------------------------ |
| 硬件配置 | Centos6.32<br />RAM: 2G<br />HDD:80G | Centos6.32<br />RAM: 2G<br />HDD:80G                   | Centos6.32<br />RAM: 2G<br />HDD:80G |
| HDFS     | NameNode<br />DataNode               | DataNode                                               | SecondaryNameNode<br />DataNode      |
| YARN     | NodeManager                          | ResourceManager<br />JobHistoryServer<br />NodeManager | NodeManager                          |
| Hive     | Hive                                 |                                                        |                                      |

# ETL（Extract-Transform-Load）数据清洗

* 过滤（字段数据缺失），此处可以优化，可在python中进行数据清洗
* category空格问题，视频有多个分类时，字段数据中存在&
* relatedId中，用&替换\t符号，category是用&分割，因此想到用&替换relateid中的\t

法一：java(MR)清洗数据，打成jar包在hadoop上运行

法二：python清洗数据，输出txt文本，hadoop读取txt文本

# 数据准备

* 使用orc存储数据，原因：查询效率与TextFile，Parquet基本相同，但orc格式占用的内存更少，故采用Orc格式
* 建立原始数据表和Orc数据表，并从原始数据表向Orc中查询插入数据，方能完成Orc格式存储

# 业务需求

## 1. 统计视频观看数 Top10

相关字段：views  观看次数（Int）  视频被浏览的次数

```sql
<<<<<<< HEAD
select * from gulivideo_orc order by views desc limit 10;//报错，JVM内存堆溢出
select videoId, category, views from gulivideo_orc order by views desc limit 10;//执行完成
=======
select * from youtubevideo_orc order by views desc limit 10;//报错，JVM内存堆溢出
select videoId, category, views from youtubevideo_orc order by views desc limit 10;//执行完成
>>>>>>> 4ee2bed (update readme)
```

#### 问题及解决

堆溢出：修改了yarn配置，修改了Hive参数配置

> yarn配置
>
> 描述：java.lang.OutOfMemoryError: Java heap space
>
> 解决：在yarn-site.xml 中加入如下代码
>
> <property>     <name>yarn.scheduler.maximum-allocation-mb</name>     <value>2048</value>     </property>     <property>     <name>yarn.scheduler.minimum-allocation-mb</name>     <value>2048</value>     </property>     <property>     <name>yarn.nodemanager.vmem-pmem-ratio</name>     <value>2.1</value>     </property>     <property>     <name>mapred.child.java.opts</name>     <value>-Xmx1024m</value>     </property>          

> Hive参数配置
>
> 在hive语句前 设置 set io.sort.mb=10;  默认值是100
>
> io.sort.mb 的作用
>
> 排序所使用的内存数量。

#### 查询结果

videoid	category	views
dMH0bHeiRNg	["Comedy"]	42513417
0XxI-hvPRRA	["Comedy"]	20282464
1dmVU08zVpA	["Entertainment"]	16087899
RB-wUgnyGv0	["Entertainment"]	15712924
QjA5faZF1A8	["Music"]	15256922
-_CSo1gOd48	["People","Blogs"]	13199833
49IDp76kjPw	["Comedy"]	11970018
tYnn51C3X_w	["Music"]	11823701
pv5zWaTEVkI	["Music"]	11672017
D2kJZOfq7zk	["People","Blogs"]	11184051

## 2. 统计视频类别热度 Top10

热度定义：将视频类别的视频个数作为视频类别的热度

```sql
//统计各个类别的视频数量,需要用到炸裂函数，将数组中的类别炸开
//lateral VIEW explode(category) 表别名 as 炸裂后行程的列的别名
select category_name, count(videoId) videoNum
<<<<<<< HEAD
from gulivideo_orc
lateral VIEW explode(category) gulivideo_orc_explode as category_name
group by category_name
//排序得到前十名
select category_name, videoNum
from ()gulivideo_orc_explode
=======
from youtubevideo_orc
lateral VIEW explode(category) youtubevideo_orc_explode as category_name
group by category_name
//排序得到前十名
select category_name, videoNum
from ()youtubevideo_orc_explode
>>>>>>> 4ee2bed (update readme)
order by videoNum desc
limit 10;

select category_name, videoNum
from (select category_name, count(videoId) videoNum
<<<<<<< HEAD
from gulivideo_orc
lateral VIEW explode(category) gulivideo_orc_explode as category_name
group by category_name)gulivideo_orc_explode
=======
from youtubevideo_orc
lateral VIEW explode(category) youtubevideo_orc_explode as category_name
group by category_name)youtubevideo_orc_explode
>>>>>>> 4ee2bed (update readme)
order by videoNum desc
limit 10;
```

#### 问题及解决

* category存放的是数组结构，因此需要使用炸裂函数
* 炸裂函数explode需要与侧写函数连用 lateral VIEW 可以使炸裂出来的列与原表关联起来，形成笛卡尔积

#### 查询结果

category_name	videonum
Music					 179049
Entertainment		127674
Comedy				  87818
Animation			   73293
Film					    73293
Sports					67329
Gadgets				 59817
Games				   59817
Blogs					 48890
People				   48890

## 3. 统计出视频观看数最高的 20 个视频的所属类别以及类别包含 Top20 视频的个数

```sql
//观看数最高的20个视频
select videoId, category, views
<<<<<<< HEAD
from gulivideo_orc
=======
from youtubevideo_orc
>>>>>>> 4ee2bed (update readme)
order by views desc
limit 20;

//这20个视频所属类别
select category_name, videoId, views
from t1
lateral view explode(category) temp_explode as category_name


select category_name, videoId, views
from (select videoId, category, views
<<<<<<< HEAD
from gulivideo_orc
=======
from youtubevideo_orc
>>>>>>> 4ee2bed (update readme)
order by views desc
limit 20)t1
lateral view explode(category) temp_explode as category_name


//该类别包含前20视频的个数,根据id进行count
select category_name, count(videoId) videoNum
from ()t2
group by category_name

select category_name, count(videoId) videoNum
from (select category_name, videoId, views
from (select videoId, category, views
<<<<<<< HEAD
from gulivideo_orc
=======
from youtubevideo_orc
>>>>>>> 4ee2bed (update readme)
order by views desc
limit 20)t1
lateral view explode(category) temp_explode as category_name)t2
group by category_name
order by videoNum desc
```

#### 问题及解决

对类别包含Top20视频个数的理解

是类别包含 在目前查出前20名视频的个数有多少

是否需要join关联，本题前后都是20，查询范围相同，因此都可以在第一次查出的结果中进行后续查询，若改为“类别包含Top50”则需要关联，因为查询范围不同了

#### 查询结果

category_name	videonum
Entertainment	6
Comedy	6
Music	5
People	2
Blogs	2
UNA	1

## 4. 统计视频观看数 Top50 所关联视频的所属类别排序 

```sql
//1.统计观看数前50的视频
select videoId, category, relatedId, views
<<<<<<< HEAD
from gulivideo_orc
=======
from youtubevideo_orc
>>>>>>> 4ee2bed (update readme)
order by views desc
limit 50;

//2.炸裂所关联的视频
select distinct relateId_explode 
from t1
lateral view explode(relateId) temp_explode as relatedId_explode

select distinct relatedId_explode 
from (select videoId, category, relatedId, views
<<<<<<< HEAD
from gulivideo_orc
=======
from youtubevideo_orc
>>>>>>> 4ee2bed (update readme)
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode

//测试：limit 1时的关联id
relatedid_explode
OxBtqwlTMJQ
1hX1LxXwdl8
NvVbuVGtGSE
Ft6fC6RI4Ms
plv1e3MvxFw
1VL-ShAEjmg
y8k5QbVz3SE
weRfgj_349Q
_MFpPziLP9o
0M-xqfP1ibo
n4Pr_iCxxGU
UrWnNAMec98
QoREX_TLtZo
I-cm3GF-jX0
doIQXfJvydY
6hD3gGg9jMk
Hfbzju1FluI
vVN_pLl5ngg
3PnoFu027hc
7nrpwEDvusY


//3.查询关联视频所属类别
select tableRelate.relatedId_explode relatedId_result, tableCate.category category_needexplode
from tableRelate
<<<<<<< HEAD
join gulivideo_orc tableCate
=======
join youtubevideo_orc tableCate
>>>>>>> 4ee2bed (update readme)
on tableRelate.relatedId_explode = tableCate.videoId

select tableRelate.relatedId_explode relatedId_result, tableCate.category category_needexplode
from (select distinct relatedId_explode 
from (select videoId, category, relatedId, views
<<<<<<< HEAD
from gulivideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join gulivideo_orc tableCate
=======
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join youtubevideo_orc tableCate
>>>>>>> 4ee2bed (update readme)
on tableRelate.relatedId_explode = tableCate.videoId

//测试：limit 1
tablerelate.relatedid_explode	tablecate.category
7nrpwEDvusY	["Comedy"]
3PnoFu027hc	["Comedy"]
vVN_pLl5ngg	["Entertainment"]
Hfbzju1FluI	["Comedy"]
6hD3gGg9jMk	["People","Blogs"]
doIQXfJvydY	["Comedy"]
I-cm3GF-jX0	["Comedy"]
QoREX_TLtZo	["Comedy"]
UrWnNAMec98	["Comedy"]
n4Pr_iCxxGU	["Comedy"]
0M-xqfP1ibo	["Comedy"]
_MFpPziLP9o	["Comedy"]
weRfgj_349Q	["Music"]
y8k5QbVz3SE	["Music"]
1VL-ShAEjmg	["Music"]
plv1e3MvxFw	["Comedy"]
Ft6fC6RI4Ms	["Comedy"]
NvVbuVGtGSE	["Comedy"]
1hX1LxXwdl8	["Comedy"]
OxBtqwlTMJQ	["Comedy"] 

//4.炸裂关联视频所属类别
select relatedId_result, category_name
from ()t3
lateral view explode(category_needexplode) temp_explode as category_name

select relatedId_result, category_name
from (select tableRelate.relatedId_explode relatedId_result, tableCate.category category_needexplode
from (select distinct relatedId_explode 
from (select videoId, category, relatedId, views
<<<<<<< HEAD
from gulivideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join gulivideo_orc tableCate
=======
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join youtubevideo_orc tableCate
>>>>>>> 4ee2bed (update readme)
on tableRelate.relatedId_explode = tableCate.videoId)t3
lateral view explode(category_needexplode) temp_explode as category_name

//5.分组求和
select category_name, count(relatedId_result) result
from t4
order by result desc

select category_name, count(relatedId_result) result
from (select relatedId_result, category_name
from (select tableRelate.relatedId_explode relatedId_result, tableCate.category category_needexplode
from (select distinct relatedId_explode 
from (select videoId, category, relatedId, views
<<<<<<< HEAD
from gulivideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join gulivideo_orc tableCate
=======
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join youtubevideo_orc tableCate
>>>>>>> 4ee2bed (update readme)
on tableRelate.relatedId_explode = tableCate.videoId)t3
lateral view explode(category_needexplode) temp_explode as category_name)t4
group by category_name
order by result desc
```

#### 问题及解决

* 所属类别排序——对关联视频的类别总共包含的关联视频数进行统计并排序
  * 先拿到关联视频id
  * 在拿到关联视频所属类别
  * 再对相同类别求关联视频总数
* 已经拿到关联视频的id，再查找关联视频id所属类别——关联join操作
* 注意 去重操作！！可能不同视频的关联视频相同,group by relatedId或者distinct
* 注意关联后拿到的category，此时的category为数组，还需要炸裂
* 注意最后要对类别分组求和

#### 查询结果

category_name	result
Comedy	232
Entertainment	216
Music	195
Blogs	51
People	51
Film	47
Animation	47
News	22
Politics	22
Games	20
Gadgets	20
Sports	19
Howto	14
DIY	14
UNA	13
Places	12
Travel	12
Animals	11
Pets	11
Autos	4
Vehicles	4

## 5. 统计每个类别中的视频热度 Top10,以 Music 为例

```sql
//创建中间表,方便之后的查询操作
<<<<<<< HEAD
create table gulivideo_category(
=======
create table youtubevideo_category(
>>>>>>> 4ee2bed (update readme)
videoId string, uploader string, age int,
categoryId string, length int,
views int, rate float, ratings int, comments int,
relatedId array<string>)
row format delimited fields terminated by "\t" collection items terminated by "&"
stored as orc;

<<<<<<< HEAD
insert into table gulivideo_category
select videoId, uploader, age, categoryId, length, views, rate, ratings, comments, relatedId
from gulivideo_orc
=======
insert into table youtubevideo_category
select videoId, uploader, age, categoryId, length, views, rate, ratings, comments, relatedId
from youtubevideo_orc
>>>>>>> 4ee2bed (update readme)
lateral view explode(category) catetory as categoryId;
```

```sql
//以Music为例，查询Music中热度前10的视频
select videoId, categoryId, views
<<<<<<< HEAD
from gulivideo_category
=======
from youtubevideo_category
>>>>>>> 4ee2bed (update readme)
where categoryId = "Music"
order by views desc
limit 10;
```

```sql
//统计每个类的视频热度Top10

//统计每个种类下，根据观看数，添加rank值
select videoId, categoryId, views, rank() over(partition by categoryId order by views desc) rk
<<<<<<< HEAD
from gulivideo_category
=======
from youtubevideo_category
>>>>>>> 4ee2bed (update readme)

//过滤前十
select videoId, categoryId, views
from t1
where rk <= 10;

select videoId, categoryId, views
from (select videoId, categoryId, views, rank() over(partition by categoryId order by views desc) rk
<<<<<<< HEAD
from gulivideo_category)t1
=======
from youtubevideo_category)t1
>>>>>>> 4ee2bed (update readme)
where rk <= 10;
```



#### 问题及解决

* 如何对每个类进行——采用窗口函数，**分组TopN**——rank() over(partition by categoryId order by views desc) rk

#### 查询结果

videoid	categoryid	views
2GWPOPSXGYI	Animals	3660009
xmsV9R8FsDA	Animals	3164582
12PsUW-8ge4	Animals	3133523
OeNggIGSKH8	Animals	2457750
WofFb_eOxxA	Animals	2075728
AgEmZ39EtFk	Animals	1999469
a-gW3RbJd8U	Animals	1836870
8CL2hetqpfg	Animals	1646808
QmroaYVD_so	Animals	1645984
Sg9x5mUjbH8	Animals	1527238
sdUUx5FdySs	Animation	5840839
6B26asyGKDo	Animation	5147533
H20dhY01Xjk	Animation	3772116
55YYaJIrmzo	Animation	3356163
JzqumbhfxRo	Animation	3230774
eAhfZUZiwSE	Animation	3114215
h7svw0m-wO0	Animation	2866490
tAq3hWBlalU	Animation	2830024
AJzU3NjDikY	Animation	2569611
ElrldD02if0	Animation	2337238

## 6. 统计每个类别视频观看数 Top10

同第五题

## 7.统计上传视频最多的用户 Top10 以及他们上传的视频观看次数在前 20 的视频

题目可能存在歧义：上传视频观看次数   在整体前20的哪些  or  一个人发50个视频，他自己的视频排名前20（本题理解为后者）

```sql
//上传视频Top10用户
select uploader, videos
<<<<<<< HEAD
from gulivideo_user_orc
=======
from youtubevideo_user_orc
>>>>>>> 4ee2bed (update readme)
order by videos desc
limit 10;

uploader	videos
expertvillage	86228
TourFactory	49078
myHotelVideo	33506
AlexanderRodchenko	24315
VHTStudios	20230
ephemeral8	19498
HSN	15371
rattanakorn	12637
Ruchaneewan	10059
futifu	9668

//关联用户表和视频表
select tableVideo.videoId videoId_result, tableUser.uploader uploader_result, tableVideo.views views_result,
rank() over(partition by tableUser.uploader order by tableVideo.views desc) rk
from ()tableUser
<<<<<<< HEAD
join gulivideo_orc tableVideo
=======
join youtubevideo_orc tableVideo
>>>>>>> 4ee2bed (update readme)
on tableUser.uploader = tableVideo.uploader


select tableVideo.videoId videoId_result, tableUser.uploader uploader_result, tableVideo.views views_result,
rank() over(partition by tableUser.uploader order by tableVideo.views desc) rk
from (select uploader, videos
<<<<<<< HEAD
from gulivideo_user_orc
order by videos desc
limit 10)tableUser
join gulivideo_orc tableVideo
=======
from youtubevideo_user_orc
order by videos desc
limit 10)tableUser
join youtubevideo_orc tableVideo
>>>>>>> 4ee2bed (update readme)
on tableUser.uploader = tableVideo.uploader //只查出105条记录，原因是当前video表中的记录并没有完整记录所有用户发布的所有视频

//rank分组TopN
select videoId_result, uploader_result, views_result
from ()t2
where rk <= 20

select videoId_result, uploader_result, views_result
from (select tableVideo.videoId videoId_result, tableUser.uploader uploader_result, tableVideo.views views_result,
rank() over(partition by tableUser.uploader order by tableVideo.views desc) rk
from (select uploader, videos
<<<<<<< HEAD
from gulivideo_user_orc
order by videos desc
limit 10)tableUser
join gulivideo_orc tableVideo
=======
from youtubevideo_user_orc
order by videos desc
limit 10)tableUser
join youtubevideo_orc tableVideo
>>>>>>> 4ee2bed (update readme)
on tableUser.uploader = tableVideo.uploader)t2
where rk <= 20;//40条数据

```

#### 问题及解决

只查出40条记录的原因，uploader发的所有视频记录并不一定都在video表中

#### 查询结果

videoid_result	uploader_result	views_result
5_T5Inddsuo	Ruchaneewan	3132
wje4lUtbYNU	Ruchaneewan	1086
i8rLbOUhAlM	Ruchaneewan	549
OwnEtde9_Co	Ruchaneewan	453
5Zf0lbAdJP0	Ruchaneewan	441
wenI5MrYT20	Ruchaneewan	426
3hzOiFP-5so	Ruchaneewan	420
Iq4e3SopjxQ	Ruchaneewan	420
JgyOlXjjuw0	Ruchaneewan	418
fGBVShTsuyo	Ruchaneewan	395
O3aoL70DlVc	Ruchaneewan	389
q4y2ZS5OQ88	Ruchaneewan	344
lyUJB2eMVVg	Ruchaneewan	271
_RF_3VhaQpw	Ruchaneewan	242
DDl2cjI-aJs	Ruchaneewan	231
xbYyjUdhtJw	Ruchaneewan	227
4dkKeIUkN7E	Ruchaneewan	226
qCfuQA6N4K0	Ruchaneewan	213
TmYbGQaRcNM	Ruchaneewan	209
dOlfPsFSjw0	Ruchaneewan	206
-IxHBW0YpZw	expertvillage	39059
BU-fT5XI_8I	expertvillage	29975
ADOcaBYbMl0	expertvillage	26270
yAqsULIDJFE	expertvillage	25511
vcm-t0TJXNg	expertvillage	25366
0KYGFawp14c	expertvillage	24659
j4DpuPvMLF4	expertvillage	22593
Msu4lZb2oeQ	expertvillage	18822
ZHZVj44rpjE	expertvillage	16304
foATQY3wovI	expertvillage	13576
-UnQ8rcBOQs	expertvillage	13450
crtNd46CDks	expertvillage	11639
D1leA0JKHhE	expertvillage	11553
NJu2oG1Wm98	expertvillage	11452
CapbXdyv4j4	expertvillage	10915
epr5erraEp4	expertvillage	10817
IyQoDgaLM7U	expertvillage	10597
tbZibBnusLQ	expertvillage	10402
_GnCHodc7mk	expertvillage	9422
hvEYlSlRitU	expertvillage	7123
Time taken: 73.908 seconds, Fetched: 40 row(s)
