1. 统计视频观看数 Top10
select * from youtubevideo_orc order by views desc limit 10;//报错，JVM内存堆溢出
select videoId, category, views from youtubevideo_orc order by views desc limit 10;//执行完成

2. 统计视频类别热度 Top10
//统计各个类别的视频数量,需要用到炸裂函数，将数组中的类别炸开
//lateral VIEW explode(category) 表别名 as 炸裂后行程的列的别名
select category_name, count(videoId) videoNum
from youtubevideo_orc
lateral VIEW explode(category) youtubevideo_orc_explode as category_name
group by category_name
//排序得到前十名
select category_name, videoNum
from ()youtubevideo_orc_explode
order by videoNum desc
limit 10;

select category_name, videoNum
from (select category_name, count(videoId) videoNum
from youtubevideo_orc
lateral VIEW explode(category) youtubevideo_orc_explode as category_name
group by category_name)youtubevideo_orc_explode
order by videoNum desc
limit 10;

3. 统计出视频观看数最高的 20 个视频的所属类别以及类别包含 Top20 视频的个数
//观看数最高的20个视频
select videoId, category, views
from youtubevideo_orc
order by views desc
limit 20;

//这20个视频所属类别
select category_name, videoId, views
from t1
lateral view explode(category) temp_explode as category_name


select category_name, videoId, views
from (select videoId, category, views
from youtubevideo_orc
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
from youtubevideo_orc
order by views desc
limit 20)t1
lateral view explode(category) temp_explode as category_name)t2
group by category_name
order by videoNum DESC

4. 统计视频观看数 Top50 所关联视频的所属类别排序 
//1.统计观看数前50的视频
select videoId, category, relatedId, views
from youtubevideo_orc
order by views desc
limit 50;

//2.炸裂所关联的视频
select distinct relateId_explode 
from t1
lateral view explode(relateId) temp_explode as relatedId_explode

select distinct relatedId_explode 
from (select videoId, category, relatedId, views
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode


//3.查询关联视频所属类别
select tableRelate.relatedId_explode relatedId_result, tableCate.category category_needexplode
from tableRelate
join youtubevideo_orc tableCate
on tableRelate.relatedId_explode = tableCate.videoId

select tableRelate.relatedId_explode relatedId_result, tableCate.category category_needexplode
from (select distinct relatedId_explode 
from (select videoId, category, relatedId, views
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join youtubevideo_orc tableCate
on tableRelate.relatedId_explode = tableCate.videoId


//4.炸裂关联视频所属类别
select relatedId_result, category_name
from ()t3
lateral view explode(category_needexplode) temp_explode as category_name

select relatedId_result, category_name
from (select tableRelate.relatedId_explode relatedId_result, tableCate.category category_needexplode
from (select distinct relatedId_explode 
from (select videoId, category, relatedId, views
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join youtubevideo_orc tableCate
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
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode)tableRelate
join youtubevideo_orc tableCate
on tableRelate.relatedId_explode = tableCate.videoId)t3
lateral view explode(category_needexplode) temp_explode as category_name)t4
group by category_name
order by result DESC

5. 统计每个类别中的视频热度 Top10,以 Music 为例
//统计每个种类下，根据观看数，添加rank值
select videoId, categoryId, views, rank() over(partition by categoryId order by views desc) rk
from youtubevideo_category

//过滤前十
select videoId, categoryId, views
from t1
where rk <= 10;

select videoId, categoryId, views
from (select videoId, categoryId, views, rank() over(partition by categoryId order by views desc) rk
from youtubevideo_category)t1
where rk <= 10;

6. 统计每个类别视频观看数 Top10
同第五题


7.统计上传视频最多的用户 Top10 以及他们上传的视频观看次数在前 20 的视频
//上传视频Top10用户
select uploader, videos
from youtubevideo_user_orc
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
join youtubevideo_orc tableVideo
on tableUser.uploader = tableVideo.uploader


select tableVideo.videoId videoId_result, tableUser.uploader uploader_result, tableVideo.views views_result,
rank() over(partition by tableUser.uploader order by tableVideo.views desc) rk
from (select uploader, videos
from youtubevideo_user_orc
order by videos desc
limit 10)tableUser
join youtubevideo_orc tableVideo
on tableUser.uploader = tableVideo.uploader //只查出105条记录，原因是当前video表中的记录并没有完整记录所有用户发布的所有视频

//rank分组TopN
select videoId_result, uploader_result, views_result
from ()t2
where rk <= 20

select videoId_result, uploader_result, views_result
from (select tableVideo.videoId videoId_result, tableUser.uploader uploader_result, tableVideo.views views_result,
rank() over(partition by tableUser.uploader order by tableVideo.views desc) rk
from (select uploader, videos
from youtubevideo_user_orc
order by videos desc
limit 10)tableUser
join youtubevideo_orc tableVideo
on tableUser.uploader = tableVideo.uploader)t2
where rk <= 20;//40条数据