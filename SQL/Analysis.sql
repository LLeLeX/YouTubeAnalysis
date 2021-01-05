1. ͳ����Ƶ�ۿ��� Top10
select * from youtubevideo_orc order by views desc limit 10;//����JVM�ڴ�����
select videoId, category, views from youtubevideo_orc order by views desc limit 10;//ִ�����

2. ͳ����Ƶ����ȶ� Top10
//ͳ�Ƹ���������Ƶ����,��Ҫ�õ�ը�Ѻ������������е����ը��
//lateral VIEW explode(category) ����� as ը�Ѻ��г̵��еı���
select category_name, count(videoId) videoNum
from youtubevideo_orc
lateral VIEW explode(category) youtubevideo_orc_explode as category_name
group by category_name
//����õ�ǰʮ��
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

3. ͳ�Ƴ���Ƶ�ۿ�����ߵ� 20 ����Ƶ����������Լ������� Top20 ��Ƶ�ĸ���
//�ۿ�����ߵ�20����Ƶ
select videoId, category, views
from youtubevideo_orc
order by views desc
limit 20;

//��20����Ƶ�������
select category_name, videoId, views
from t1
lateral view explode(category) temp_explode as category_name


select category_name, videoId, views
from (select videoId, category, views
from youtubevideo_orc
order by views desc
limit 20)t1
lateral view explode(category) temp_explode as category_name


//��������ǰ20��Ƶ�ĸ���,����id����count
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

4. ͳ����Ƶ�ۿ��� Top50 ��������Ƶ������������� 
//1.ͳ�ƹۿ���ǰ50����Ƶ
select videoId, category, relatedId, views
from youtubevideo_orc
order by views desc
limit 50;

//2.ը������������Ƶ
select distinct relateId_explode 
from t1
lateral view explode(relateId) temp_explode as relatedId_explode

select distinct relatedId_explode 
from (select videoId, category, relatedId, views
from youtubevideo_orc
order by views desc
limit 50)t1
lateral view explode(relatedId) temp_explode as relatedId_explode


//3.��ѯ������Ƶ�������
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


//4.ը�ѹ�����Ƶ�������
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

//5.�������
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

5. ͳ��ÿ������е���Ƶ�ȶ� Top10,�� Music Ϊ��
//ͳ��ÿ�������£����ݹۿ��������rankֵ
select videoId, categoryId, views, rank() over(partition by categoryId order by views desc) rk
from youtubevideo_category

//����ǰʮ
select videoId, categoryId, views
from t1
where rk <= 10;

select videoId, categoryId, views
from (select videoId, categoryId, views, rank() over(partition by categoryId order by views desc) rk
from youtubevideo_category)t1
where rk <= 10;

6. ͳ��ÿ�������Ƶ�ۿ��� Top10
ͬ������


7.ͳ���ϴ���Ƶ�����û� Top10 �Լ������ϴ�����Ƶ�ۿ�������ǰ 20 ����Ƶ
//�ϴ���ƵTop10�û�
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

//�����û������Ƶ��
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
on tableUser.uploader = tableVideo.uploader //ֻ���105����¼��ԭ���ǵ�ǰvideo���еļ�¼��û��������¼�����û�������������Ƶ

//rank����TopN
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
where rk <= 20;//40������