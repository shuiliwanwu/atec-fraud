drop table if exists zp_time_feature;
create table if not exists zp_time_feature as
select 
	t2.*,
	case when t2.dayofweek=5 or t2.dayofweek=6 then 1 else 0 end as is_weekend,
    case when t2.dayofweek=0 then 1 else 0 end as is_week1,
    case when t2.dayofweek=1 then 1 else 0 end as is_week2,
    case when t2.dayofweek=2 then 1 else 0 end as is_week3,
    case when t2.dayofweek=3 then 1 else 0 end as is_week4,
    case when t2.dayofweek=4 then 1 else 0 end as is_week5,
    case when t2.dayofweek=5 then 1 else 0 end as is_week6,
    case when t2.dayofweek=6 then 1 else 0 end as is_week7,
    case when t2.hour<=5 and t2.hour>=1 then 1 else 0 end as is_lingchen  
    from
    (
        select 
            t1.event_id,
            datepart(t1.datetimes, 'dd') as day,
            datepart(t1.datetimes, 'hh') as hour, 
        	weekday(t1.datetimes) as dayofweek
    	from (select event_id, to_date(gmt_occur, 'yyyy-mm-dd hh') as datetimes from zp_train_test) t1
    ) t2;
    

-- amt与历史amt max min avg medain的距离
drop table if exists zp_amt_feature;
create table if not exists zp_amt_feature as
select
	t2.event_id,
	(t2.amt-min_amt2) as amt_min_distance, 
    (t2.amt-max_amt2) as amt_max_distance,
    (t2.amt-median_amt2) as amt_median_distance, 
    (t2.amt-avg_amt2) as amt_avg_distance 
from
(
    select 
        event_id, avg(amt) as amt,
        max(amt2) as max_amt2, min(amt2) as min_amt2, avg(amt2) as avg_amt2, median(amt2) as median_amt2 from
        (
            select event_id, amt, amt2 from zp_join_all where datediff(gmt_occur, gmt_occur2, 'hh')>0
        ) t1
    group by event_id    
) t2 