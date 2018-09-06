--短时间（一个小时）内ip的省市有大的变化
drop table if exists zp_ip_1hour;
create table if not exists zp_ip_1hour as
select t1.event_id, 
	if (sum(t1.ip_prov_same_1hour)=0, 1, 0) as ip_prov_same_1hour_is_first,
    if (sum(t1.ip_city_same_1hour)=0, 1, 0) as ip_city_same_1hour_is_first

from
(select event_id, 
    int(ip_prov=ip_prov2) as ip_prov_same_1hour,
    int(ip_city=ip_city2) as ip_city_same_1hour
from zp_join_all 
 	where datediff(gmt_occur, gmt_occur2, 'hh')=1 )t1
group by t1.event_id;




















