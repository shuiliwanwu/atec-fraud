------15*5*4=300----------
------------------------------------------------------------1day---------------------------------------------------
drop table if exists zp_new_window_feature_1day;
create table if not exists zp_new_window_feature_1day as
select 
	t2.event_id,
    -----------------------------------
	t2.ip_same_1day_nb,
    t2.ip_same_1day_all_nb,
    t2.ip_same_1day_rate,
    t2.ip_same_1day_is_first,
    
	t2.network_same_1day_nb,
    t2.network_same_1day_all_nb,
    t2.network_same_1day_rate,    
    t2.network_same_1day_is_first,

	t2.device_sign_same_1day_nb,
    t2.device_sign_same_1day_all_nb,
    t2.device_sign_same_1day_rate,
    t2.device_sign_same_1day_is_first,
    
	t2.info_1_same_1day_nb,
    t2.info_1_same_1day_all_nb,
    t2.info_1_same_1day_rate,    
    t2.info_1_same_1day_is_first,
    
	t2.info_2_same_1day_nb,
    t2.info_2_same_1day_all_nb,
    t2.info_2_same_1day_rate,    
    t2.info_2_same_1day_is_first,
    
	t2.ip_prov_same_1day_nb,
    t2.ip_prov_same_1day_all_nb,
    t2.ip_prov_same_1day_rate,    
    t2.ip_prov_same_1day_is_first,
    
	t2.ip_city_same_1day_nb,
    t2.ip_city_same_1day_all_nb,
    t2.ip_city_same_1day_rate,    
    t2.ip_city_same_1day_is_first,
    
	t2.cert_prov_same_1day_nb,
    t2.cert_prov_same_1day_all_nb,
    t2.cert_prov_same_1day_rate,    
    t2.cert_prov_same_1day_is_first,
    
	t2.is_one_people_same_1day_nb,
    t2.is_one_people_same_1day_all_nb,
    t2.is_one_people_same_1day_rate,    
    t2.is_one_people_same_1day_is_first,
    
	t2.mobile_oper_platform_same_1day_nb,
    t2.mobile_oper_platform_same_1day_all_nb,
    t2.mobile_oper_platform_same_1day_rate,    
    t2.mobile_oper_platform_same_1day_is_first,
    
	t2.operation_channel_same_1day_nb,
    t2.operation_channel_same_1day_all_nb,
    t2.operation_channel_same_1day_rate,    
    t2.operation_channel_same_1day_is_first,
    
	t2.pay_scene_same_1day_nb,
    t2.pay_scene_same_1day_all_nb,
    t2.pay_scene_same_1day_rate,    
    t2.pay_scene_same_1day_is_first,
    
	t2.amt_same_1day_nb,
    t2.amt_same_1day_all_nb,
    t2.amt_same_1day_rate,    
    t2.amt_same_1day_is_first,

	t2.opposing_id_same_1day_nb,
    t2.opposing_id_same_1day_all_nb,
    t2.opposing_id_same_1day_rate, 
    t2.opposing_id_same_1day_is_first,
    
	t2.is_peer_pay_same_1day_nb,
    t2.is_peer_pay_same_1day_all_nb,
    t2.is_peer_pay_same_1day_rate,    
    t2.is_peer_pay_same_1day_is_first,
    -------------------------------------
    t2.ip_nb as ip_distinct_1day,
    t2.network_nb as network_distinct_1day,
    t2.device_sign_nb as device_sign_distinct_1day,
    t2.info_1_nb as info_1_distinct_1day,
    t2.info_2_nb as info_2_distinct_1day,
    t2.ip_prov_nb as ip_prov_distinct_1day,
    t2.ip_city_nb as ip_city_distinct_1day,
    t2.cert_prov_nb as cert_prov_distinct_1day,
    t2.is_one_people_nb as is_one_people_distinct_1day,
    t2.mobile_oper_platform_nb as mobile_oper_platform_distinct_1day,
    t2.operation_channel_nb as operation_channel_distinct_1day,
    t2.pay_scene_nb as pay_scene_distinct_1day,
    t2.amt_nb as amt_distinct_1day,
    t2.opposing_id_nb as opposing_id_distinct_1day,
    t2.is_peer_pay_nb as is_peer_pay_distinct_1day,
    --------------------------------------------------------------------
    1.0/(t2.ip_nb) - t2.ip_same_1day_rate_1 as ip_1day_distance_rate,
    1.0/(t2.network_nb) - t2.network_same_1day_rate_1 as network_1day_distance_rate,
    1.0/(t2.device_sign_nb) - t2.device_sign_same_1day_rate_1 as device_sign_1day_distance_rate,
    1.0/(t2.info_1_nb) - t2.info_1_same_1day_rate_1 as info_1_1day_distance_rate,
    1.0/(t2.info_2_nb) - t2.info_2_same_1day_rate_1 as info_2_1day_distance_rate,
    1.0/(t2.ip_prov_nb) - t2.ip_prov_same_1day_rate_1 as ip_prov_1day_distance_rate,
    1.0/(t2.ip_city_nb) - t2.ip_city_same_1day_rate_1 as ip_city_1day_distance_rate,
    1.0/(t2.cert_prov_nb) - t2.cert_prov_same_1day_rate_1 as cert_prov_1day_distance_rate,
    1.0/(t2.is_one_people_nb) - t2.is_one_people_same_1day_rate_1 as is_one_people_1day_distance_rate,
    1.0/(t2.mobile_oper_platform_nb) - t2.mobile_oper_platform_same_1day_rate_1 as mobile_oper_platform_1day_distance_rate,
    1.0/(t2.operation_channel_nb) - t2.operation_channel_same_1day_rate_1 as operation_channel_1day_distance_rate,
    1.0/(t2.pay_scene_nb) - t2.pay_scene_same_1day_rate_1 as pay_scene_1day_distance_rate,
    1.0/(t2.amt_nb) - t2.amt_same_1day_rate_1 as amt_1day_distance_rate,
    1.0/(t2.opposing_id_nb) - t2.opposing_id_same_1day_rate_1 as opposing_id_1day_distance_rate,
    1.0/(t2.is_peer_pay_nb) - t2.is_peer_pay_same_1day_rate_1 as is_peer_pay_1day_distance_rate
    

from
    (select t1.event_id as event_id, 

        count(distinct t1.client_ip2) as ip_nb,
        count(distinct t1.network2) as network_nb,
        count(distinct t1.device_sign2) as device_sign_nb,
        count(distinct t1.info_12) as info_1_nb,
        count(distinct t1.info_22) as info_2_nb,
        count(distinct t1.ip_prov2) as ip_prov_nb,
        count(distinct t1.ip_city2) as ip_city_nb,
        count(distinct t1.cert_prov2) as cert_prov_nb,
        count(distinct t1.is_one_people2) as is_one_people_nb,
        count(distinct t1.mobile_oper_platform2) as mobile_oper_platform_nb,
        count(distinct t1.operation_channel2) as operation_channel_nb,
        count(distinct t1.pay_scene2) as pay_scene_nb,
        count(distinct t1.amt2) as amt_nb,
        count(distinct t1.opposing_id2) as opposing_id_nb,
        count(distinct t1.is_peer_pay2) as is_peer_pay_nb,

        sum(t1.ip_same_1day) as ip_same_1day_nb,
        count(t1.ip_same_1day) as ip_same_1day_all_nb,
        sum(t1.ip_same_1day)/(count(t1.ip_same_1day)+3) as ip_same_1day_rate,
        sum(t1.ip_same_1day)/count(t1.ip_same_1day) as ip_same_1day_rate_1,


        sum(t1.network_same_1day) as network_same_1day_nb,
        count(t1.network_same_1day) as network_same_1day_all_nb,
        sum(t1.network_same_1day)/(count(t1.network_same_1day)+3) as network_same_1day_rate,
     	sum(t1.network_same_1day)/count(t1.network_same_1day) as network_same_1day_rate_1,


        sum(t1.device_sign_same_1day) as device_sign_same_1day_nb,
        count(t1.device_sign_same_1day) as device_sign_same_1day_all_nb,    
        sum(t1.device_sign_same_1day)/(count(t1.device_sign_same_1day)+3) as device_sign_same_1day_rate,
        sum(t1.device_sign_same_1day)/count(t1.device_sign_same_1day) as device_sign_same_1day_rate_1,

        sum(t1.info_1_same_1day) as info_1_same_1day_nb,
        count(t1.info_1_same_1day) as info_1_same_1day_all_nb,   
        sum(t1.info_1_same_1day)/(count(t1.info_1_same_1day)+3) as info_1_same_1day_rate,
        sum(t1.info_1_same_1day)/count(t1.info_1_same_1day) as info_1_same_1day_rate_1,
     
        sum(t1.info_2_same_1day) as info_2_same_1day_nb,
        count(t1.info_2_same_1day) as info_2_same_1day_all_nb,     
        sum(t1.info_2_same_1day)/(count(t1.info_2_same_1day)+3) as info_2_same_1day_rate,
        sum(t1.info_2_same_1day)/count(t1.info_2_same_1day) as info_2_same_1day_rate_1,

        sum(t1.ip_prov_same_1day) as ip_prov_same_1day_nb,
        count(t1.ip_prov_same_1day) as ip_prov_same_1day_all_nb,      
        sum(t1.ip_prov_same_1day)/(count(t1.ip_prov_same_1day)+3) as ip_prov_same_1day_rate,
        sum(t1.ip_prov_same_1day)/count(t1.ip_prov_same_1day) as ip_prov_same_1day_rate_1,

        sum(t1.ip_city_same_1day) as ip_city_same_1day_nb,
        count(t1.ip_city_same_1day) as ip_city_same_1day_all_nb,      
        sum(t1.ip_city_same_1day)/(count(t1.ip_city_same_1day)+3) as ip_city_same_1day_rate,
        sum(t1.ip_city_same_1day)/count(t1.ip_city_same_1day) as ip_city_same_1day_rate_1,

        sum(t1.cert_prov_same_1day) as cert_prov_same_1day_nb,
        count(t1.cert_prov_same_1day) as cert_prov_same_1day_all_nb,     
        sum(t1.cert_prov_same_1day)/(count(t1.cert_prov_same_1day)+3) as cert_prov_same_1day_rate,
        sum(t1.cert_prov_same_1day)/count(t1.cert_prov_same_1day) as cert_prov_same_1day_rate_1,

        sum(t1.is_one_people_same_1day) as is_one_people_same_1day_nb,
        count(t1.is_one_people_same_1day) as is_one_people_same_1day_all_nb,    	
        sum(t1.is_one_people_same_1day)/(count(t1.is_one_people_same_1day)+3) as is_one_people_same_1day_rate,
        sum(t1.is_one_people_same_1day)/count(t1.is_one_people_same_1day) as is_one_people_same_1day_rate_1,

        sum(t1.mobile_oper_platform_same_1day) as mobile_oper_platform_same_1day_nb,
        count(t1.mobile_oper_platform_same_1day) as mobile_oper_platform_same_1day_all_nb,    	   
        sum(t1.mobile_oper_platform_same_1day)/(count(t1.mobile_oper_platform_same_1day)+3) as mobile_oper_platform_same_1day_rate,
        sum(t1.mobile_oper_platform_same_1day)/count(t1.mobile_oper_platform_same_1day) as mobile_oper_platform_same_1day_rate_1,

        sum(t1.operation_channel_same_1day) as operation_channel_same_1day_nb,
        count(t1.operation_channel_same_1day) as operation_channel_same_1day_all_nb,     
        sum(t1.operation_channel_same_1day)/(count(t1.operation_channel_same_1day)+3) as operation_channel_same_1day_rate,
        sum(t1.operation_channel_same_1day)/count(t1.operation_channel_same_1day) as operation_channel_same_1day_rate_1,

        sum(t1.pay_scene_same_1day) as pay_scene_same_1day_nb,
        count(t1.pay_scene_same_1day) as pay_scene_same_1day_all_nb,     
        sum(t1.pay_scene_same_1day)/(count(t1.pay_scene_same_1day)+3) as pay_scene_same_1day_rate,
        sum(t1.pay_scene_same_1day)/count(t1.pay_scene_same_1day) as pay_scene_same_1day_rate_1,

        sum(t1.amt_same_1day) as amt_same_1day_nb,
        count(t1.amt_same_1day) as amt_same_1day_all_nb,     
        sum(t1.amt_same_1day)/(count(t1.amt_same_1day)+3) as amt_same_1day_rate,
        sum(t1.amt_same_1day)/count(t1.amt_same_1day) as amt_same_1day_rate_1,

        sum(t1.opposing_id_same_1day) as opposing_id_same_1day_nb,
        count(t1.opposing_id_same_1day) as opposing_id_same_1day_all_nb,     
        sum(t1.opposing_id_same_1day)/(count(t1.opposing_id_same_1day)+3) as opposing_id_same_1day_rate,
        sum(t1.opposing_id_same_1day)/count(t1.opposing_id_same_1day) as opposing_id_same_1day_rate_1,

        sum(t1.is_peer_pay_same_1day) as is_peer_pay_same_1day_nb,
        count(t1.is_peer_pay_same_1day) as is_peer_pay_same_1day_all_nb,     
        sum(t1.is_peer_pay_same_1day)/(count(t1.is_peer_pay_same_1day)+3) as is_peer_pay_same_1day_rate,
        sum(t1.is_peer_pay_same_1day)/count(t1.is_peer_pay_same_1day) as is_peer_pay_same_1day_rate_1,

        if (sum(t1.ip_same_1day)=0, 1, 0) as ip_same_1day_is_first,
        if (sum(t1.network_same_1day)=0, 1, 0) as network_same_1day_is_first,
        if (sum(t1.device_sign_same_1day)=0, 1, 0) as device_sign_same_1day_is_first,
        if (sum(t1.info_1_same_1day)=0, 1, 0) as info_1_same_1day_is_first,
        if (sum(t1.info_2_same_1day)=0, 1, 0) as info_2_same_1day_is_first,
        if (sum(t1.ip_prov_same_1day)=0, 1, 0) as ip_prov_same_1day_is_first,
        if (sum(t1.ip_city_same_1day)=0, 1, 0) as ip_city_same_1day_is_first,
        if (sum(t1.cert_prov_same_1day)=0, 1, 0) as cert_prov_same_1day_is_first,
        if (sum(t1.is_one_people_same_1day)=0, 1, 0) as is_one_people_same_1day_is_first,
        if (sum(t1.mobile_oper_platform_same_1day)=0, 1, 0) as mobile_oper_platform_same_1day_is_first,
        if (sum(t1.operation_channel_same_1day)=0, 1, 0) as operation_channel_same_1day_is_first,
        if (sum(t1.pay_scene_same_1day)=0, 1, 0) as pay_scene_same_1day_is_first,
        if (sum(t1.amt_same_1day)=0, 1, 0) as amt_same_1day_is_first,
        if (sum(t1.opposing_id_same_1day)=0, 1, 0) as opposing_id_same_1day_is_first,
        if (sum(t1.is_peer_pay_same_1day)=0, 1, 0) as is_peer_pay_same_1day_is_first


    from
    (select event_id, 
        --------------------------------
        client_ip2,
        network2,
        device_sign2,
        info_12,
        info_22,
        ip_prov2,
        ip_city2,
        cert_prov2,
        is_one_people2,
        mobile_oper_platform2,
        operation_channel2,
        pay_scene2,
        amt2,
        opposing_id2,
        is_peer_pay2,
        ---------------------------------------------------
        int(client_ip=client_ip2) as ip_same_1day,
        int(network=network2) as network_same_1day,
        int(device_sign=device_sign2) as device_sign_same_1day,
        int(info_1=info_12) as info_1_same_1day,
        int(info_2=info_22) as info_2_same_1day,
        int(ip_prov=ip_prov2) as ip_prov_same_1day,
        int(ip_city=ip_city2) as ip_city_same_1day,
        int(cert_prov=cert_prov2) as cert_prov_same_1day,
        int(is_one_people=is_one_people2) as is_one_people_same_1day,
        int(mobile_oper_platform=mobile_oper_platform2) as mobile_oper_platform_same_1day,
        int(operation_channel=operation_channel2) as operation_channel_same_1day,
        int(pay_scene=pay_scene2) as pay_scene_same_1day,
        int(amt=amt2) as amt_same_1day,
        int(opposing_id=opposing_id2) as opposing_id_same_1day,
        int(is_peer_pay=is_peer_pay2) as is_peer_pay_same_1day

    from zp_join_all 
        where datediff(gmt_occur, gmt_occur2, 'hh')<24 and datediff(gmt_occur, gmt_occur2, 'hh')>=1)t1
    group by t1.event_id) t2;
------------------------------------------------------------3day---------------------------------------------------
drop table if exists zp_new_window_feature_3day;
create table if not exists zp_new_window_feature_3day as
select 
	t2.event_id,
    -----------------------------------
	t2.ip_same_3day_nb,
    t2.ip_same_3day_all_nb,
    t2.ip_same_3day_rate,
    t2.ip_same_3day_is_first,
    
	t2.network_same_3day_nb,
    t2.network_same_3day_all_nb,
    t2.network_same_3day_rate,    
    t2.network_same_3day_is_first,

	t2.device_sign_same_3day_nb,
    t2.device_sign_same_3day_all_nb,
    t2.device_sign_same_3day_rate,
    t2.device_sign_same_3day_is_first,
    
	t2.info_1_same_3day_nb,
    t2.info_1_same_3day_all_nb,
    t2.info_1_same_3day_rate,    
    t2.info_1_same_3day_is_first,
    
	t2.info_2_same_3day_nb,
    t2.info_2_same_3day_all_nb,
    t2.info_2_same_3day_rate,    
    t2.info_2_same_3day_is_first,
    
	t2.ip_prov_same_3day_nb,
    t2.ip_prov_same_3day_all_nb,
    t2.ip_prov_same_3day_rate,    
    t2.ip_prov_same_3day_is_first,
    
	t2.ip_city_same_3day_nb,
    t2.ip_city_same_3day_all_nb,
    t2.ip_city_same_3day_rate,    
    t2.ip_city_same_3day_is_first,
    
	t2.cert_prov_same_3day_nb,
    t2.cert_prov_same_3day_all_nb,
    t2.cert_prov_same_3day_rate,    
    t2.cert_prov_same_3day_is_first,
    
	t2.is_one_people_same_3day_nb,
    t2.is_one_people_same_3day_all_nb,
    t2.is_one_people_same_3day_rate,    
    t2.is_one_people_same_3day_is_first,
    
	t2.mobile_oper_platform_same_3day_nb,
    t2.mobile_oper_platform_same_3day_all_nb,
    t2.mobile_oper_platform_same_3day_rate,    
    t2.mobile_oper_platform_same_3day_is_first,
    
	t2.operation_channel_same_3day_nb,
    t2.operation_channel_same_3day_all_nb,
    t2.operation_channel_same_3day_rate,    
    t2.operation_channel_same_3day_is_first,
    
	t2.pay_scene_same_3day_nb,
    t2.pay_scene_same_3day_all_nb,
    t2.pay_scene_same_3day_rate,    
    t2.pay_scene_same_3day_is_first,
    
	t2.amt_same_3day_nb,
    t2.amt_same_3day_all_nb,
    t2.amt_same_3day_rate,    
    t2.amt_same_3day_is_first,

	t2.opposing_id_same_3day_nb,
    t2.opposing_id_same_3day_all_nb,
    t2.opposing_id_same_3day_rate, 
    t2.opposing_id_same_3day_is_first,
    
	t2.is_peer_pay_same_3day_nb,
    t2.is_peer_pay_same_3day_all_nb,
    t2.is_peer_pay_same_3day_rate,    
    t2.is_peer_pay_same_3day_is_first,
    -----------------------------------
    t2.ip_nb as ip_distinct_3day,
    t2.network_nb as network_distinct_3day,
    t2.device_sign_nb as device_sign_distinct_3day,
    t2.info_1_nb as info_1_distinct_3day,
    t2.info_2_nb as info_2_distinct_3day,
    t2.ip_prov_nb as ip_prov_distinct_3day,
    t2.ip_city_nb as ip_city_distinct_3day,
    t2.cert_prov_nb as cert_prov_distinct_3day,
    t2.is_one_people_nb as is_one_people_distinct_3day,
    t2.mobile_oper_platform_nb as mobile_oper_platform_distinct_3day,
    t2.operation_channel_nb as operation_channel_distinct_3day,
    t2.pay_scene_nb as pay_scene_distinct_3day,
    t2.amt_nb as amt_distinct_3day,
    t2.opposing_id_nb as opposing_id_distinct_3day,
    t2.is_peer_pay_nb as is_peer_pay_distinct_3day,
    --------------------------------------------------------------------
    1.0/(t2.ip_nb) - t2.ip_same_3day_rate_1 as ip_3day_distance_rate,
    1.0/(t2.network_nb) - t2.network_same_3day_rate_1 as network_3day_distance_rate,
    1.0/(t2.device_sign_nb) - t2.device_sign_same_3day_rate_1 as device_sign_3day_distance_rate,
    1.0/(t2.info_1_nb) - t2.info_1_same_3day_rate_1 as info_1_3day_distance_rate,
    1.0/(t2.info_2_nb) - t2.info_2_same_3day_rate_1 as info_2_3day_distance_rate,
    1.0/(t2.ip_prov_nb) - t2.ip_prov_same_3day_rate_1 as ip_prov_3day_distance_rate,
    1.0/(t2.ip_city_nb) - t2.ip_city_same_3day_rate_1 as ip_city_3day_distance_rate,
    1.0/(t2.cert_prov_nb) - t2.cert_prov_same_3day_rate_1 as cert_prov_3day_distance_rate,
    1.0/(t2.is_one_people_nb) - t2.is_one_people_same_3day_rate_1 as is_one_people_3day_distance_rate,
    1.0/(t2.mobile_oper_platform_nb) - t2.mobile_oper_platform_same_3day_rate_1 as mobile_oper_platform_3day_distance_rate,
    1.0/(t2.operation_channel_nb) - t2.operation_channel_same_3day_rate_1 as operation_channel_3day_distance_rate,
    1.0/(t2.pay_scene_nb) - t2.pay_scene_same_3day_rate_1 as pay_scene_3day_distance_rate,
    1.0/(t2.amt_nb) - t2.amt_same_3day_rate_1 as amt_3day_distance_rate,
    1.0/(t2.opposing_id_nb) - t2.opposing_id_same_3day_rate_1 as opposing_id_3day_distance_rate,
    1.0/(t2.is_peer_pay_nb) - t2.is_peer_pay_same_3day_rate_1 as is_peer_pay_3day_distance_rate
    

from
    (select t1.event_id as event_id, 

        count(distinct t1.client_ip2) as ip_nb,
        count(distinct t1.network2) as network_nb,
        count(distinct t1.device_sign2) as device_sign_nb,
        count(distinct t1.info_12) as info_1_nb,
        count(distinct t1.info_22) as info_2_nb,
        count(distinct t1.ip_prov2) as ip_prov_nb,
        count(distinct t1.ip_city2) as ip_city_nb,
        count(distinct t1.cert_prov2) as cert_prov_nb,
        count(distinct t1.is_one_people2) as is_one_people_nb,
        count(distinct t1.mobile_oper_platform2) as mobile_oper_platform_nb,
        count(distinct t1.operation_channel2) as operation_channel_nb,
        count(distinct t1.pay_scene2) as pay_scene_nb,
        count(distinct t1.amt2) as amt_nb,
        count(distinct t1.opposing_id2) as opposing_id_nb,
        count(distinct t1.is_peer_pay2) as is_peer_pay_nb,

        sum(t1.ip_same_3day) as ip_same_3day_nb,
        count(t1.ip_same_3day) as ip_same_3day_all_nb,
        sum(t1.ip_same_3day)/(count(t1.ip_same_3day)+3) as ip_same_3day_rate,
        sum(t1.ip_same_3day)/count(t1.ip_same_3day) as ip_same_3day_rate_1,


        sum(t1.network_same_3day) as network_same_3day_nb,
        count(t1.network_same_3day) as network_same_3day_all_nb,
        sum(t1.network_same_3day)/(count(t1.network_same_3day)+3) as network_same_3day_rate,
     	sum(t1.network_same_3day)/count(t1.network_same_3day) as network_same_3day_rate_1,


        sum(t1.device_sign_same_3day) as device_sign_same_3day_nb,
        count(t1.device_sign_same_3day) as device_sign_same_3day_all_nb,    
        sum(t1.device_sign_same_3day)/(count(t1.device_sign_same_3day)+3) as device_sign_same_3day_rate,
        sum(t1.device_sign_same_3day)/count(t1.device_sign_same_3day) as device_sign_same_3day_rate_1,

        sum(t1.info_1_same_3day) as info_1_same_3day_nb,
        count(t1.info_1_same_3day) as info_1_same_3day_all_nb,   
        sum(t1.info_1_same_3day)/(count(t1.info_1_same_3day)+3) as info_1_same_3day_rate,
        sum(t1.info_1_same_3day)/count(t1.info_1_same_3day) as info_1_same_3day_rate_1,
     
        sum(t1.info_2_same_3day) as info_2_same_3day_nb,
        count(t1.info_2_same_3day) as info_2_same_3day_all_nb,     
        sum(t1.info_2_same_3day)/(count(t1.info_2_same_3day)+3) as info_2_same_3day_rate,
        sum(t1.info_2_same_3day)/count(t1.info_2_same_3day) as info_2_same_3day_rate_1,

        sum(t1.ip_prov_same_3day) as ip_prov_same_3day_nb,
        count(t1.ip_prov_same_3day) as ip_prov_same_3day_all_nb,      
        sum(t1.ip_prov_same_3day)/(count(t1.ip_prov_same_3day)+3) as ip_prov_same_3day_rate,
        sum(t1.ip_prov_same_3day)/count(t1.ip_prov_same_3day) as ip_prov_same_3day_rate_1,

        sum(t1.ip_city_same_3day) as ip_city_same_3day_nb,
        count(t1.ip_city_same_3day) as ip_city_same_3day_all_nb,      
        sum(t1.ip_city_same_3day)/(count(t1.ip_city_same_3day)+3) as ip_city_same_3day_rate,
        sum(t1.ip_city_same_3day)/count(t1.ip_city_same_3day) as ip_city_same_3day_rate_1,

        sum(t1.cert_prov_same_3day) as cert_prov_same_3day_nb,
        count(t1.cert_prov_same_3day) as cert_prov_same_3day_all_nb,     
        sum(t1.cert_prov_same_3day)/(count(t1.cert_prov_same_3day)+3) as cert_prov_same_3day_rate,
        sum(t1.cert_prov_same_3day)/count(t1.cert_prov_same_3day) as cert_prov_same_3day_rate_1,

        sum(t1.is_one_people_same_3day) as is_one_people_same_3day_nb,
        count(t1.is_one_people_same_3day) as is_one_people_same_3day_all_nb,    	
        sum(t1.is_one_people_same_3day)/(count(t1.is_one_people_same_3day)+3) as is_one_people_same_3day_rate,
        sum(t1.is_one_people_same_3day)/count(t1.is_one_people_same_3day) as is_one_people_same_3day_rate_1,

        sum(t1.mobile_oper_platform_same_3day) as mobile_oper_platform_same_3day_nb,
        count(t1.mobile_oper_platform_same_3day) as mobile_oper_platform_same_3day_all_nb,    	   
        sum(t1.mobile_oper_platform_same_3day)/(count(t1.mobile_oper_platform_same_3day)+3) as mobile_oper_platform_same_3day_rate,
        sum(t1.mobile_oper_platform_same_3day)/count(t1.mobile_oper_platform_same_3day) as mobile_oper_platform_same_3day_rate_1,

        sum(t1.operation_channel_same_3day) as operation_channel_same_3day_nb,
        count(t1.operation_channel_same_3day) as operation_channel_same_3day_all_nb,     
        sum(t1.operation_channel_same_3day)/(count(t1.operation_channel_same_3day)+3) as operation_channel_same_3day_rate,
        sum(t1.operation_channel_same_3day)/count(t1.operation_channel_same_3day) as operation_channel_same_3day_rate_1,

        sum(t1.pay_scene_same_3day) as pay_scene_same_3day_nb,
        count(t1.pay_scene_same_3day) as pay_scene_same_3day_all_nb,     
        sum(t1.pay_scene_same_3day)/(count(t1.pay_scene_same_3day)+3) as pay_scene_same_3day_rate,
        sum(t1.pay_scene_same_3day)/count(t1.pay_scene_same_3day) as pay_scene_same_3day_rate_1,

        sum(t1.amt_same_3day) as amt_same_3day_nb,
        count(t1.amt_same_3day) as amt_same_3day_all_nb,     
        sum(t1.amt_same_3day)/(count(t1.amt_same_3day)+3) as amt_same_3day_rate,
        sum(t1.amt_same_3day)/count(t1.amt_same_3day) as amt_same_3day_rate_1,

        sum(t1.opposing_id_same_3day) as opposing_id_same_3day_nb,
        count(t1.opposing_id_same_3day) as opposing_id_same_3day_all_nb,     
        sum(t1.opposing_id_same_3day)/(count(t1.opposing_id_same_3day)+3) as opposing_id_same_3day_rate,
        sum(t1.opposing_id_same_3day)/count(t1.opposing_id_same_3day) as opposing_id_same_3day_rate_1,

        sum(t1.is_peer_pay_same_3day) as is_peer_pay_same_3day_nb,
        count(t1.is_peer_pay_same_3day) as is_peer_pay_same_3day_all_nb,     
        sum(t1.is_peer_pay_same_3day)/(count(t1.is_peer_pay_same_3day)+3) as is_peer_pay_same_3day_rate,
        sum(t1.is_peer_pay_same_3day)/count(t1.is_peer_pay_same_3day) as is_peer_pay_same_3day_rate_1,

        if (sum(t1.ip_same_3day)=0, 1, 0) as ip_same_3day_is_first,
        if (sum(t1.network_same_3day)=0, 1, 0) as network_same_3day_is_first,
        if (sum(t1.device_sign_same_3day)=0, 1, 0) as device_sign_same_3day_is_first,
        if (sum(t1.info_1_same_3day)=0, 1, 0) as info_1_same_3day_is_first,
        if (sum(t1.info_2_same_3day)=0, 1, 0) as info_2_same_3day_is_first,
        if (sum(t1.ip_prov_same_3day)=0, 1, 0) as ip_prov_same_3day_is_first,
        if (sum(t1.ip_city_same_3day)=0, 1, 0) as ip_city_same_3day_is_first,
        if (sum(t1.cert_prov_same_3day)=0, 1, 0) as cert_prov_same_3day_is_first,
        if (sum(t1.is_one_people_same_3day)=0, 1, 0) as is_one_people_same_3day_is_first,
        if (sum(t1.mobile_oper_platform_same_3day)=0, 1, 0) as mobile_oper_platform_same_3day_is_first,
        if (sum(t1.operation_channel_same_3day)=0, 1, 0) as operation_channel_same_3day_is_first,
        if (sum(t1.pay_scene_same_3day)=0, 1, 0) as pay_scene_same_3day_is_first,
        if (sum(t1.amt_same_3day)=0, 1, 0) as amt_same_3day_is_first,
        if (sum(t1.opposing_id_same_3day)=0, 1, 0) as opposing_id_same_3day_is_first,
        if (sum(t1.is_peer_pay_same_3day)=0, 1, 0) as is_peer_pay_same_3day_is_first


    from
    (select event_id, 
        --------------------------------
        client_ip2,
        network2,
        device_sign2,
        info_12,
        info_22,
        ip_prov2,
        ip_city2,
        cert_prov2,
        is_one_people2,
        mobile_oper_platform2,
        operation_channel2,
        pay_scene2,
        amt2,
        opposing_id2,
        is_peer_pay2,
        ---------------------------------------------------
        int(client_ip=client_ip2) as ip_same_3day,
        int(network=network2) as network_same_3day,
        int(device_sign=device_sign2) as device_sign_same_3day,
        int(info_1=info_12) as info_1_same_3day,
        int(info_2=info_22) as info_2_same_3day,
        int(ip_prov=ip_prov2) as ip_prov_same_3day,
        int(ip_city=ip_city2) as ip_city_same_3day,
        int(cert_prov=cert_prov2) as cert_prov_same_3day,
        int(is_one_people=is_one_people2) as is_one_people_same_3day,
        int(mobile_oper_platform=mobile_oper_platform2) as mobile_oper_platform_same_3day,
        int(operation_channel=operation_channel2) as operation_channel_same_3day,
        int(pay_scene=pay_scene2) as pay_scene_same_3day,
        int(amt=amt2) as amt_same_3day,
        int(opposing_id=opposing_id2) as opposing_id_same_3day,
        int(is_peer_pay=is_peer_pay2) as is_peer_pay_same_3day

 
    from zp_join_all 
        where datediff(gmt_occur, gmt_occur2, 'hh')<72 and datediff(gmt_occur, gmt_occur2, 'hh')>=1)t1
    group by t1.event_id) t2;
------------------------------------------------------------7day----------------------------------------------------
drop table if exists zp_new_window_feature_7day;
create table if not exists zp_new_window_feature_7day as
select 
	t2.event_id,
    -----------------------------------
	t2.ip_same_7day_nb,
    t2.ip_same_7day_all_nb,
    t2.ip_same_7day_rate,
    t2.ip_same_7day_is_first,
    
	t2.network_same_7day_nb,
    t2.network_same_7day_all_nb,
    t2.network_same_7day_rate,    
    t2.network_same_7day_is_first,

	t2.device_sign_same_7day_nb,
    t2.device_sign_same_7day_all_nb,
    t2.device_sign_same_7day_rate,
    t2.device_sign_same_7day_is_first,
    
	t2.info_1_same_7day_nb,
    t2.info_1_same_7day_all_nb,
    t2.info_1_same_7day_rate,    
    t2.info_1_same_7day_is_first,
    
	t2.info_2_same_7day_nb,
    t2.info_2_same_7day_all_nb,
    t2.info_2_same_7day_rate,    
    t2.info_2_same_7day_is_first,
    
	t2.ip_prov_same_7day_nb,
    t2.ip_prov_same_7day_all_nb,
    t2.ip_prov_same_7day_rate,    
    t2.ip_prov_same_7day_is_first,
    
	t2.ip_city_same_7day_nb,
    t2.ip_city_same_7day_all_nb,
    t2.ip_city_same_7day_rate,    
    t2.ip_city_same_7day_is_first,
    
	t2.cert_prov_same_7day_nb,
    t2.cert_prov_same_7day_all_nb,
    t2.cert_prov_same_7day_rate,    
    t2.cert_prov_same_7day_is_first,
    
	t2.is_one_people_same_7day_nb,
    t2.is_one_people_same_7day_all_nb,
    t2.is_one_people_same_7day_rate,    
    t2.is_one_people_same_7day_is_first,
    
	t2.mobile_oper_platform_same_7day_nb,
    t2.mobile_oper_platform_same_7day_all_nb,
    t2.mobile_oper_platform_same_7day_rate,    
    t2.mobile_oper_platform_same_7day_is_first,
    
	t2.operation_channel_same_7day_nb,
    t2.operation_channel_same_7day_all_nb,
    t2.operation_channel_same_7day_rate,    
    t2.operation_channel_same_7day_is_first,
    
	t2.pay_scene_same_7day_nb,
    t2.pay_scene_same_7day_all_nb,
    t2.pay_scene_same_7day_rate,    
    t2.pay_scene_same_7day_is_first,
    
	t2.amt_same_7day_nb,
    t2.amt_same_7day_all_nb,
    t2.amt_same_7day_rate,    
    t2.amt_same_7day_is_first,

	t2.opposing_id_same_7day_nb,
    t2.opposing_id_same_7day_all_nb,
    t2.opposing_id_same_7day_rate, 
    t2.opposing_id_same_7day_is_first,
    
	t2.is_peer_pay_same_7day_nb,
    t2.is_peer_pay_same_7day_all_nb,
    t2.is_peer_pay_same_7day_rate,    
    t2.is_peer_pay_same_7day_is_first,
    ------------------------------------
    t2.ip_nb as ip_distinct_7day,
    t2.network_nb as network_distinct_7day,
    t2.device_sign_nb as device_sign_distinct_7day,
    t2.info_1_nb as info_1_distinct_7day,
    t2.info_2_nb as info_2_distinct_7day,
    t2.ip_prov_nb as ip_prov_distinct_7day,
    t2.ip_city_nb as ip_city_distinct_7day,
    t2.cert_prov_nb as cert_prov_distinct_7day,
    t2.is_one_people_nb as is_one_people_distinct_7day,
    t2.mobile_oper_platform_nb as mobile_oper_platform_distinct_7day,
    t2.operation_channel_nb as operation_channel_distinct_7day,
    t2.pay_scene_nb as pay_scene_distinct_7day,
    t2.amt_nb as amt_distinct_7day,
    t2.opposing_id_nb as opposing_id_distinct_7day,
    t2.is_peer_pay_nb as is_peer_pay_distinct_7day,
    --------------------------------------------------------------------
    1.0/(t2.ip_nb) - t2.ip_same_7day_rate_1 as ip_7day_distance_rate,
    1.0/(t2.network_nb) - t2.network_same_7day_rate_1 as network_7day_distance_rate,
    1.0/(t2.device_sign_nb) - t2.device_sign_same_7day_rate_1 as device_sign_7day_distance_rate,
    1.0/(t2.info_1_nb) - t2.info_1_same_7day_rate_1 as info_1_7day_distance_rate,
    1.0/(t2.info_2_nb) - t2.info_2_same_7day_rate_1 as info_2_7day_distance_rate,
    1.0/(t2.ip_prov_nb) - t2.ip_prov_same_7day_rate_1 as ip_prov_7day_distance_rate,
    1.0/(t2.ip_city_nb) - t2.ip_city_same_7day_rate_1 as ip_city_7day_distance_rate,
    1.0/(t2.cert_prov_nb) - t2.cert_prov_same_7day_rate_1 as cert_prov_7day_distance_rate,
    1.0/(t2.is_one_people_nb) - t2.is_one_people_same_7day_rate_1 as is_one_people_7day_distance_rate,
    1.0/(t2.mobile_oper_platform_nb) - t2.mobile_oper_platform_same_7day_rate_1 as mobile_oper_platform_7day_distance_rate,
    1.0/(t2.operation_channel_nb) - t2.operation_channel_same_7day_rate_1 as operation_channel_7day_distance_rate,
    1.0/(t2.pay_scene_nb) - t2.pay_scene_same_7day_rate_1 as pay_scene_7day_distance_rate,
    1.0/(t2.amt_nb) - t2.amt_same_7day_rate_1 as amt_7day_distance_rate,
    1.0/(t2.opposing_id_nb) - t2.opposing_id_same_7day_rate_1 as opposing_id_7day_distance_rate,
    1.0/(t2.is_peer_pay_nb) - t2.is_peer_pay_same_7day_rate_1 as is_peer_pay_7day_distance_rate
    

from
    (select t1.event_id as event_id, 

        count(distinct t1.client_ip2) as ip_nb,
        count(distinct t1.network2) as network_nb,
        count(distinct t1.device_sign2) as device_sign_nb,
        count(distinct t1.info_12) as info_1_nb,
        count(distinct t1.info_22) as info_2_nb,
        count(distinct t1.ip_prov2) as ip_prov_nb,
        count(distinct t1.ip_city2) as ip_city_nb,
        count(distinct t1.cert_prov2) as cert_prov_nb,
        count(distinct t1.is_one_people2) as is_one_people_nb,
        count(distinct t1.mobile_oper_platform2) as mobile_oper_platform_nb,
        count(distinct t1.operation_channel2) as operation_channel_nb,
        count(distinct t1.pay_scene2) as pay_scene_nb,
        count(distinct t1.amt2) as amt_nb,
        count(distinct t1.opposing_id2) as opposing_id_nb,
        count(distinct t1.is_peer_pay2) as is_peer_pay_nb,

        sum(t1.ip_same_7day) as ip_same_7day_nb,
        count(t1.ip_same_7day) as ip_same_7day_all_nb,
        sum(t1.ip_same_7day)/(count(t1.ip_same_7day)+3) as ip_same_7day_rate,
        sum(t1.ip_same_7day)/count(t1.ip_same_7day) as ip_same_7day_rate_1,


        sum(t1.network_same_7day) as network_same_7day_nb,
        count(t1.network_same_7day) as network_same_7day_all_nb,
        sum(t1.network_same_7day)/(count(t1.network_same_7day)+3) as network_same_7day_rate,
     	sum(t1.network_same_7day)/count(t1.network_same_7day) as network_same_7day_rate_1,


        sum(t1.device_sign_same_7day) as device_sign_same_7day_nb,
        count(t1.device_sign_same_7day) as device_sign_same_7day_all_nb,    
        sum(t1.device_sign_same_7day)/(count(t1.device_sign_same_7day)+3) as device_sign_same_7day_rate,
        sum(t1.device_sign_same_7day)/count(t1.device_sign_same_7day) as device_sign_same_7day_rate_1,

        sum(t1.info_1_same_7day) as info_1_same_7day_nb,
        count(t1.info_1_same_7day) as info_1_same_7day_all_nb,   
        sum(t1.info_1_same_7day)/(count(t1.info_1_same_7day)+3) as info_1_same_7day_rate,
        sum(t1.info_1_same_7day)/count(t1.info_1_same_7day) as info_1_same_7day_rate_1,
     
        sum(t1.info_2_same_7day) as info_2_same_7day_nb,
        count(t1.info_2_same_7day) as info_2_same_7day_all_nb,     
        sum(t1.info_2_same_7day)/(count(t1.info_2_same_7day)+3) as info_2_same_7day_rate,
        sum(t1.info_2_same_7day)/count(t1.info_2_same_7day) as info_2_same_7day_rate_1,

        sum(t1.ip_prov_same_7day) as ip_prov_same_7day_nb,
        count(t1.ip_prov_same_7day) as ip_prov_same_7day_all_nb,      
        sum(t1.ip_prov_same_7day)/(count(t1.ip_prov_same_7day)+3) as ip_prov_same_7day_rate,
        sum(t1.ip_prov_same_7day)/count(t1.ip_prov_same_7day) as ip_prov_same_7day_rate_1,

        sum(t1.ip_city_same_7day) as ip_city_same_7day_nb,
        count(t1.ip_city_same_7day) as ip_city_same_7day_all_nb,      
        sum(t1.ip_city_same_7day)/(count(t1.ip_city_same_7day)+3) as ip_city_same_7day_rate,
        sum(t1.ip_city_same_7day)/count(t1.ip_city_same_7day) as ip_city_same_7day_rate_1,

        sum(t1.cert_prov_same_7day) as cert_prov_same_7day_nb,
        count(t1.cert_prov_same_7day) as cert_prov_same_7day_all_nb,     
        sum(t1.cert_prov_same_7day)/(count(t1.cert_prov_same_7day)+3) as cert_prov_same_7day_rate,
        sum(t1.cert_prov_same_7day)/count(t1.cert_prov_same_7day) as cert_prov_same_7day_rate_1,

        sum(t1.is_one_people_same_7day) as is_one_people_same_7day_nb,
        count(t1.is_one_people_same_7day) as is_one_people_same_7day_all_nb,    	
        sum(t1.is_one_people_same_7day)/(count(t1.is_one_people_same_7day)+3) as is_one_people_same_7day_rate,
        sum(t1.is_one_people_same_7day)/count(t1.is_one_people_same_7day) as is_one_people_same_7day_rate_1,

        sum(t1.mobile_oper_platform_same_7day) as mobile_oper_platform_same_7day_nb,
        count(t1.mobile_oper_platform_same_7day) as mobile_oper_platform_same_7day_all_nb,    	   
        sum(t1.mobile_oper_platform_same_7day)/(count(t1.mobile_oper_platform_same_7day)+3) as mobile_oper_platform_same_7day_rate,
        sum(t1.mobile_oper_platform_same_7day)/count(t1.mobile_oper_platform_same_7day) as mobile_oper_platform_same_7day_rate_1,

        sum(t1.operation_channel_same_7day) as operation_channel_same_7day_nb,
        count(t1.operation_channel_same_7day) as operation_channel_same_7day_all_nb,     
        sum(t1.operation_channel_same_7day)/(count(t1.operation_channel_same_7day)+3) as operation_channel_same_7day_rate,
        sum(t1.operation_channel_same_7day)/count(t1.operation_channel_same_7day) as operation_channel_same_7day_rate_1,

        sum(t1.pay_scene_same_7day) as pay_scene_same_7day_nb,
        count(t1.pay_scene_same_7day) as pay_scene_same_7day_all_nb,     
        sum(t1.pay_scene_same_7day)/(count(t1.pay_scene_same_7day)+3) as pay_scene_same_7day_rate,
        sum(t1.pay_scene_same_7day)/count(t1.pay_scene_same_7day) as pay_scene_same_7day_rate_1,

        sum(t1.amt_same_7day) as amt_same_7day_nb,
        count(t1.amt_same_7day) as amt_same_7day_all_nb,     
        sum(t1.amt_same_7day)/(count(t1.amt_same_7day)+3) as amt_same_7day_rate,
        sum(t1.amt_same_7day)/count(t1.amt_same_7day) as amt_same_7day_rate_1,

        sum(t1.opposing_id_same_7day) as opposing_id_same_7day_nb,
        count(t1.opposing_id_same_7day) as opposing_id_same_7day_all_nb,     
        sum(t1.opposing_id_same_7day)/(count(t1.opposing_id_same_7day)+3) as opposing_id_same_7day_rate,
        sum(t1.opposing_id_same_7day)/count(t1.opposing_id_same_7day) as opposing_id_same_7day_rate_1,

        sum(t1.is_peer_pay_same_7day) as is_peer_pay_same_7day_nb,
        count(t1.is_peer_pay_same_7day) as is_peer_pay_same_7day_all_nb,     
        sum(t1.is_peer_pay_same_7day)/(count(t1.is_peer_pay_same_7day)+3) as is_peer_pay_same_7day_rate,
        sum(t1.is_peer_pay_same_7day)/count(t1.is_peer_pay_same_7day) as is_peer_pay_same_7day_rate_1,

        if (sum(t1.ip_same_7day)=0, 1, 0) as ip_same_7day_is_first,
        if (sum(t1.network_same_7day)=0, 1, 0) as network_same_7day_is_first,
        if (sum(t1.device_sign_same_7day)=0, 1, 0) as device_sign_same_7day_is_first,
        if (sum(t1.info_1_same_7day)=0, 1, 0) as info_1_same_7day_is_first,
        if (sum(t1.info_2_same_7day)=0, 1, 0) as info_2_same_7day_is_first,
        if (sum(t1.ip_prov_same_7day)=0, 1, 0) as ip_prov_same_7day_is_first,
        if (sum(t1.ip_city_same_7day)=0, 1, 0) as ip_city_same_7day_is_first,
        if (sum(t1.cert_prov_same_7day)=0, 1, 0) as cert_prov_same_7day_is_first,
        if (sum(t1.is_one_people_same_7day)=0, 1, 0) as is_one_people_same_7day_is_first,
        if (sum(t1.mobile_oper_platform_same_7day)=0, 1, 0) as mobile_oper_platform_same_7day_is_first,
        if (sum(t1.operation_channel_same_7day)=0, 1, 0) as operation_channel_same_7day_is_first,
        if (sum(t1.pay_scene_same_7day)=0, 1, 0) as pay_scene_same_7day_is_first,
        if (sum(t1.amt_same_7day)=0, 1, 0) as amt_same_7day_is_first,
        if (sum(t1.opposing_id_same_7day)=0, 1, 0) as opposing_id_same_7day_is_first,
        if (sum(t1.is_peer_pay_same_7day)=0, 1, 0) as is_peer_pay_same_7day_is_first


    from
    (select event_id, 
        --------------------------------
        client_ip2,
        network2,
        device_sign2,
        info_12,
        info_22,
        ip_prov2,
        ip_city2,
        cert_prov2,
        is_one_people2,
        mobile_oper_platform2,
        operation_channel2,
        pay_scene2,
        amt2,
        opposing_id2,
        is_peer_pay2,
        ---------------------------------------------------
        int(client_ip=client_ip2) as ip_same_7day,
        int(network=network2) as network_same_7day,
        int(device_sign=device_sign2) as device_sign_same_7day,
        int(info_1=info_12) as info_1_same_7day,
        int(info_2=info_22) as info_2_same_7day,
        int(ip_prov=ip_prov2) as ip_prov_same_7day,
        int(ip_city=ip_city2) as ip_city_same_7day,
        int(cert_prov=cert_prov2) as cert_prov_same_7day,
        int(is_one_people=is_one_people2) as is_one_people_same_7day,
        int(mobile_oper_platform=mobile_oper_platform2) as mobile_oper_platform_same_7day,
        int(operation_channel=operation_channel2) as operation_channel_same_7day,
        int(pay_scene=pay_scene2) as pay_scene_same_7day,
        int(amt=amt2) as amt_same_7day,
        int(opposing_id=opposing_id2) as opposing_id_same_7day,
        int(is_peer_pay=is_peer_pay2) as is_peer_pay_same_7day

 
    from zp_join_all 
        where datediff(gmt_occur, gmt_occur2, 'hh')<168 and datediff(gmt_occur, gmt_occur2, 'hh')>=1)t1
    group by t1.event_id) t2;
-----------------------------------------------------------all_day--------------------------------------------------
drop table if exists zp_new_window_feature_all_day;
create table if not exists zp_new_window_feature_all_day as
select 
	t2.event_id,
    -----------------------------------
	t2.ip_same_all_day_nb,
    t2.ip_same_all_day_all_nb,
    t2.ip_same_all_day_rate,
    t2.ip_same_all_day_is_first,
    
	t2.network_same_all_day_nb,
    t2.network_same_all_day_all_nb,
    t2.network_same_all_day_rate,    
    t2.network_same_all_day_is_first,

	t2.device_sign_same_all_day_nb,
    t2.device_sign_same_all_day_all_nb,
    t2.device_sign_same_all_day_rate,
    t2.device_sign_same_all_day_is_first,
    
	t2.info_1_same_all_day_nb,
    t2.info_1_same_all_day_all_nb,
    t2.info_1_same_all_day_rate,    
    t2.info_1_same_all_day_is_first,
    
	t2.info_2_same_all_day_nb,
    t2.info_2_same_all_day_all_nb,
    t2.info_2_same_all_day_rate,    
    t2.info_2_same_all_day_is_first,
    
	t2.ip_prov_same_all_day_nb,
    t2.ip_prov_same_all_day_all_nb,
    t2.ip_prov_same_all_day_rate,    
    t2.ip_prov_same_all_day_is_first,
    
	t2.ip_city_same_all_day_nb,
    t2.ip_city_same_all_day_all_nb,
    t2.ip_city_same_all_day_rate,    
    t2.ip_city_same_all_day_is_first,
    
	t2.cert_prov_same_all_day_nb,
    t2.cert_prov_same_all_day_all_nb,
    t2.cert_prov_same_all_day_rate,    
    t2.cert_prov_same_all_day_is_first,
    
	t2.is_one_people_same_all_day_nb,
    t2.is_one_people_same_all_day_all_nb,
    t2.is_one_people_same_all_day_rate,    
    t2.is_one_people_same_all_day_is_first,
    
	t2.mobile_oper_platform_same_all_day_nb,
    t2.mobile_oper_platform_same_all_day_all_nb,
    t2.mobile_oper_platform_same_all_day_rate,    
    t2.mobile_oper_platform_same_all_day_is_first,
    
	t2.operation_channel_same_all_day_nb,
    t2.operation_channel_same_all_day_all_nb,
    t2.operation_channel_same_all_day_rate,    
    t2.operation_channel_same_all_day_is_first,
    
	t2.pay_scene_same_all_day_nb,
    t2.pay_scene_same_all_day_all_nb,
    t2.pay_scene_same_all_day_rate,    
    t2.pay_scene_same_all_day_is_first,
    
	t2.amt_same_all_day_nb,
    t2.amt_same_all_day_all_nb,
    t2.amt_same_all_day_rate,    
    t2.amt_same_all_day_is_first,

	t2.opposing_id_same_all_day_nb,
    t2.opposing_id_same_all_day_all_nb,
    t2.opposing_id_same_all_day_rate, 
    t2.opposing_id_same_all_day_is_first,
    
	t2.is_peer_pay_same_all_day_nb,
    t2.is_peer_pay_same_all_day_all_nb,
    t2.is_peer_pay_same_all_day_rate,    
    t2.is_peer_pay_same_all_day_is_first,
    ---------------------------------------
    t2.ip_nb as ip_distinct_all_day,
    t2.network_nb as network_distinct_all_day,
    t2.device_sign_nb as device_sign_distinct_all_day,
    t2.info_1_nb as info_1_distinct_all_day,
    t2.info_2_nb as info_2_distinct_all_day,
    t2.ip_prov_nb as ip_prov_distinct_all_day,
    t2.ip_city_nb as ip_city_distinct_all_day,
    t2.cert_prov_nb as cert_prov_distinct_all_day,
    t2.is_one_people_nb as is_one_people_distinct_all_day,
    t2.mobile_oper_platform_nb as mobile_oper_platform_distinct_all_day,
    t2.operation_channel_nb as operation_channel_distinct_all_day,
    t2.pay_scene_nb as pay_scene_distinct_all_day,
    t2.amt_nb as amt_distinct_all_day,
    t2.opposing_id_nb as opposing_id_distinct_all_day,
    t2.is_peer_pay_nb as is_peer_pay_distinct_all_day,
    --------------------------------------------------------------------
    1.0/(t2.ip_nb) - t2.ip_same_all_day_rate_1 as ip_all_day_distance_rate,
    1.0/(t2.network_nb) - t2.network_same_all_day_rate_1 as network_all_day_distance_rate,
    1.0/(t2.device_sign_nb) - t2.device_sign_same_all_day_rate_1 as device_sign_all_day_distance_rate,
    1.0/(t2.info_1_nb) - t2.info_1_same_all_day_rate_1 as info_1_all_day_distance_rate,
    1.0/(t2.info_2_nb) - t2.info_2_same_all_day_rate_1 as info_2_all_day_distance_rate,
    1.0/(t2.ip_prov_nb) - t2.ip_prov_same_all_day_rate_1 as ip_prov_all_day_distance_rate,
    1.0/(t2.ip_city_nb) - t2.ip_city_same_all_day_rate_1 as ip_city_all_day_distance_rate,
    1.0/(t2.cert_prov_nb) - t2.cert_prov_same_all_day_rate_1 as cert_prov_all_day_distance_rate,
    1.0/(t2.is_one_people_nb) - t2.is_one_people_same_all_day_rate_1 as is_one_people_all_day_distance_rate,
    1.0/(t2.mobile_oper_platform_nb) - t2.mobile_oper_platform_same_all_day_rate_1 as mobile_oper_platform_all_day_distance_rate,
    1.0/(t2.operation_channel_nb) - t2.operation_channel_same_all_day_rate_1 as operation_channel_all_day_distance_rate,
    1.0/(t2.pay_scene_nb) - t2.pay_scene_same_all_day_rate_1 as pay_scene_all_day_distance_rate,
    1.0/(t2.amt_nb) - t2.amt_same_all_day_rate_1 as amt_all_day_distance_rate,
    1.0/(t2.opposing_id_nb) - t2.opposing_id_same_all_day_rate_1 as opposing_id_all_day_distance_rate,
    1.0/(t2.is_peer_pay_nb) - t2.is_peer_pay_same_all_day_rate_1 as is_peer_pay_all_day_distance_rate
    

from
    (select t1.event_id as event_id, 

        count(distinct t1.client_ip2) as ip_nb,
        count(distinct t1.network2) as network_nb,
        count(distinct t1.device_sign2) as device_sign_nb,
        count(distinct t1.info_12) as info_1_nb,
        count(distinct t1.info_22) as info_2_nb,
        count(distinct t1.ip_prov2) as ip_prov_nb,
        count(distinct t1.ip_city2) as ip_city_nb,
        count(distinct t1.cert_prov2) as cert_prov_nb,
        count(distinct t1.is_one_people2) as is_one_people_nb,
        count(distinct t1.mobile_oper_platform2) as mobile_oper_platform_nb,
        count(distinct t1.operation_channel2) as operation_channel_nb,
        count(distinct t1.pay_scene2) as pay_scene_nb,
        count(distinct t1.amt2) as amt_nb,
        count(distinct t1.opposing_id2) as opposing_id_nb,
        count(distinct t1.is_peer_pay2) as is_peer_pay_nb,

        sum(t1.ip_same_all_day) as ip_same_all_day_nb,
        count(t1.ip_same_all_day) as ip_same_all_day_all_nb,
        sum(t1.ip_same_all_day)/(count(t1.ip_same_all_day)+3) as ip_same_all_day_rate,
        sum(t1.ip_same_all_day)/count(t1.ip_same_all_day) as ip_same_all_day_rate_1,


        sum(t1.network_same_all_day) as network_same_all_day_nb,
        count(t1.network_same_all_day) as network_same_all_day_all_nb,
        sum(t1.network_same_all_day)/(count(t1.network_same_all_day)+3) as network_same_all_day_rate,
     	sum(t1.network_same_all_day)/count(t1.network_same_all_day) as network_same_all_day_rate_1,


        sum(t1.device_sign_same_all_day) as device_sign_same_all_day_nb,
        count(t1.device_sign_same_all_day) as device_sign_same_all_day_all_nb,    
        sum(t1.device_sign_same_all_day)/(count(t1.device_sign_same_all_day)+3) as device_sign_same_all_day_rate,
        sum(t1.device_sign_same_all_day)/count(t1.device_sign_same_all_day) as device_sign_same_all_day_rate_1,

        sum(t1.info_1_same_all_day) as info_1_same_all_day_nb,
        count(t1.info_1_same_all_day) as info_1_same_all_day_all_nb,   
        sum(t1.info_1_same_all_day)/(count(t1.info_1_same_all_day)+3) as info_1_same_all_day_rate,
        sum(t1.info_1_same_all_day)/count(t1.info_1_same_all_day) as info_1_same_all_day_rate_1,
     
        sum(t1.info_2_same_all_day) as info_2_same_all_day_nb,
        count(t1.info_2_same_all_day) as info_2_same_all_day_all_nb,     
        sum(t1.info_2_same_all_day)/(count(t1.info_2_same_all_day)+3) as info_2_same_all_day_rate,
        sum(t1.info_2_same_all_day)/count(t1.info_2_same_all_day) as info_2_same_all_day_rate_1,

        sum(t1.ip_prov_same_all_day) as ip_prov_same_all_day_nb,
        count(t1.ip_prov_same_all_day) as ip_prov_same_all_day_all_nb,      
        sum(t1.ip_prov_same_all_day)/(count(t1.ip_prov_same_all_day)+3) as ip_prov_same_all_day_rate,
        sum(t1.ip_prov_same_all_day)/count(t1.ip_prov_same_all_day) as ip_prov_same_all_day_rate_1,

        sum(t1.ip_city_same_all_day) as ip_city_same_all_day_nb,
        count(t1.ip_city_same_all_day) as ip_city_same_all_day_all_nb,      
        sum(t1.ip_city_same_all_day)/(count(t1.ip_city_same_all_day)+3) as ip_city_same_all_day_rate,
        sum(t1.ip_city_same_all_day)/count(t1.ip_city_same_all_day) as ip_city_same_all_day_rate_1,

        sum(t1.cert_prov_same_all_day) as cert_prov_same_all_day_nb,
        count(t1.cert_prov_same_all_day) as cert_prov_same_all_day_all_nb,     
        sum(t1.cert_prov_same_all_day)/(count(t1.cert_prov_same_all_day)+3) as cert_prov_same_all_day_rate,
        sum(t1.cert_prov_same_all_day)/count(t1.cert_prov_same_all_day) as cert_prov_same_all_day_rate_1,

        sum(t1.is_one_people_same_all_day) as is_one_people_same_all_day_nb,
        count(t1.is_one_people_same_all_day) as is_one_people_same_all_day_all_nb,    	
        sum(t1.is_one_people_same_all_day)/(count(t1.is_one_people_same_all_day)+3) as is_one_people_same_all_day_rate,
        sum(t1.is_one_people_same_all_day)/count(t1.is_one_people_same_all_day) as is_one_people_same_all_day_rate_1,

        sum(t1.mobile_oper_platform_same_all_day) as mobile_oper_platform_same_all_day_nb,
        count(t1.mobile_oper_platform_same_all_day) as mobile_oper_platform_same_all_day_all_nb,    	   
        sum(t1.mobile_oper_platform_same_all_day)/(count(t1.mobile_oper_platform_same_all_day)+3) as mobile_oper_platform_same_all_day_rate,
        sum(t1.mobile_oper_platform_same_all_day)/count(t1.mobile_oper_platform_same_all_day) as mobile_oper_platform_same_all_day_rate_1,

        sum(t1.operation_channel_same_all_day) as operation_channel_same_all_day_nb,
        count(t1.operation_channel_same_all_day) as operation_channel_same_all_day_all_nb,     
        sum(t1.operation_channel_same_all_day)/(count(t1.operation_channel_same_all_day)+3) as operation_channel_same_all_day_rate,
        sum(t1.operation_channel_same_all_day)/count(t1.operation_channel_same_all_day) as operation_channel_same_all_day_rate_1,

        sum(t1.pay_scene_same_all_day) as pay_scene_same_all_day_nb,
        count(t1.pay_scene_same_all_day) as pay_scene_same_all_day_all_nb,     
        sum(t1.pay_scene_same_all_day)/(count(t1.pay_scene_same_all_day)+3) as pay_scene_same_all_day_rate,
        sum(t1.pay_scene_same_all_day)/count(t1.pay_scene_same_all_day) as pay_scene_same_all_day_rate_1,

        sum(t1.amt_same_all_day) as amt_same_all_day_nb,
        count(t1.amt_same_all_day) as amt_same_all_day_all_nb,     
        sum(t1.amt_same_all_day)/(count(t1.amt_same_all_day)+3) as amt_same_all_day_rate,
        sum(t1.amt_same_all_day)/count(t1.amt_same_all_day) as amt_same_all_day_rate_1,

        sum(t1.opposing_id_same_all_day) as opposing_id_same_all_day_nb,
        count(t1.opposing_id_same_all_day) as opposing_id_same_all_day_all_nb,     
        sum(t1.opposing_id_same_all_day)/(count(t1.opposing_id_same_all_day)+3) as opposing_id_same_all_day_rate,
        sum(t1.opposing_id_same_all_day)/count(t1.opposing_id_same_all_day) as opposing_id_same_all_day_rate_1,

        sum(t1.is_peer_pay_same_all_day) as is_peer_pay_same_all_day_nb,
        count(t1.is_peer_pay_same_all_day) as is_peer_pay_same_all_day_all_nb,     
        sum(t1.is_peer_pay_same_all_day)/(count(t1.is_peer_pay_same_all_day)+3) as is_peer_pay_same_all_day_rate,
        sum(t1.is_peer_pay_same_all_day)/count(t1.is_peer_pay_same_all_day) as is_peer_pay_same_all_day_rate_1,

        if (sum(t1.ip_same_all_day)=0, 1, 0) as ip_same_all_day_is_first,
        if (sum(t1.network_same_all_day)=0, 1, 0) as network_same_all_day_is_first,
        if (sum(t1.device_sign_same_all_day)=0, 1, 0) as device_sign_same_all_day_is_first,
        if (sum(t1.info_1_same_all_day)=0, 1, 0) as info_1_same_all_day_is_first,
        if (sum(t1.info_2_same_all_day)=0, 1, 0) as info_2_same_all_day_is_first,
        if (sum(t1.ip_prov_same_all_day)=0, 1, 0) as ip_prov_same_all_day_is_first,
        if (sum(t1.ip_city_same_all_day)=0, 1, 0) as ip_city_same_all_day_is_first,
        if (sum(t1.cert_prov_same_all_day)=0, 1, 0) as cert_prov_same_all_day_is_first,
        if (sum(t1.is_one_people_same_all_day)=0, 1, 0) as is_one_people_same_all_day_is_first,
        if (sum(t1.mobile_oper_platform_same_all_day)=0, 1, 0) as mobile_oper_platform_same_all_day_is_first,
        if (sum(t1.operation_channel_same_all_day)=0, 1, 0) as operation_channel_same_all_day_is_first,
        if (sum(t1.pay_scene_same_all_day)=0, 1, 0) as pay_scene_same_all_day_is_first,
        if (sum(t1.amt_same_all_day)=0, 1, 0) as amt_same_all_day_is_first,
        if (sum(t1.opposing_id_same_all_day)=0, 1, 0) as opposing_id_same_all_day_is_first,
        if (sum(t1.is_peer_pay_same_all_day)=0, 1, 0) as is_peer_pay_same_all_day_is_first


    from
    (select event_id, 
        --------------------------------
        client_ip2,
        network2,
        device_sign2,
        info_12,
        info_22,
        ip_prov2,
        ip_city2,
        cert_prov2,
        is_one_people2,
        mobile_oper_platform2,
        operation_channel2,
        pay_scene2,
        amt2,
        opposing_id2,
        is_peer_pay2,
        ---------------------------------------------------
        int(client_ip=client_ip2) as ip_same_all_day,
        int(network=network2) as network_same_all_day,
        int(device_sign=device_sign2) as device_sign_same_all_day,
        int(info_1=info_12) as info_1_same_all_day,
        int(info_2=info_22) as info_2_same_all_day,
        int(ip_prov=ip_prov2) as ip_prov_same_all_day,
        int(ip_city=ip_city2) as ip_city_same_all_day,
        int(cert_prov=cert_prov2) as cert_prov_same_all_day,
        int(is_one_people=is_one_people2) as is_one_people_same_all_day,
        int(mobile_oper_platform=mobile_oper_platform2) as mobile_oper_platform_same_all_day,
        int(operation_channel=operation_channel2) as operation_channel_same_all_day,
        int(pay_scene=pay_scene2) as pay_scene_same_all_day,
        int(amt=amt2) as amt_same_all_day,
        int(opposing_id=opposing_id2) as opposing_id_same_all_day,
        int(is_peer_pay=is_peer_pay2) as is_peer_pay_same_all_day

    from zp_join_all 
        where datediff(gmt_occur, gmt_occur2, 'hh')>=1)t1
    group by t1.event_id) t2;



