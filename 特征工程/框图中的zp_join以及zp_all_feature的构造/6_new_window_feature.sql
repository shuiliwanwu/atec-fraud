------------------------------------------------------------1hour---------------------------------------------------
drop table if exists zp_new_window_feature_1hour;
create table if not exists zp_new_window_feature_1hour as
select 
	t2.event_id,
    -----------------------------------
	t2.ip_same_1hour_nb,
    t2.ip_same_1hour_all_nb,
    t2.ip_same_1hour_rate,
    t2.ip_same_1hour_is_first,
    
	t2.network_same_1hour_nb,
    t2.network_same_1hour_all_nb,
    t2.network_same_1hour_rate,    
    t2.network_same_1hour_is_first,

	t2.device_sign_same_1hour_nb,
    t2.device_sign_same_1hour_all_nb,
    t2.device_sign_same_1hour_rate,
    t2.device_sign_same_1hour_is_first,
    
	t2.info_1_same_1hour_nb,
    t2.info_1_same_1hour_all_nb,
    t2.info_1_same_1hour_rate,    
    t2.info_1_same_1hour_is_first,
    
	t2.info_2_same_1hour_nb,
    t2.info_2_same_1hour_all_nb,
    t2.info_2_same_1hour_rate,    
    t2.info_2_same_1hour_is_first,
    
	t2.ip_prov_same_1hour_nb,
    t2.ip_prov_same_1hour_all_nb,
    t2.ip_prov_same_1hour_rate,    
    t2.ip_prov_same_1hour_is_first,
    
	t2.ip_city_same_1hour_nb,
    t2.ip_city_same_1hour_all_nb,
    t2.ip_city_same_1hour_rate,    
    t2.ip_city_same_1hour_is_first,
    
	t2.cert_prov_same_1hour_nb,
    t2.cert_prov_same_1hour_all_nb,
    t2.cert_prov_same_1hour_rate,    
    t2.cert_prov_same_1hour_is_first,
    
	t2.is_one_people_same_1hour_nb,
    t2.is_one_people_same_1hour_all_nb,
    t2.is_one_people_same_1hour_rate,    
    t2.is_one_people_same_1hour_is_first,
    
	t2.mobile_oper_platform_same_1hour_nb,
    t2.mobile_oper_platform_same_1hour_all_nb,
    t2.mobile_oper_platform_same_1hour_rate,    
    t2.mobile_oper_platform_same_1hour_is_first,
    
	t2.operation_channel_same_1hour_nb,
    t2.operation_channel_same_1hour_all_nb,
    t2.operation_channel_same_1hour_rate,    
    t2.operation_channel_same_1hour_is_first,
    
	t2.pay_scene_same_1hour_nb,
    t2.pay_scene_same_1hour_all_nb,
    t2.pay_scene_same_1hour_rate,    
    t2.pay_scene_same_1hour_is_first,
    
	t2.amt_same_1hour_nb,
    t2.amt_same_1hour_all_nb,
    t2.amt_same_1hour_rate,    
    t2.amt_same_1hour_is_first,

	t2.opposing_id_same_1hour_nb,
    t2.opposing_id_same_1hour_all_nb,
    t2.opposing_id_same_1hour_rate, 
    t2.opposing_id_same_1hour_is_first,
    
	t2.is_peer_pay_same_1hour_nb,
    t2.is_peer_pay_same_1hour_all_nb,
    t2.is_peer_pay_same_1hour_rate,    
    t2.is_peer_pay_same_1hour_is_first,
    -------------------------------------
    t2.ip_nb as ip_distinct_1hour,
    t2.network_nb as network_distinct_1hour,
    t2.device_sign_nb as device_sign_distinct_1hour,
    t2.info_1_nb as info_1_distinct_1hour,
    t2.info_2_nb as info_2_distinct_1hour,
    t2.ip_prov_nb as ip_prov_distinct_1hour,
    t2.ip_city_nb as ip_city_distinct_1hour,
    t2.cert_prov_nb as cert_prov_distinct_1hour,
    t2.is_one_people_nb as is_one_people_distinct_1hour,
    t2.mobile_oper_platform_nb as mobile_oper_platform_distinct_1hour,
    t2.operation_channel_nb as operation_channel_distinct_1hour,
    t2.pay_scene_nb as pay_scene_distinct_1hour,
    t2.amt_nb as amt_distinct_1hour,
    t2.opposing_id_nb as opposing_id_distinct_1hour,
    t2.is_peer_pay_nb as is_peer_pay_distinct_1hour,
    --------------------------------------------------------------------
    1.0/(t2.ip_nb) - t2.ip_same_1hour_rate_1 as ip_1hour_distance_rate,
    1.0/(t2.network_nb) - t2.network_same_1hour_rate_1 as network_1hour_distance_rate,
    1.0/(t2.device_sign_nb) - t2.device_sign_same_1hour_rate_1 as device_sign_1hour_distance_rate,
    1.0/(t2.info_1_nb) - t2.info_1_same_1hour_rate_1 as info_1_1hour_distance_rate,
    1.0/(t2.info_2_nb) - t2.info_2_same_1hour_rate_1 as info_2_1hour_distance_rate,
    1.0/(t2.ip_prov_nb) - t2.ip_prov_same_1hour_rate_1 as ip_prov_1hour_distance_rate,
    1.0/(t2.ip_city_nb) - t2.ip_city_same_1hour_rate_1 as ip_city_1hour_distance_rate,
    1.0/(t2.cert_prov_nb) - t2.cert_prov_same_1hour_rate_1 as cert_prov_1hour_distance_rate,
    1.0/(t2.is_one_people_nb) - t2.is_one_people_same_1hour_rate_1 as is_one_people_1hour_distance_rate,
    1.0/(t2.mobile_oper_platform_nb) - t2.mobile_oper_platform_same_1hour_rate_1 as mobile_oper_platform_1hour_distance_rate,
    1.0/(t2.operation_channel_nb) - t2.operation_channel_same_1hour_rate_1 as operation_channel_1hour_distance_rate,
    1.0/(t2.pay_scene_nb) - t2.pay_scene_same_1hour_rate_1 as pay_scene_1hour_distance_rate,
    1.0/(t2.amt_nb) - t2.amt_same_1hour_rate_1 as amt_1hour_distance_rate,
    1.0/(t2.opposing_id_nb) - t2.opposing_id_same_1hour_rate_1 as opposing_id_1hour_distance_rate,
    1.0/(t2.is_peer_pay_nb) - t2.is_peer_pay_same_1hour_rate_1 as is_peer_pay_1hour_distance_rate
    

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

        sum(t1.ip_same_1hour) as ip_same_1hour_nb,
        count(t1.ip_same_1hour) as ip_same_1hour_all_nb,
        sum(t1.ip_same_1hour)/(count(t1.ip_same_1hour)+3) as ip_same_1hour_rate,
        sum(t1.ip_same_1hour)/count(t1.ip_same_1hour) as ip_same_1hour_rate_1,


        sum(t1.network_same_1hour) as network_same_1hour_nb,
        count(t1.network_same_1hour) as network_same_1hour_all_nb,
        sum(t1.network_same_1hour)/(count(t1.network_same_1hour)+3) as network_same_1hour_rate,
     	sum(t1.network_same_1hour)/count(t1.network_same_1hour) as network_same_1hour_rate_1,


        sum(t1.device_sign_same_1hour) as device_sign_same_1hour_nb,
        count(t1.device_sign_same_1hour) as device_sign_same_1hour_all_nb,    
        sum(t1.device_sign_same_1hour)/(count(t1.device_sign_same_1hour)+3) as device_sign_same_1hour_rate,
        sum(t1.device_sign_same_1hour)/count(t1.device_sign_same_1hour) as device_sign_same_1hour_rate_1,

        sum(t1.info_1_same_1hour) as info_1_same_1hour_nb,
        count(t1.info_1_same_1hour) as info_1_same_1hour_all_nb,   
        sum(t1.info_1_same_1hour)/(count(t1.info_1_same_1hour)+3) as info_1_same_1hour_rate,
        sum(t1.info_1_same_1hour)/count(t1.info_1_same_1hour) as info_1_same_1hour_rate_1,
     
        sum(t1.info_2_same_1hour) as info_2_same_1hour_nb,
        count(t1.info_2_same_1hour) as info_2_same_1hour_all_nb,     
        sum(t1.info_2_same_1hour)/(count(t1.info_2_same_1hour)+3) as info_2_same_1hour_rate,
        sum(t1.info_2_same_1hour)/count(t1.info_2_same_1hour) as info_2_same_1hour_rate_1,

        sum(t1.ip_prov_same_1hour) as ip_prov_same_1hour_nb,
        count(t1.ip_prov_same_1hour) as ip_prov_same_1hour_all_nb,      
        sum(t1.ip_prov_same_1hour)/(count(t1.ip_prov_same_1hour)+3) as ip_prov_same_1hour_rate,
        sum(t1.ip_prov_same_1hour)/count(t1.ip_prov_same_1hour) as ip_prov_same_1hour_rate_1,

        sum(t1.ip_city_same_1hour) as ip_city_same_1hour_nb,
        count(t1.ip_city_same_1hour) as ip_city_same_1hour_all_nb,      
        sum(t1.ip_city_same_1hour)/(count(t1.ip_city_same_1hour)+3) as ip_city_same_1hour_rate,
        sum(t1.ip_city_same_1hour)/count(t1.ip_city_same_1hour) as ip_city_same_1hour_rate_1,

        sum(t1.cert_prov_same_1hour) as cert_prov_same_1hour_nb,
        count(t1.cert_prov_same_1hour) as cert_prov_same_1hour_all_nb,     
        sum(t1.cert_prov_same_1hour)/(count(t1.cert_prov_same_1hour)+3) as cert_prov_same_1hour_rate,
        sum(t1.cert_prov_same_1hour)/count(t1.cert_prov_same_1hour) as cert_prov_same_1hour_rate_1,

        sum(t1.is_one_people_same_1hour) as is_one_people_same_1hour_nb,
        count(t1.is_one_people_same_1hour) as is_one_people_same_1hour_all_nb,    	
        sum(t1.is_one_people_same_1hour)/(count(t1.is_one_people_same_1hour)+3) as is_one_people_same_1hour_rate,
        sum(t1.is_one_people_same_1hour)/count(t1.is_one_people_same_1hour) as is_one_people_same_1hour_rate_1,

        sum(t1.mobile_oper_platform_same_1hour) as mobile_oper_platform_same_1hour_nb,
        count(t1.mobile_oper_platform_same_1hour) as mobile_oper_platform_same_1hour_all_nb,    	   
        sum(t1.mobile_oper_platform_same_1hour)/(count(t1.mobile_oper_platform_same_1hour)+3) as mobile_oper_platform_same_1hour_rate,
        sum(t1.mobile_oper_platform_same_1hour)/count(t1.mobile_oper_platform_same_1hour) as mobile_oper_platform_same_1hour_rate_1,

        sum(t1.operation_channel_same_1hour) as operation_channel_same_1hour_nb,
        count(t1.operation_channel_same_1hour) as operation_channel_same_1hour_all_nb,     
        sum(t1.operation_channel_same_1hour)/(count(t1.operation_channel_same_1hour)+3) as operation_channel_same_1hour_rate,
        sum(t1.operation_channel_same_1hour)/count(t1.operation_channel_same_1hour) as operation_channel_same_1hour_rate_1,

        sum(t1.pay_scene_same_1hour) as pay_scene_same_1hour_nb,
        count(t1.pay_scene_same_1hour) as pay_scene_same_1hour_all_nb,     
        sum(t1.pay_scene_same_1hour)/(count(t1.pay_scene_same_1hour)+3) as pay_scene_same_1hour_rate,
        sum(t1.pay_scene_same_1hour)/count(t1.pay_scene_same_1hour) as pay_scene_same_1hour_rate_1,

        sum(t1.amt_same_1hour) as amt_same_1hour_nb,
        count(t1.amt_same_1hour) as amt_same_1hour_all_nb,     
        sum(t1.amt_same_1hour)/(count(t1.amt_same_1hour)+3) as amt_same_1hour_rate,
        sum(t1.amt_same_1hour)/count(t1.amt_same_1hour) as amt_same_1hour_rate_1,

        sum(t1.opposing_id_same_1hour) as opposing_id_same_1hour_nb,
        count(t1.opposing_id_same_1hour) as opposing_id_same_1hour_all_nb,     
        sum(t1.opposing_id_same_1hour)/(count(t1.opposing_id_same_1hour)+3) as opposing_id_same_1hour_rate,
        sum(t1.opposing_id_same_1hour)/count(t1.opposing_id_same_1hour) as opposing_id_same_1hour_rate_1,

        sum(t1.is_peer_pay_same_1hour) as is_peer_pay_same_1hour_nb,
        count(t1.is_peer_pay_same_1hour) as is_peer_pay_same_1hour_all_nb,     
        sum(t1.is_peer_pay_same_1hour)/(count(t1.is_peer_pay_same_1hour)+3) as is_peer_pay_same_1hour_rate,
        sum(t1.is_peer_pay_same_1hour)/count(t1.is_peer_pay_same_1hour) as is_peer_pay_same_1hour_rate_1,

        if (sum(t1.ip_same_1hour)=0, 1, 0) as ip_same_1hour_is_first,
        if (sum(t1.network_same_1hour)=0, 1, 0) as network_same_1hour_is_first,
        if (sum(t1.device_sign_same_1hour)=0, 1, 0) as device_sign_same_1hour_is_first,
        if (sum(t1.info_1_same_1hour)=0, 1, 0) as info_1_same_1hour_is_first,
        if (sum(t1.info_2_same_1hour)=0, 1, 0) as info_2_same_1hour_is_first,
        if (sum(t1.ip_prov_same_1hour)=0, 1, 0) as ip_prov_same_1hour_is_first,
        if (sum(t1.ip_city_same_1hour)=0, 1, 0) as ip_city_same_1hour_is_first,
        if (sum(t1.cert_prov_same_1hour)=0, 1, 0) as cert_prov_same_1hour_is_first,
        if (sum(t1.is_one_people_same_1hour)=0, 1, 0) as is_one_people_same_1hour_is_first,
        if (sum(t1.mobile_oper_platform_same_1hour)=0, 1, 0) as mobile_oper_platform_same_1hour_is_first,
        if (sum(t1.operation_channel_same_1hour)=0, 1, 0) as operation_channel_same_1hour_is_first,
        if (sum(t1.pay_scene_same_1hour)=0, 1, 0) as pay_scene_same_1hour_is_first,
        if (sum(t1.amt_same_1hour)=0, 1, 0) as amt_same_1hour_is_first,
        if (sum(t1.opposing_id_same_1hour)=0, 1, 0) as opposing_id_same_1hour_is_first,
        if (sum(t1.is_peer_pay_same_1hour)=0, 1, 0) as is_peer_pay_same_1hour_is_first


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
        int(client_ip=client_ip2) as ip_same_1hour,
        int(network=network2) as network_same_1hour,
        int(device_sign=device_sign2) as device_sign_same_1hour,
        int(info_1=info_12) as info_1_same_1hour,
        int(info_2=info_22) as info_2_same_1hour,
        int(ip_prov=ip_prov2) as ip_prov_same_1hour,
        int(ip_city=ip_city2) as ip_city_same_1hour,
        int(cert_prov=cert_prov2) as cert_prov_same_1hour,
        int(is_one_people=is_one_people2) as is_one_people_same_1hour,
        int(mobile_oper_platform=mobile_oper_platform2) as mobile_oper_platform_same_1hour,
        int(operation_channel=operation_channel2) as operation_channel_same_1hour,
        int(pay_scene=pay_scene2) as pay_scene_same_1hour,
        int(amt=amt2) as amt_same_1hour,
        int(opposing_id=opposing_id2) as opposing_id_same_1hour,
        int(is_peer_pay=is_peer_pay2) as is_peer_pay_same_1hour

    from zp_join_all 
        where datediff(gmt_occur, gmt_occur2, 'hh')=1 )t1
    group by t1.event_id) t2;

------------------------------------------------------------5hour---------------------------------------------------
drop table if exists zp_new_window_feature_5hour;
create table if not exists zp_new_window_feature_5hour as
select 
	t2.event_id,
    -----------------------------------
	t2.ip_same_5hour_nb,
    t2.ip_same_5hour_all_nb,
    t2.ip_same_5hour_rate,
    t2.ip_same_5hour_is_first,
    
	t2.network_same_5hour_nb,
    t2.network_same_5hour_all_nb,
    t2.network_same_5hour_rate,    
    t2.network_same_5hour_is_first,

	t2.device_sign_same_5hour_nb,
    t2.device_sign_same_5hour_all_nb,
    t2.device_sign_same_5hour_rate,
    t2.device_sign_same_5hour_is_first,
    
	t2.info_1_same_5hour_nb,
    t2.info_1_same_5hour_all_nb,
    t2.info_1_same_5hour_rate,    
    t2.info_1_same_5hour_is_first,
    
	t2.info_2_same_5hour_nb,
    t2.info_2_same_5hour_all_nb,
    t2.info_2_same_5hour_rate,    
    t2.info_2_same_5hour_is_first,
    
	t2.ip_prov_same_5hour_nb,
    t2.ip_prov_same_5hour_all_nb,
    t2.ip_prov_same_5hour_rate,    
    t2.ip_prov_same_5hour_is_first,
    
	t2.ip_city_same_5hour_nb,
    t2.ip_city_same_5hour_all_nb,
    t2.ip_city_same_5hour_rate,    
    t2.ip_city_same_5hour_is_first,
    
	t2.cert_prov_same_5hour_nb,
    t2.cert_prov_same_5hour_all_nb,
    t2.cert_prov_same_5hour_rate,    
    t2.cert_prov_same_5hour_is_first,
    
	t2.is_one_people_same_5hour_nb,
    t2.is_one_people_same_5hour_all_nb,
    t2.is_one_people_same_5hour_rate,    
    t2.is_one_people_same_5hour_is_first,
    
	t2.mobile_oper_platform_same_5hour_nb,
    t2.mobile_oper_platform_same_5hour_all_nb,
    t2.mobile_oper_platform_same_5hour_rate,    
    t2.mobile_oper_platform_same_5hour_is_first,
    
	t2.operation_channel_same_5hour_nb,
    t2.operation_channel_same_5hour_all_nb,
    t2.operation_channel_same_5hour_rate,    
    t2.operation_channel_same_5hour_is_first,
    
	t2.pay_scene_same_5hour_nb,
    t2.pay_scene_same_5hour_all_nb,
    t2.pay_scene_same_5hour_rate,    
    t2.pay_scene_same_5hour_is_first,
    
	t2.amt_same_5hour_nb,
    t2.amt_same_5hour_all_nb,
    t2.amt_same_5hour_rate,    
    t2.amt_same_5hour_is_first,

	t2.opposing_id_same_5hour_nb,
    t2.opposing_id_same_5hour_all_nb,
    t2.opposing_id_same_5hour_rate, 
    t2.opposing_id_same_5hour_is_first,
    
	t2.is_peer_pay_same_5hour_nb,
    t2.is_peer_pay_same_5hour_all_nb,
    t2.is_peer_pay_same_5hour_rate,    
    t2.is_peer_pay_same_5hour_is_first,
    --------------------------------------
    t2.ip_nb as ip_distinct_5hour,
    t2.network_nb as network_distinct_5hour,
    t2.device_sign_nb as device_sign_distinct_5hour,
    t2.info_1_nb as info_1_distinct_5hour,
    t2.info_2_nb as info_2_distinct_5hour,
    t2.ip_prov_nb as ip_prov_distinct_5hour,
    t2.ip_city_nb as ip_city_distinct_5hour,
    t2.cert_prov_nb as cert_prov_distinct_5hour,
    t2.is_one_people_nb as is_one_people_distinct_5hour,
    t2.mobile_oper_platform_nb as mobile_oper_platform_distinct_5hour,
    t2.operation_channel_nb as operation_channel_distinct_5hour,
    t2.pay_scene_nb as pay_scene_distinct_5hour,
    t2.amt_nb as amt_distinct_5hour,
    t2.opposing_id_nb as opposing_id_distinct_5hour,
    t2.is_peer_pay_nb as is_peer_pay_distinct_5hour,
    --------------------------------------------------------------------
    1.0/(t2.ip_nb) - t2.ip_same_5hour_rate_1 as ip_5hour_distance_rate,
    1.0/(t2.network_nb) - t2.network_same_5hour_rate_1 as network_5hour_distance_rate,
    1.0/(t2.device_sign_nb) - t2.device_sign_same_5hour_rate_1 as device_sign_5hour_distance_rate,
    1.0/(t2.info_1_nb) - t2.info_1_same_5hour_rate_1 as info_1_5hour_distance_rate,
    1.0/(t2.info_2_nb) - t2.info_2_same_5hour_rate_1 as info_2_5hour_distance_rate,
    1.0/(t2.ip_prov_nb) - t2.ip_prov_same_5hour_rate_1 as ip_prov_5hour_distance_rate,
    1.0/(t2.ip_city_nb) - t2.ip_city_same_5hour_rate_1 as ip_city_5hour_distance_rate,
    1.0/(t2.cert_prov_nb) - t2.cert_prov_same_5hour_rate_1 as cert_prov_5hour_distance_rate,
    1.0/(t2.is_one_people_nb) - t2.is_one_people_same_5hour_rate_1 as is_one_people_5hour_distance_rate,
    1.0/(t2.mobile_oper_platform_nb) - t2.mobile_oper_platform_same_5hour_rate_1 as mobile_oper_platform_5hour_distance_rate,
    1.0/(t2.operation_channel_nb) - t2.operation_channel_same_5hour_rate_1 as operation_channel_5hour_distance_rate,
    1.0/(t2.pay_scene_nb) - t2.pay_scene_same_5hour_rate_1 as pay_scene_5hour_distance_rate,
    1.0/(t2.amt_nb) - t2.amt_same_5hour_rate_1 as amt_5hour_distance_rate,
    1.0/(t2.opposing_id_nb) - t2.opposing_id_same_5hour_rate_1 as opposing_id_5hour_distance_rate,
    1.0/(t2.is_peer_pay_nb) - t2.is_peer_pay_same_5hour_rate_1 as is_peer_pay_5hour_distance_rate
    

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

        sum(t1.ip_same_5hour) as ip_same_5hour_nb,
        count(t1.ip_same_5hour) as ip_same_5hour_all_nb,
        sum(t1.ip_same_5hour)/(count(t1.ip_same_5hour)+3) as ip_same_5hour_rate,
        sum(t1.ip_same_5hour)/count(t1.ip_same_5hour) as ip_same_5hour_rate_1,


        sum(t1.network_same_5hour) as network_same_5hour_nb,
        count(t1.network_same_5hour) as network_same_5hour_all_nb,
        sum(t1.network_same_5hour)/(count(t1.network_same_5hour)+3) as network_same_5hour_rate,
     	sum(t1.network_same_5hour)/count(t1.network_same_5hour) as network_same_5hour_rate_1,


        sum(t1.device_sign_same_5hour) as device_sign_same_5hour_nb,
        count(t1.device_sign_same_5hour) as device_sign_same_5hour_all_nb,    
        sum(t1.device_sign_same_5hour)/(count(t1.device_sign_same_5hour)+3) as device_sign_same_5hour_rate,
        sum(t1.device_sign_same_5hour)/count(t1.device_sign_same_5hour) as device_sign_same_5hour_rate_1,

        sum(t1.info_1_same_5hour) as info_1_same_5hour_nb,
        count(t1.info_1_same_5hour) as info_1_same_5hour_all_nb,   
        sum(t1.info_1_same_5hour)/(count(t1.info_1_same_5hour)+3) as info_1_same_5hour_rate,
        sum(t1.info_1_same_5hour)/count(t1.info_1_same_5hour) as info_1_same_5hour_rate_1,
     
        sum(t1.info_2_same_5hour) as info_2_same_5hour_nb,
        count(t1.info_2_same_5hour) as info_2_same_5hour_all_nb,     
        sum(t1.info_2_same_5hour)/(count(t1.info_2_same_5hour)+3) as info_2_same_5hour_rate,
        sum(t1.info_2_same_5hour)/count(t1.info_2_same_5hour) as info_2_same_5hour_rate_1,

        sum(t1.ip_prov_same_5hour) as ip_prov_same_5hour_nb,
        count(t1.ip_prov_same_5hour) as ip_prov_same_5hour_all_nb,      
        sum(t1.ip_prov_same_5hour)/(count(t1.ip_prov_same_5hour)+3) as ip_prov_same_5hour_rate,
        sum(t1.ip_prov_same_5hour)/count(t1.ip_prov_same_5hour) as ip_prov_same_5hour_rate_1,

        sum(t1.ip_city_same_5hour) as ip_city_same_5hour_nb,
        count(t1.ip_city_same_5hour) as ip_city_same_5hour_all_nb,      
        sum(t1.ip_city_same_5hour)/(count(t1.ip_city_same_5hour)+3) as ip_city_same_5hour_rate,
        sum(t1.ip_city_same_5hour)/count(t1.ip_city_same_5hour) as ip_city_same_5hour_rate_1,

        sum(t1.cert_prov_same_5hour) as cert_prov_same_5hour_nb,
        count(t1.cert_prov_same_5hour) as cert_prov_same_5hour_all_nb,     
        sum(t1.cert_prov_same_5hour)/(count(t1.cert_prov_same_5hour)+3) as cert_prov_same_5hour_rate,
        sum(t1.cert_prov_same_5hour)/count(t1.cert_prov_same_5hour) as cert_prov_same_5hour_rate_1,

        sum(t1.is_one_people_same_5hour) as is_one_people_same_5hour_nb,
        count(t1.is_one_people_same_5hour) as is_one_people_same_5hour_all_nb,    	
        sum(t1.is_one_people_same_5hour)/(count(t1.is_one_people_same_5hour)+3) as is_one_people_same_5hour_rate,
        sum(t1.is_one_people_same_5hour)/count(t1.is_one_people_same_5hour) as is_one_people_same_5hour_rate_1,

        sum(t1.mobile_oper_platform_same_5hour) as mobile_oper_platform_same_5hour_nb,
        count(t1.mobile_oper_platform_same_5hour) as mobile_oper_platform_same_5hour_all_nb,    	   
        sum(t1.mobile_oper_platform_same_5hour)/(count(t1.mobile_oper_platform_same_5hour)+3) as mobile_oper_platform_same_5hour_rate,
        sum(t1.mobile_oper_platform_same_5hour)/count(t1.mobile_oper_platform_same_5hour) as mobile_oper_platform_same_5hour_rate_1,

        sum(t1.operation_channel_same_5hour) as operation_channel_same_5hour_nb,
        count(t1.operation_channel_same_5hour) as operation_channel_same_5hour_all_nb,     
        sum(t1.operation_channel_same_5hour)/(count(t1.operation_channel_same_5hour)+3) as operation_channel_same_5hour_rate,
        sum(t1.operation_channel_same_5hour)/count(t1.operation_channel_same_5hour) as operation_channel_same_5hour_rate_1,

        sum(t1.pay_scene_same_5hour) as pay_scene_same_5hour_nb,
        count(t1.pay_scene_same_5hour) as pay_scene_same_5hour_all_nb,     
        sum(t1.pay_scene_same_5hour)/(count(t1.pay_scene_same_5hour)+3) as pay_scene_same_5hour_rate,
        sum(t1.pay_scene_same_5hour)/count(t1.pay_scene_same_5hour) as pay_scene_same_5hour_rate_1,

        sum(t1.amt_same_5hour) as amt_same_5hour_nb,
        count(t1.amt_same_5hour) as amt_same_5hour_all_nb,     
        sum(t1.amt_same_5hour)/(count(t1.amt_same_5hour)+3) as amt_same_5hour_rate,
        sum(t1.amt_same_5hour)/count(t1.amt_same_5hour) as amt_same_5hour_rate_1,

        sum(t1.opposing_id_same_5hour) as opposing_id_same_5hour_nb,
        count(t1.opposing_id_same_5hour) as opposing_id_same_5hour_all_nb,     
        sum(t1.opposing_id_same_5hour)/(count(t1.opposing_id_same_5hour)+3) as opposing_id_same_5hour_rate,
        sum(t1.opposing_id_same_5hour)/count(t1.opposing_id_same_5hour) as opposing_id_same_5hour_rate_1,

        sum(t1.is_peer_pay_same_5hour) as is_peer_pay_same_5hour_nb,
        count(t1.is_peer_pay_same_5hour) as is_peer_pay_same_5hour_all_nb,     
        sum(t1.is_peer_pay_same_5hour)/(count(t1.is_peer_pay_same_5hour)+3) as is_peer_pay_same_5hour_rate,
        sum(t1.is_peer_pay_same_5hour)/count(t1.is_peer_pay_same_5hour) as is_peer_pay_same_5hour_rate_1,

        if (sum(t1.ip_same_5hour)=0, 1, 0) as ip_same_5hour_is_first,
        if (sum(t1.network_same_5hour)=0, 1, 0) as network_same_5hour_is_first,
        if (sum(t1.device_sign_same_5hour)=0, 1, 0) as device_sign_same_5hour_is_first,
        if (sum(t1.info_1_same_5hour)=0, 1, 0) as info_1_same_5hour_is_first,
        if (sum(t1.info_2_same_5hour)=0, 1, 0) as info_2_same_5hour_is_first,
        if (sum(t1.ip_prov_same_5hour)=0, 1, 0) as ip_prov_same_5hour_is_first,
        if (sum(t1.ip_city_same_5hour)=0, 1, 0) as ip_city_same_5hour_is_first,
        if (sum(t1.cert_prov_same_5hour)=0, 1, 0) as cert_prov_same_5hour_is_first,
        if (sum(t1.is_one_people_same_5hour)=0, 1, 0) as is_one_people_same_5hour_is_first,
        if (sum(t1.mobile_oper_platform_same_5hour)=0, 1, 0) as mobile_oper_platform_same_5hour_is_first,
        if (sum(t1.operation_channel_same_5hour)=0, 1, 0) as operation_channel_same_5hour_is_first,
        if (sum(t1.pay_scene_same_5hour)=0, 1, 0) as pay_scene_same_5hour_is_first,
        if (sum(t1.amt_same_5hour)=0, 1, 0) as amt_same_5hour_is_first,
        if (sum(t1.opposing_id_same_5hour)=0, 1, 0) as opposing_id_same_5hour_is_first,
        if (sum(t1.is_peer_pay_same_5hour)=0, 1, 0) as is_peer_pay_same_5hour_is_first


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
        int(client_ip=client_ip2) as ip_same_5hour,
        int(network=network2) as network_same_5hour,
        int(device_sign=device_sign2) as device_sign_same_5hour,
        int(info_1=info_12) as info_1_same_5hour,
        int(info_2=info_22) as info_2_same_5hour,
        int(ip_prov=ip_prov2) as ip_prov_same_5hour,
        int(ip_city=ip_city2) as ip_city_same_5hour,
        int(cert_prov=cert_prov2) as cert_prov_same_5hour,
        int(is_one_people=is_one_people2) as is_one_people_same_5hour,
        int(mobile_oper_platform=mobile_oper_platform2) as mobile_oper_platform_same_5hour,
        int(operation_channel=operation_channel2) as operation_channel_same_5hour,
        int(pay_scene=pay_scene2) as pay_scene_same_5hour,
        int(amt=amt2) as amt_same_5hour,
        int(opposing_id=opposing_id2) as opposing_id_same_5hour,
        int(is_peer_pay=is_peer_pay2) as is_peer_pay_same_5hour

    from zp_join_all 
        where datediff(gmt_occur, gmt_occur2, 'hh')<=5 and datediff(gmt_occur, gmt_occur2, 'hh')>=1)t1
    group by t1.event_id) t2;
    
------------------------------------------------------------12hour---------------------------------------------------
drop table if exists zp_new_window_feature_12hour;
create table if not exists zp_new_window_feature_12hour as
select 
	t2.event_id,
    -----------------------------------
	t2.ip_same_12hour_nb,
    t2.ip_same_12hour_all_nb,
    t2.ip_same_12hour_rate,
    t2.ip_same_12hour_is_first,
    
	t2.network_same_12hour_nb,
    t2.network_same_12hour_all_nb,
    t2.network_same_12hour_rate,    
    t2.network_same_12hour_is_first,

	t2.device_sign_same_12hour_nb,
    t2.device_sign_same_12hour_all_nb,
    t2.device_sign_same_12hour_rate,
    t2.device_sign_same_12hour_is_first,
    
	t2.info_1_same_12hour_nb,
    t2.info_1_same_12hour_all_nb,
    t2.info_1_same_12hour_rate,    
    t2.info_1_same_12hour_is_first,
    
	t2.info_2_same_12hour_nb,
    t2.info_2_same_12hour_all_nb,
    t2.info_2_same_12hour_rate,    
    t2.info_2_same_12hour_is_first,
    
	t2.ip_prov_same_12hour_nb,
    t2.ip_prov_same_12hour_all_nb,
    t2.ip_prov_same_12hour_rate,    
    t2.ip_prov_same_12hour_is_first,
    
	t2.ip_city_same_12hour_nb,
    t2.ip_city_same_12hour_all_nb,
    t2.ip_city_same_12hour_rate,    
    t2.ip_city_same_12hour_is_first,
    
	t2.cert_prov_same_12hour_nb,
    t2.cert_prov_same_12hour_all_nb,
    t2.cert_prov_same_12hour_rate,    
    t2.cert_prov_same_12hour_is_first,
    
	t2.is_one_people_same_12hour_nb,
    t2.is_one_people_same_12hour_all_nb,
    t2.is_one_people_same_12hour_rate,    
    t2.is_one_people_same_12hour_is_first,
    
	t2.mobile_oper_platform_same_12hour_nb,
    t2.mobile_oper_platform_same_12hour_all_nb,
    t2.mobile_oper_platform_same_12hour_rate,    
    t2.mobile_oper_platform_same_12hour_is_first,
    
	t2.operation_channel_same_12hour_nb,
    t2.operation_channel_same_12hour_all_nb,
    t2.operation_channel_same_12hour_rate,    
    t2.operation_channel_same_12hour_is_first,
    
	t2.pay_scene_same_12hour_nb,
    t2.pay_scene_same_12hour_all_nb,
    t2.pay_scene_same_12hour_rate,    
    t2.pay_scene_same_12hour_is_first,
    
	t2.amt_same_12hour_nb,
    t2.amt_same_12hour_all_nb,
    t2.amt_same_12hour_rate,    
    t2.amt_same_12hour_is_first,

	t2.opposing_id_same_12hour_nb,
    t2.opposing_id_same_12hour_all_nb,
    t2.opposing_id_same_12hour_rate, 
    t2.opposing_id_same_12hour_is_first,
    
	t2.is_peer_pay_same_12hour_nb,
    t2.is_peer_pay_same_12hour_all_nb,
    t2.is_peer_pay_same_12hour_rate,    
    t2.is_peer_pay_same_12hour_is_first,
    ------------------------------------
    t2.ip_nb as ip_distinct_12hour,
    t2.network_nb as network_distinct_12hour,
    t2.device_sign_nb as device_sign_distinct_12hour,
    t2.info_1_nb as info_1_distinct_12hour,
    t2.info_2_nb as info_2_distinct_12hour,
    t2.ip_prov_nb as ip_prov_distinct_12hour,
    t2.ip_city_nb as ip_city_distinct_12hour,
    t2.cert_prov_nb as cert_prov_distinct_12hour,
    t2.is_one_people_nb as is_one_people_distinct_12hour,
    t2.mobile_oper_platform_nb as mobile_oper_platform_distinct_12hour,
    t2.operation_channel_nb as operation_channel_distinct_12hour,
    t2.pay_scene_nb as pay_scene_distinct_12hour,
    t2.amt_nb as amt_distinct_12hour,
    t2.opposing_id_nb as opposing_id_distinct_12hour,
    t2.is_peer_pay_nb as is_peer_pay_distinct_12hour,
    --------------------------------------------------------------------
    1.0/(t2.ip_nb) - t2.ip_same_12hour_rate_1 as ip_12hour_distance_rate,
    1.0/(t2.network_nb) - t2.network_same_12hour_rate_1 as network_12hour_distance_rate,
    1.0/(t2.device_sign_nb) - t2.device_sign_same_12hour_rate_1 as device_sign_12hour_distance_rate,
    1.0/(t2.info_1_nb) - t2.info_1_same_12hour_rate_1 as info_1_12hour_distance_rate,
    1.0/(t2.info_2_nb) - t2.info_2_same_12hour_rate_1 as info_2_12hour_distance_rate,
    1.0/(t2.ip_prov_nb) - t2.ip_prov_same_12hour_rate_1 as ip_prov_12hour_distance_rate,
    1.0/(t2.ip_city_nb) - t2.ip_city_same_12hour_rate_1 as ip_city_12hour_distance_rate,
    1.0/(t2.cert_prov_nb) - t2.cert_prov_same_12hour_rate_1 as cert_prov_12hour_distance_rate,
    1.0/(t2.is_one_people_nb) - t2.is_one_people_same_12hour_rate_1 as is_one_people_12hour_distance_rate,
    1.0/(t2.mobile_oper_platform_nb) - t2.mobile_oper_platform_same_12hour_rate_1 as mobile_oper_platform_12hour_distance_rate,
    1.0/(t2.operation_channel_nb) - t2.operation_channel_same_12hour_rate_1 as operation_channel_12hour_distance_rate,
    1.0/(t2.pay_scene_nb) - t2.pay_scene_same_12hour_rate_1 as pay_scene_12hour_distance_rate,
    1.0/(t2.amt_nb) - t2.amt_same_12hour_rate_1 as amt_12hour_distance_rate,
    1.0/(t2.opposing_id_nb) - t2.opposing_id_same_12hour_rate_1 as opposing_id_12hour_distance_rate,
    1.0/(t2.is_peer_pay_nb) - t2.is_peer_pay_same_12hour_rate_1 as is_peer_pay_12hour_distance_rate
    

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

        sum(t1.ip_same_12hour) as ip_same_12hour_nb,
        count(t1.ip_same_12hour) as ip_same_12hour_all_nb,
        sum(t1.ip_same_12hour)/(count(t1.ip_same_12hour)+3) as ip_same_12hour_rate,
        sum(t1.ip_same_12hour)/count(t1.ip_same_12hour) as ip_same_12hour_rate_1,


        sum(t1.network_same_12hour) as network_same_12hour_nb,
        count(t1.network_same_12hour) as network_same_12hour_all_nb,
        sum(t1.network_same_12hour)/(count(t1.network_same_12hour)+3) as network_same_12hour_rate,
     	sum(t1.network_same_12hour)/count(t1.network_same_12hour) as network_same_12hour_rate_1,


        sum(t1.device_sign_same_12hour) as device_sign_same_12hour_nb,
        count(t1.device_sign_same_12hour) as device_sign_same_12hour_all_nb,    
        sum(t1.device_sign_same_12hour)/(count(t1.device_sign_same_12hour)+3) as device_sign_same_12hour_rate,
        sum(t1.device_sign_same_12hour)/count(t1.device_sign_same_12hour) as device_sign_same_12hour_rate_1,

        sum(t1.info_1_same_12hour) as info_1_same_12hour_nb,
        count(t1.info_1_same_12hour) as info_1_same_12hour_all_nb,   
        sum(t1.info_1_same_12hour)/(count(t1.info_1_same_12hour)+3) as info_1_same_12hour_rate,
        sum(t1.info_1_same_12hour)/count(t1.info_1_same_12hour) as info_1_same_12hour_rate_1,
     
        sum(t1.info_2_same_12hour) as info_2_same_12hour_nb,
        count(t1.info_2_same_12hour) as info_2_same_12hour_all_nb,     
        sum(t1.info_2_same_12hour)/(count(t1.info_2_same_12hour)+3) as info_2_same_12hour_rate,
        sum(t1.info_2_same_12hour)/count(t1.info_2_same_12hour) as info_2_same_12hour_rate_1,

        sum(t1.ip_prov_same_12hour) as ip_prov_same_12hour_nb,
        count(t1.ip_prov_same_12hour) as ip_prov_same_12hour_all_nb,      
        sum(t1.ip_prov_same_12hour)/(count(t1.ip_prov_same_12hour)+3) as ip_prov_same_12hour_rate,
        sum(t1.ip_prov_same_12hour)/count(t1.ip_prov_same_12hour) as ip_prov_same_12hour_rate_1,

        sum(t1.ip_city_same_12hour) as ip_city_same_12hour_nb,
        count(t1.ip_city_same_12hour) as ip_city_same_12hour_all_nb,      
        sum(t1.ip_city_same_12hour)/(count(t1.ip_city_same_12hour)+3) as ip_city_same_12hour_rate,
        sum(t1.ip_city_same_12hour)/count(t1.ip_city_same_12hour) as ip_city_same_12hour_rate_1,

        sum(t1.cert_prov_same_12hour) as cert_prov_same_12hour_nb,
        count(t1.cert_prov_same_12hour) as cert_prov_same_12hour_all_nb,     
        sum(t1.cert_prov_same_12hour)/(count(t1.cert_prov_same_12hour)+3) as cert_prov_same_12hour_rate,
        sum(t1.cert_prov_same_12hour)/count(t1.cert_prov_same_12hour) as cert_prov_same_12hour_rate_1,

        sum(t1.is_one_people_same_12hour) as is_one_people_same_12hour_nb,
        count(t1.is_one_people_same_12hour) as is_one_people_same_12hour_all_nb,    	
        sum(t1.is_one_people_same_12hour)/(count(t1.is_one_people_same_12hour)+3) as is_one_people_same_12hour_rate,
        sum(t1.is_one_people_same_12hour)/count(t1.is_one_people_same_12hour) as is_one_people_same_12hour_rate_1,

        sum(t1.mobile_oper_platform_same_12hour) as mobile_oper_platform_same_12hour_nb,
        count(t1.mobile_oper_platform_same_12hour) as mobile_oper_platform_same_12hour_all_nb,    	   
        sum(t1.mobile_oper_platform_same_12hour)/(count(t1.mobile_oper_platform_same_12hour)+3) as mobile_oper_platform_same_12hour_rate,
        sum(t1.mobile_oper_platform_same_12hour)/count(t1.mobile_oper_platform_same_12hour) as mobile_oper_platform_same_12hour_rate_1,

        sum(t1.operation_channel_same_12hour) as operation_channel_same_12hour_nb,
        count(t1.operation_channel_same_12hour) as operation_channel_same_12hour_all_nb,     
        sum(t1.operation_channel_same_12hour)/(count(t1.operation_channel_same_12hour)+3) as operation_channel_same_12hour_rate,
        sum(t1.operation_channel_same_12hour)/count(t1.operation_channel_same_12hour) as operation_channel_same_12hour_rate_1,

        sum(t1.pay_scene_same_12hour) as pay_scene_same_12hour_nb,
        count(t1.pay_scene_same_12hour) as pay_scene_same_12hour_all_nb,     
        sum(t1.pay_scene_same_12hour)/(count(t1.pay_scene_same_12hour)+3) as pay_scene_same_12hour_rate,
        sum(t1.pay_scene_same_12hour)/count(t1.pay_scene_same_12hour) as pay_scene_same_12hour_rate_1,

        sum(t1.amt_same_12hour) as amt_same_12hour_nb,
        count(t1.amt_same_12hour) as amt_same_12hour_all_nb,     
        sum(t1.amt_same_12hour)/(count(t1.amt_same_12hour)+3) as amt_same_12hour_rate,
        sum(t1.amt_same_12hour)/count(t1.amt_same_12hour) as amt_same_12hour_rate_1,

        sum(t1.opposing_id_same_12hour) as opposing_id_same_12hour_nb,
        count(t1.opposing_id_same_12hour) as opposing_id_same_12hour_all_nb,     
        sum(t1.opposing_id_same_12hour)/(count(t1.opposing_id_same_12hour)+3) as opposing_id_same_12hour_rate,
        sum(t1.opposing_id_same_12hour)/count(t1.opposing_id_same_12hour) as opposing_id_same_12hour_rate_1,

        sum(t1.is_peer_pay_same_12hour) as is_peer_pay_same_12hour_nb,
        count(t1.is_peer_pay_same_12hour) as is_peer_pay_same_12hour_all_nb,     
        sum(t1.is_peer_pay_same_12hour)/(count(t1.is_peer_pay_same_12hour)+3) as is_peer_pay_same_12hour_rate,
        sum(t1.is_peer_pay_same_12hour)/count(t1.is_peer_pay_same_12hour) as is_peer_pay_same_12hour_rate_1,

        if (sum(t1.ip_same_12hour)=0, 1, 0) as ip_same_12hour_is_first,
        if (sum(t1.network_same_12hour)=0, 1, 0) as network_same_12hour_is_first,
        if (sum(t1.device_sign_same_12hour)=0, 1, 0) as device_sign_same_12hour_is_first,
        if (sum(t1.info_1_same_12hour)=0, 1, 0) as info_1_same_12hour_is_first,
        if (sum(t1.info_2_same_12hour)=0, 1, 0) as info_2_same_12hour_is_first,
        if (sum(t1.ip_prov_same_12hour)=0, 1, 0) as ip_prov_same_12hour_is_first,
        if (sum(t1.ip_city_same_12hour)=0, 1, 0) as ip_city_same_12hour_is_first,
        if (sum(t1.cert_prov_same_12hour)=0, 1, 0) as cert_prov_same_12hour_is_first,
        if (sum(t1.is_one_people_same_12hour)=0, 1, 0) as is_one_people_same_12hour_is_first,
        if (sum(t1.mobile_oper_platform_same_12hour)=0, 1, 0) as mobile_oper_platform_same_12hour_is_first,
        if (sum(t1.operation_channel_same_12hour)=0, 1, 0) as operation_channel_same_12hour_is_first,
        if (sum(t1.pay_scene_same_12hour)=0, 1, 0) as pay_scene_same_12hour_is_first,
        if (sum(t1.amt_same_12hour)=0, 1, 0) as amt_same_12hour_is_first,
        if (sum(t1.opposing_id_same_12hour)=0, 1, 0) as opposing_id_same_12hour_is_first,
        if (sum(t1.is_peer_pay_same_12hour)=0, 1, 0) as is_peer_pay_same_12hour_is_first


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
        int(client_ip=client_ip2) as ip_same_12hour,
        int(network=network2) as network_same_12hour,
        int(device_sign=device_sign2) as device_sign_same_12hour,
        int(info_1=info_12) as info_1_same_12hour,
        int(info_2=info_22) as info_2_same_12hour,
        int(ip_prov=ip_prov2) as ip_prov_same_12hour,
        int(ip_city=ip_city2) as ip_city_same_12hour,
        int(cert_prov=cert_prov2) as cert_prov_same_12hour,
        int(is_one_people=is_one_people2) as is_one_people_same_12hour,
        int(mobile_oper_platform=mobile_oper_platform2) as mobile_oper_platform_same_12hour,
        int(operation_channel=operation_channel2) as operation_channel_same_12hour,
        int(pay_scene=pay_scene2) as pay_scene_same_12hour,
        int(amt=amt2) as amt_same_12hour,
        int(opposing_id=opposing_id2) as opposing_id_same_12hour,
        int(is_peer_pay=is_peer_pay2) as is_peer_pay_same_12hour

    from zp_join_all 
        where datediff(gmt_occur, gmt_occur2, 'hh')<=12 and datediff(gmt_occur, gmt_occur2, 'hh')>=1)t1
    group by t1.event_id) t2;