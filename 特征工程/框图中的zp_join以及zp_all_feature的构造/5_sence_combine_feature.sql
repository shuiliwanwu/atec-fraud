drop table if exists zp_scene_combine;
create table if not exists zp_scene_combine as	
select 
	event_id,
----------------------------------    
	case when network_same_1day_is_first=1 or ip_city_same_1day_is_first=1 then 1 else 0 end as ip_scene_1day_first,
   	case when mobile_oper_platform_same_1day_is_first=1 or device_sign_same_1day_is_first=1 then 1 else 0 end as mobile_scene_1day_first,
	case when operation_channel_same_1day_is_first=1 or pay_scene_same_1day_is_first=1 or is_peer_pay_same_1day_is_first=1 then 1 else 0 end as pay_scene_1day_first,

	(network_same_1day_is_first+
    ip_city_same_1day_is_first+
    mobile_oper_platform_same_1day_is_first+
    device_sign_same_1day_is_first+
    operation_channel_same_1day_is_first+
    pay_scene_same_1day_is_first+
    is_peer_pay_same_1day_is_first) as all_is_first_1day,
    
    network_same_1day_rate + ip_city_same_1day_rate as ip_scene_1day_first_rate,
    mobile_oper_platform_same_1day_rate + device_sign_same_1day_rate as mobile_scene_1day_rate,
    operation_channel_same_1day_rate + pay_scene_same_1day_rate + is_peer_pay_same_1day_rate as pay_scene_1day_rate,
----------------------------------
	case when network_same_3day_is_first=1 or ip_city_same_3day_is_first=1 then 1 else 0 end as ip_scene_3day_first,
   	case when mobile_oper_platform_same_3day_is_first=1 or device_sign_same_3day_is_first=1 then 1 else 0 end as mobile_scene_3day_first,
	case when operation_channel_same_3day_is_first=1 or pay_scene_same_3day_is_first=1 or is_peer_pay_same_3day_is_first=1 then 1 else 0 end as pay_scene_3day_first,
    
    (network_same_3day_is_first+
    ip_city_same_3day_is_first+
    mobile_oper_platform_same_3day_is_first+
    device_sign_same_3day_is_first+
    operation_channel_same_3day_is_first+
    pay_scene_same_3day_is_first+
    is_peer_pay_same_3day_is_first) as all_is_first_3day,
    
    network_same_3day_rate + ip_city_same_3day_rate as ip_scene_3day_first_rate,
    mobile_oper_platform_same_3day_rate + device_sign_same_3day_rate as mobile_scene_3day_rate,
    operation_channel_same_3day_rate + pay_scene_same_3day_rate + is_peer_pay_same_3day_rate as pay_scene_3day_rate,
-------------------------------------
	case when network_same_7day_is_first=1 or ip_city_same_7day_is_first=1 then 1 else 0 end as ip_scene_7day_first,
   	case when mobile_oper_platform_same_7day_is_first=1 or device_sign_same_7day_is_first=1 then 1 else 0 end as mobile_scene_7day_first,
	case when operation_channel_same_7day_is_first=1 or pay_scene_same_7day_is_first=1 or is_peer_pay_same_7day_is_first=1 then 1 else 0 end as pay_scene_7day_first,
    
    (network_same_7day_is_first+
    ip_city_same_7day_is_first+
    mobile_oper_platform_same_7day_is_first+
    device_sign_same_7day_is_first+
    operation_channel_same_7day_is_first+
    pay_scene_same_7day_is_first+
    is_peer_pay_same_7day_is_first) as all_is_first_7day,
    
    network_same_7day_rate + ip_city_same_7day_rate as ip_scene_7day_first_rate,
    mobile_oper_platform_same_7day_rate + device_sign_same_7day_rate as mobile_scene_7day_rate,
    operation_channel_same_7day_rate + pay_scene_same_7day_rate + is_peer_pay_same_7day_rate as pay_scene_7day_rate,
--------------------------------------
	case when network_same_all_day_is_first=1 or ip_city_same_all_day_is_first=1 then 1 else 0 end as ip_scene_all_day_first,
   	case when mobile_oper_platform_same_all_day_is_first=1 or device_sign_same_all_day_is_first=1 then 1 else 0 end as mobile_scene_all_day_first,
	case when operation_channel_same_all_day_is_first=1 or pay_scene_same_all_day_is_first=1 or is_peer_pay_same_all_day_is_first=1 then 1 else 0 end as pay_scene_all_day_first,
    
    (network_same_all_day_is_first+
    ip_city_same_all_day_is_first+
    mobile_oper_platform_same_all_day_is_first+
    device_sign_same_all_day_is_first+
    operation_channel_same_all_day_is_first+
    pay_scene_same_all_day_is_first+
    is_peer_pay_same_all_day_is_first) as all_is_first_all_day,
    
    network_same_all_day_rate + ip_city_same_all_day_rate as ip_scene_all_day_first_rate,
    mobile_oper_platform_same_all_day_rate + device_sign_same_all_day_rate as mobile_scene_all_day_rate,
    operation_channel_same_all_day_rate + pay_scene_same_all_day_rate + is_peer_pay_same_all_day_rate as pay_scene_all_day_rate

    from zp_f9_new_window;