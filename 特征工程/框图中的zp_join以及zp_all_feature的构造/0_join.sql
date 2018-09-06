drop table zp_join;
create table if not exists zp_join as
select
	  t1.event_id 
      ,t1.user_id 
      ,t1.gmt_occur 
      ,t1.client_ip 
      ,t1.network 
      ,t1.device_sign 
      ,t1.info_1 
      ,t1.info_2 
      ,t1.ip_prov 
      ,t1.ip_city 
      ,t1.cert_prov 
      ,t1.cert_city 
      ,t1.card_bin_prov 
      ,t1.card_bin_city 
      ,t1.card_mobile_prov 
      ,t1.card_mobile_city 
      ,t1.card_cert_prov 
      ,t1.card_cert_city 
      ,t1.is_one_people 
      ,t1.mobile_oper_platform 
      ,t1.operation_channel 
      ,t1.pay_scene 
      ,t1.amt 
      ,t1.card_cert_no 
      ,t1.opposing_id 
      ,t1.income_card_no 
      ,t1.income_card_cert_no 
      ,t1.income_card_mobile 
      ,t1.income_card_bank_code 
      ,t1.province 
      ,t1.city 
      ,t1.is_peer_pay 
      ,t1.version 
      ,t1.dt
      ,t1.is_fraud
---------------------
      ,t2.gmt_occur as gmt_occur2
      ,t2.client_ip as client_ip2
      ,t2.network as network2
      ,t2.device_sign as device_sign2
      ,t2.info_1 as info_12
      ,t2.info_2 as info_22
      ,t2.ip_prov as ip_prov2
      ,t2.ip_city as ip_city2
      ,t2.cert_prov as cert_prov2
      ,t2.cert_city as cert_city2
      ,t2.card_bin_prov as card_bin_prov2
      ,t2.card_bin_city as card_bin_city2
      ,t2.card_mobile_prov as card_mobile_prov2
      ,t2.card_mobile_city as card_mobile_city2
      ,t2.card_cert_prov as card_cert_prov2
      ,t2.card_cert_city as card_cert_city2
      ,t2.is_one_people as is_one_people2
      ,t2.mobile_oper_platform as mobile_oper_platform2
      ,t2.operation_channel as operation_channel2
      ,t2.pay_scene as pay_scene2
      ,t2.amt as amt2
      ,t2.card_cert_no as card_cert_no2 
      ,t2.opposing_id as opposing_id2
      ,t2.income_card_no as income_card_no2
      ,t2.income_card_cert_no as income_card_cert_no2
      ,t2.income_card_mobile as income_card_mobile2
      ,t2.income_card_bank_code as income_card_bank_code2
      ,t2.province as province2
      ,t2.city as city2
      ,t2.is_peer_pay as is_peer_pay2
      ,t2.version as version2
      ,t2.dt as dt2
      ,t2.is_fraud as is_fraud2


from zp_train_test t1 join zp_train_test t2 on t1.user_id = t2.user_id 
where t1.gmt_occur >= t2.gmt_occur
;

drop table if exists zp_join_na;
create table if not exists zp_join_na as
select
	event_id,
    user_id,
    to_date(gmt_occur, 'yyyy-mm-dd hh') as gmt_occur,
	case when client_ip is null then 'NA' else client_ip end as client_ip,
	case when network is null then 'NA' else network end as network,
	case when device_sign is null then 'NA' else device_sign end as device_sign,
	case when info_1 is null then 'NA' else info_1 end as info_1,
	case when info_2 is null then 'NA' else info_2 end as info_2,
	case when ip_prov is null then 'NA' else ip_prov end as ip_prov,
	case when ip_city is null then 'NA' else ip_city end as ip_city,
	case when cert_prov is null then 'NA' else cert_prov end as cert_prov,
	case when cert_city is null then 'NA' else cert_city end as cert_city,
	case when card_bin_prov is null then 'NA' else card_bin_prov end as card_bin_prov,
	case when card_bin_city is null then 'NA' else card_bin_city end as card_bin_city,
	case when card_mobile_prov is null then 'NA' else card_mobile_prov end as card_mobile_prov,
	case when card_mobile_city is null then 'NA' else card_mobile_city end as card_mobile_city,
	case when card_cert_prov is null then 'NA' else card_cert_prov end as card_cert_prov,
	case when card_cert_city is null then 'NA' else card_cert_city end as card_cert_city,
	case when is_one_people is null then 'NA' else is_one_people end as is_one_people,
	case when mobile_oper_platform is null then 'NA' else mobile_oper_platform end as mobile_oper_platform,
	case when operation_channel is null then 'NA' else operation_channel end as operation_channel,
	case when pay_scene is null then 'NA' else pay_scene end as pay_scene,
	case when amt is null then 0 else amt end as amt,
	case when card_cert_no is null then 'NA' else card_cert_no end as card_cert_no,
	case when opposing_id is null then 'NA' else opposing_id end as opposing_id,
	case when income_card_no is null then 'NA' else income_card_no end as income_card_no,
	case when income_card_cert_no is null then 'NA' else income_card_cert_no end as income_card_cert_no,
	case when income_card_mobile is null then 'NA' else income_card_mobile end as income_card_mobile,
	case when income_card_bank_code is null then 'NA' else income_card_bank_code end as income_card_bank_code,
    case when province is null then 'NA' else province end as province,
	case when city is null then 'NA' else city end as city,
	case when is_peer_pay is null then 'NA' else is_peer_pay end as is_peer_pay,
	case when version is null then 'NA' else version end as version,
	case when dt is null then 'NA' else dt end as dt,
    is_fraud,
---------------------
   
    to_date(gmt_occur2, 'yyyy-mm-dd hh') as gmt_occur2,
	case when client_ip2 is null then 'NA' else client_ip2 end as client_ip2,
	case when network2 is null then 'NA' else network2 end as network2,
	case when device_sign2 is null then 'NA' else device_sign2 end as device_sign2,
	case when info_12 is null then 'NA' else info_12 end as info_12,
	case when info_22 is null then 'NA' else info_22 end as info_22,
	case when ip_prov2 is null then 'NA' else ip_prov2 end as ip_prov2,
	case when ip_city2 is null then 'NA' else ip_city2 end as ip_city2,
	case when cert_prov2 is null then 'NA' else cert_prov2 end as cert_prov2,
	case when cert_city2 is null then 'NA' else cert_city2 end as cert_city2,
	case when card_bin_prov2 is null then 'NA' else card_bin_prov2 end as card_bin_prov2,
	case when card_bin_city2 is null then 'NA' else card_bin_city2 end as card_bin_city2,
	case when card_mobile_prov2 is null then 'NA' else card_mobile_prov2 end as card_mobile_prov2,
	case when card_mobile_city2 is null then 'NA' else card_mobile_city2 end as card_mobile_city2,
	case when card_cert_prov2 is null then 'NA' else card_cert_prov2 end as card_cert_prov2,
	case when card_cert_city2 is null then 'NA' else card_cert_city2 end as card_cert_city2,
	case when is_one_people2 is null then 'NA' else is_one_people2 end as is_one_people2,
	case when mobile_oper_platform2 is null then 'NA' else mobile_oper_platform2 end as mobile_oper_platform2,
	case when operation_channel2 is null then 'NA' else operation_channel2 end as operation_channel2,
	case when pay_scene2 is null then 'NA' else pay_scene2 end as pay_scene2,
	case when amt2 is null then 0 else amt2 end as amt2,
	case when card_cert_no2 is null then 'NA' else card_cert_no2 end as card_cert_no2,
	case when opposing_id2 is null then 'NA' else opposing_id2 end as opposing_id2,
	case when income_card_no2 is null then 'NA' else income_card_no2 end as income_card_no2,
	case when income_card_cert_no2 is null then 'NA' else income_card_cert_no2 end as income_card_cert_no2,
	case when income_card_mobile2 is null then 'NA' else income_card_mobile2 end as income_card_mobile2,
	case when income_card_bank_code2 is null then 'NA' else income_card_bank_code2 end as income_card_bank_code2,
    case when province2 is null then 'NA' else province2 end as province2,
	case when city2 is null then 'NA' else city2 end as city2,
	case when is_peer_pay2 is null then 'NA' else is_peer_pay2 end as is_peer_pay2,
	case when version2 is null then 'NA' else version2 end as version2,
	case when dt2 is null then 'NA' else dt2 end as dt2,
    is_fraud2

from zp_join;