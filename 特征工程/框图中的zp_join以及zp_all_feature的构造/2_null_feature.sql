-- 骗子的信息缺失特征，信息越不全信用越低
drop table if exists zp_income_null_feature;
create table zp_income_null_feature as
select 
	event_id,
	income_card_no_null,
    income_card_cert_no_null,
    income_card_mobile_null,
    income_card_bank_code_null,
    province_null,
    city_null,
    income_card_no_null+income_card_cert_no_null+
    income_card_mobile_null+income_card_bank_code_null+province_null+city_null as income_null_nb
from
    (select
     	event_id,
        case when income_card_no is null then 1 else 0 end as income_card_no_null,
        case when income_card_cert_no is null then 1 else 0 end as income_card_cert_no_null,
        case when income_card_mobile is null then 1 else 0 end as income_card_mobile_null,
        case when income_card_bank_code is null then 1 else 0 end as income_card_bank_code_null,
        case when province is null then 1 else 0 end as province_null,
        case when city is null then 1 else 0 end as city_null
        from zp_train_test)t;
