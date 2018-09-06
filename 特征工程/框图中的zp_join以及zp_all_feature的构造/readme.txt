0_join.sql
· 根据user_id进行join操作，便于后面提取防止穿越的特征，另外填充了缺失值，并将时间转化为datatime格式

1_time_amt_feature.sql
· 时间特征提取，例如一周第几天，是否为凌晨等等
· 金额特征提取，与用户历史金额的平均值、最大最小值的对比特征	

2_null_feature.sql
· 收款方字段的信息不全度，信息越不全信用越低

3_nowhour_feature.sql
· 短时间（一个小时）内ip的省市是否有大的变化

4_window_feature.sql
· 滑窗特征    4个窗口分别为1天、3天、7天、所有天
		窗口内有4种类别的特征
        	每一类又有15个字段
                1：ip_same_1day_nb		1天窗口内与此次事件的ip相同的次数
                2：ip_same_1day_all_nb		1天窗口内所有事件的次数
                3：ip_same_1day_rate		1除以2得到，分母+3平滑
                4：ip_same_1day_is_first		1天窗口内此次事件的ip是否是第一次出现
· 防止穿越，这里用到了0_join.sql生成的表，利用where将日期控制为此次event的之前多长时间

5_sence_combine_feature.sql
· 场景组合特征
    -----------------------------
    此特征主要是将一些实际场景结合起来，
    1.  例如手机操作系统和手机设备：
    	试想，一个人一直用Android换了手机一般还是会延续习惯买Android手机，所以两者都变了很有可能是欺诈
    --------------------------    
    2.  ip市和网络类型：同上
    -------------------------
    3.  支付渠道、支付场景和是否代付：同上，一个正常人会延续习惯
    ---------------------------
    特征如何表达出来：
    	从两个方面去表达，一个是是否改变，也就是is_first，另一个方面是曾经出现的rate
        mobile_scene_1day_first：如果手机设备和手机操作系统都变了此特征为1
    	mobile_scene_1day_rate： 
        	mobile_oper_platform_same_1day_rate+device_sign_same_1day_rate=此特征的值，越小越可能欺诈
6_new_window_feature.sql
· 细化滑窗特征  增加3个窗口分别为1小时、5小时、12小时， 特征与4中描述一致

7_new_sence_combine_feature.sql
· 细化场景组合特征，此特征跟随6中描述生成

最后，利用pai组件进行特征的汇总，生成all_feature