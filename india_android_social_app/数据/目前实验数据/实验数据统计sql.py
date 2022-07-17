sql=f"""
--{"day1": "20220608","hour1": "03","day2": "20220608","hour2": "03","ctit_limit": "24","pkgs": "'in.mohalla.video','in.mohalla.sharechat'"}  
SELECT  
    b.offer_pkg as offer_pkg ,b.slot as slot ,label,device_count,install,click,round((install*10000.00/click),2)crx1w 
from  
    (
        SELECT 
            slot,offer_pkg,count(*)install 
        from 
            real_conversion 
        where 
            toYYYYMMDDhhmmss ( utctime ) >= '{{day1}}{{hour1}}0000'  
            and attr_time >='{{day1}}{{hour1}}' -- 限定点击时间
            and attr_time <='{{day2}}{{hour2}}' -- 限定点击时间
            and datediff(hour,toDateTime(concat(substring(attr_time,1,4),'-',substring(attr_time,5,2),'-',substring(attr_time,7,2),' ',substring(attr_time,9,2),':00:00')), toDateTime(concat(substring(time_slot,1,4),'-',substring(time_slot,5,2),'-',substring(time_slot,7,2),' ',substring(time_slot,9,2),':00:00')) )<={{ctit_limit}}  -- 限定点击到转化的时间 
            and offer_pkg in ({{pkgs}}) 
            and event_type='1' 
            and adtype='19' 
        group by real
            slot,offer_pkg
    )a 
    right JOIN   
    (
        SELECT 
            slot,offer_pkg,sum(clk_count)click 
        from real_buzz  
        where 
            time_slot >='{{day1}}{{hour1}}' 
            and time_slot <='{{day2}}{{hour2}}'   
            and offer_pkg in({{pkgs}}) 
        group by 
            slot,offer_pkg
    )b 
    on 
        ( a.slot=b.slot and a.offer_pkg=b.offer_pkg )  
    left join 
    (
        SELECT 
            day,slot_id as slot,label,sum(device_count)device_count 
        from 
            fenix_device 
        where 
            day ='{{day1}}' 
        group by 
            day,slot,label 
    )c 
    on 
        ( b.slot=c.slot ) 
    order by offer_pkg,crx1w desc


"""