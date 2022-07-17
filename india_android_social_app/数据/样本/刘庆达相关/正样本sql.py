sql=f"""
select 
    a.*
    ,case when c.label like '%2' then '2'  when c.label like '%1' then '1' else '0' end int_tag
    ,case when c.label like '%w%' then '0'  when c.label like '%x%' then '1' when c.label like '%y%' then '3' when c.label like '%z%' then '5' else '0' end source_tag
    ,b.category_name maincategory ,b.subcategory_name subcategory,b.approx_size_bytes size 
from 
    (
        (select device_id,pkg from sdk_device)m
        left join
        (
            select 
                ifa device_id
                , bundles user_bundle_list
                ,geo country
                ,osversion os_ver_name
                ,lang language
                ,bundles_size install_bundle_size
                ,bundle
                ,lastactiveday
            from
            (
                select 
                    ifa
                    ,bundles
                    ,'IND' geo
                    ,osversion
                    ,lang
                    ,size(bundles) bundles_size
                    ,concat(bundles[size(bundles)-1],',',bundles[size(bundles)-2],',',bundles[size(bundles)-3]) bund
                    ,lastactiveday
                from 
                    source_tag 
                where
                    lastactiveday >= '${week}'
                    and
                    ( array_contains(bundles,'com.next.innovation.takatak') or array_contains(bundles,'in.mohalla.video') or array_contains(bundles,'in.mohalla.sharechat') )
            ) 
            lateral view 
                explode(split(bund,',')) t as bundle 
        ) a
        on 
            m.device_id = a.device_id
        left join
        (select id,category_name,subcategory_name ,approx_size_bytes from app_info )b
        on a.bundle = b.id
        left join
        (select  device,max(label) label from fuyu_in group by device) c
        on a.device_id = c.device
    ) 
    where 
        c.label != '' and a.device_id != ""
"""