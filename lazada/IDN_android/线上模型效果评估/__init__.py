"""
select
  now() as search_datetime,
  slot,
  sum(click_sync) as click_sync_all,
  sum(match_sync) as match_sync_all,
  sum(conv_sync) as conv_sync_all,
  sum(conv_sync)/sum(match_sync) * 10000 as cr_10000
from
  (
    SELECT
      a.day as day,
      a.hour as hour,
      a.country as country,
      a.channel as channel,
      a.offer_id as offer_id,
      a.slot as slot,
      (click_fenix + click_sync) as click_all,
      conv_all,
      round(
        conv_all * 10000.00 /(
          click_fenix + click_sync
        ),
        2
      ) as crx1w_all,
      click_sync,
      conv_sync,
      round(
        conv_sync * 10000.00 / click_sync ,
        2
      ) as crx1w_sync,
      click_fenix,
      conv_fenix,
      round(
        conv_fenix * 100.00 / click_fenix, 2
      ) as crx100_fenix,
      round(a.revenue, 2) as revenue,
      round(a.cost, 2) as cost,
      a.platform as platform,
      a.offer_pkg as offer_pkg,
      impression_sync,
      match_sync
    from      (
        SELECT
          substring(time_slot, 1, 8) as day,
          substring(time_slot, 9, 2) as hour,
          country,
          channel,
          offer_id,
          slot,
          count(
            case when (
              adtype in ('40', '20')
              and event_type = '2'
            ) then 1 else NULL END
          ) as conv_all,
          count(
            case when (
              adtype = '40'
              and event_type = '2'
            ) then 1 else NULL END
          ) as conv_fenix,
          count(
            case when (
              adtype = '20'
              and event_type = '2'
            ) then 1 else NULL END
          ) as conv_sync,
          sum(payout) revenue,
          sum(cost) cost,
          platform,
          offer_pkg
        from
          real_conversion
        where
          formatDateTime(utctime, '%Y%m%d%H') >= '{{dateh1}}'
          AND formatDateTime(utctime, '%Y%m%d%H') <= '{{dateh2}}'
          and offer_pkg = 'com.lazada.android'
          and adtype in ('20', '40')
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
          and length(slot)<= 8
        group by
          day,
          hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot
      ) a
      left JOIN (
        select
          substring(time_slot, 1, 8) as day,
          substring(time_slot, 9, 2) as hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot,
          sum(clk_count) click_sync,
          sum(imp_count) impression_sync,
          sum(match_count) match_sync
        from
          real_buzz_sync
        where
          time_slot >= '{{dateh1}}'
          and time_slot <= '{{dateh2}}'
          and offer_pkg = 'com.lazada.android'
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
          and length(slot)<= 8
        group by
          day,
          hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot
        union all
            select
          substring(time_slot, 1, 8) as day,
          substring(time_slot, 9, 2) as hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot,
          sum(clk_count) click_sync,
          sum(imp_count) impression_sync,
          sum(match_count) match_sync
        from
          real_buzz
        where
          time_slot >= '{{dateh1}}'
          and time_slot <= '{{dateh2}}'
          and offer_pkg = 'com.lazada.android'
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
          and length(slot)<= 8
        group by
          day,
          hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot
      ) b on (
        a.day = b.day
        and a.hour = b.hour
        and a.offer_id = b.offer_id
        and a.slot = b.slot
      )

      left JOIN (
        select
          substring(time_slot, 1, 8) as day,
          substring(time_slot, 9, 2) as hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot,
          count(*) click_fenix
        from
          real_fenix_click
        where
          time_slot >= '{{dateh1}}'
          and time_slot <= '{{dateh2}}'
          and offer_pkg = 'com.lazada.android'
          and click_through = '1'
          and adtype = '40'
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
        group by
          day,
          hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot
      ) c on (
        a.day = c.day
        and a.hour = c.hour
        and a.offer_id = c.offer_id
        and a.slot = c.slot
      )
    union all
      (
        SELECT
          b.day as day,
          b.hour as hour,
          b.country as country,
          b.channel as channel,
          b.offer_id as offer_id,
          b.slot as slot,
          (click_fenix + click_sync) as click_all,
          conv_all,
          round(
            conv_all * 10000.00 /(
              click_fenix + click_sync
            ),
            2
          ) as crx1w_all,
          click_sync ,
          conv_sync,
          round(
            conv_sync * 10000.00 / click_sync ,
            2
          ) as crx1w_sync,
          click_fenix,
          conv_fenix,
          round(
            conv_fenix * 100.00 / click_fenix, 2
          ) as crx100_fenix,
          round(a.revenue, 2) as revenue,
          round(a.cost, 2) as cost,
          b.platform as platform,
          b.offer_pkg as offer_pkg,
          impression_sync,
          match_sync
        from
          (
            SELECT
              substring(time_slot, 1, 8) as day,
              substring(time_slot, 9, 2) as hour,
              country,
              channel,
              offer_id,
              slot,
              count(
                case when (
                  adtype in ('40', '20')
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_all,
              count(
                case when (
                  adtype = '40'
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_fenix,
              count(
                case when (
                  adtype = '20'
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_sync,
              sum(payout) revenue,
              sum(cost) cost,
              platform,
              offer_pkg
            from
              real_conversion
            where
              formatDateTime(utctime, '%Y%m%d%H') >= '{{dateh1}}'
              AND formatDateTime(utctime, '%Y%m%d%H') <= '{{dateh2}}'
              and length(slot)<= 8
              and offer_pkg = 'com.lazada.android'
              and adtype in ('20', '40')
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
            group by
              day,
              hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot
          ) a
          right JOIN (
            select
              substring(time_slot, 1, 8) as day,
              substring(time_slot, 9, 2) as hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot,
              sum(clk_count) click_sync,
              sum(imp_count) impression_sync,
              sum(match_count) match_sync
            from
              real_buzz_sync
            where
              time_slot >= '{{dateh1}}'
              and time_slot <= '{{dateh2}}'
              and offer_pkg = 'com.lazada.android'
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
            group by
              day,
              hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot
              union all
            select
          substring(time_slot, 1, 8) as day,
          substring(time_slot, 9, 2) as hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot,
          sum(clk_count) click_sync,
          sum(imp_count) impression_sync,
          sum(match_count) match_sync
        from
          real_buzz
        where
          time_slot >= '{{dateh1}}'
          and time_slot <= '{{dateh2}}'
          and offer_pkg = 'com.lazada.android'
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
          and length(slot)<= 8
        group by
          day,
          hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot
          ) b on (
            a.day = b.day
            and a.hour = b.hour
            and a.offer_id = b.offer_id
            and a.slot = b.slot
          )

          left JOIN (
            select
              substring(time_slot, 1, 8) as day,
              substring(time_slot, 9, 2) as hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot,
              count(*) click_fenix
            from
              real_fenix_click
            where
              time_slot >= '{{dateh1}}'
              and time_slot <= '{{dateh2}}'
              and offer_pkg = 'com.lazada.android'
              and click_through = '1'
              and adtype = '40'
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
            group by
              day,
              hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot
          ) c on (
            b.day = c.day
            and b.hour = c.hour
            and b.offer_id = c.offer_id
            and b.slot = c.slot
          )
        where
          a.offer_id = ''
      )
    union all
      (
        SELECT
          c.day as day,
          c.hour as hour,
          c.country as country,
          c.channel as channel,
          c.offer_id as offer_id,
          c.slot as slot,
          (
            click_fenix + click_sync
          ) as click_all,
          conv_all,
          round(
            conv_all * 10000.00 /(
              click_fenix + click_sync
            ),
            2
          ) as crx1w_all,
          click_sync ,
          conv_sync,
          round(
            conv_sync * 10000.00 /(click_sync),
            2
          ) as crx1w_sync,
          click_fenix,
          conv_fenix,
          round(
            conv_fenix * 100.00 / click_fenix, 2
          ) as crx100_fenix,
          round(a.revenue, 2) as revenue,
          round(a.cost, 2) as cost,
          c.platform as platform,
          c.offer_pkg as offer_pkg,
          impression_sync,
          match_sync
        from
          (
            SELECT
              substring(time_slot, 1, 8) as day,
              substring(time_slot, 9, 2) as hour,
              country,
              channel,
              offer_id,
              slot,
              count(
                case when (
                  adtype in ('40', '20')
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_all,
              count(
                case when (
                  adtype = '40'
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_fenix,
              count(
                case when (
                  adtype = '20'
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_sync,
              sum(payout) revenue,
              sum(cost) cost,
              platform,
              offer_pkg
            from
              real_conversion
            where
              formatDateTime(utctime, '%Y%m%d%H') >= '{{dateh1}}'
              AND formatDateTime(utctime, '%Y%m%d%H') <= '{{dateh2}}'
              and offer_pkg = 'com.lazada.android'
              and adtype in ('20', '40')
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
            group by
              day,
              hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot
          ) a
          right JOIN (
            select
              substring(time_slot, 1, 8) as day,
              substring(time_slot, 9, 2) as hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot,
              count(*) click_fenix
            from
              real_fenix_click
            where
              time_slot >= '{{dateh1}}'
              and time_slot <= '{{dateh2}}'
              and offer_pkg = 'com.lazada.android'
              and click_through = '1'
              and adtype = '40'
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
            group by
              day,
              hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot
          ) c on (
            a.day = c.day
            and a.hour = c.hour
            and a.offer_id = c.offer_id
          )
          left JOIN (
            select
              substring(time_slot, 1, 8) as day,
              substring(time_slot, 9, 2) as hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot,
              sum(clk_count) click_sync,
              sum(imp_count) impression_sync,
              sum(match_count) match_sync
            from
              real_buzz_sync
            where
              time_slot >= '{{dateh1}}'
              and time_slot <= '{{dateh2}}'
              and offer_pkg = 'com.lazada.android'
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
              and length(slot)<= 8
            group by
              day,
              hour,
              platform,
              country,
              offer_pkg,
              channel,
              offer_id,
              slot
              union all
            select
          substring(time_slot, 1, 8) as day,
          substring(time_slot, 9, 2) as hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot,
          sum(clk_count) click_sync,
          sum(imp_count) impression_sync,
          sum(match_count) match_sync
        from
          real_buzz
        where
          time_slot >= '{{dateh1}}'
          and time_slot <= '{{dateh2}}'
          and offer_pkg = 'com.lazada.android'
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
          and length(slot)<= 8
        group by
          day,
          hour,
          platform,
          country,
          offer_pkg,
          channel,
          offer_id,
          slot
          ) b on (
            b.day = c.day
            and b.hour = c.hour
            and b.offer_id = c.offer_id
            and b.slot = c.slot
          )
        where
          a.offer_id = ''
          and b.offer_id = ''
      )
  ) ta
where
  length(slot) <= 8
  and channel='ath2'
group by
    slot
order by
    cr_10000 desc

"""



"""
select 
  now() as search_datetime,
  day,
  hour,
  slot,
  sum(click_sync) as click_sync_all,
  sum(match_sync) as match_sync_all,
  sum(conv_sync) as conv_sync_all,
  sum(conv_sync)/sum(match_sync) * 10000 as cr_10000
from 
  (
    SELECT 
      a.day as day, 
      a.hour as hour, 
      a.country as country, 
      a.channel as channel, 
      a.offer_id as offer_id, 
      a.slot as slot, 
      (click_fenix + click_sync) as click_all, 
      conv_all, 
      round(
        conv_all * 10000.00 /(
          click_fenix + click_sync 
        ), 
        2
      ) as crx1w_all, 
      click_sync, 
      conv_sync, 
      round(
        conv_sync * 10000.00 / click_sync , 
        2
      ) as crx1w_sync, 
      click_fenix, 
      conv_fenix, 
      round(
        conv_fenix * 100.00 / click_fenix, 2
      ) as crx100_fenix, 
      round(a.revenue, 2) as revenue, 
      round(a.cost, 2) as cost, 
      a.platform as platform, 
      a.offer_pkg as offer_pkg, 
      impression_sync, 
      match_sync   
    from      (
        SELECT 
          substring(time_slot, 1, 8) as day, 
          substring(time_slot, 9, 2) as hour, 
          country, 
          channel, 
          offer_id, 
          slot, 
          count(
            case when (
              adtype in ('40', '20') 
              and event_type = '2'
            ) then 1 else NULL END
          ) as conv_all, 
          count(
            case when (
              adtype = '40' 
              and event_type = '2'
            ) then 1 else NULL END
          ) as conv_fenix, 
          count(
            case when (
              adtype = '20' 
              and event_type = '2'
            ) then 1 else NULL END
          ) as conv_sync, 
          sum(payout) revenue, 
          sum(cost) cost, 
          platform, 
          offer_pkg 
        from 
          real_conversion 
        where 
          formatDateTime(utctime, '%Y%m%d%H') >= '{{dateh1}}' 
          AND formatDateTime(utctime, '%Y%m%d%H') <= '{{dateh2}}' 
          and offer_pkg = 'com.lazada.android' 
          and adtype in ('20', '40') 
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
          and length(slot)<= 8 
        group by 
          day, 
          hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot
      ) a 
      left JOIN (
        select 
          substring(time_slot, 1, 8) as day, 
          substring(time_slot, 9, 2) as hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot, 
          sum(clk_count) click_sync, 
          sum(imp_count) impression_sync, 
          sum(match_count) match_sync 
        from 
          real_buzz_sync 
        where 
          time_slot >= '{{dateh1}}' 
          and time_slot <= '{{dateh2}}' 
          and offer_pkg = 'com.lazada.android' 
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
          and length(slot)<= 8 
        group by 
          day, 
          hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot
        union all
            select 
          substring(time_slot, 1, 8) as day, 
          substring(time_slot, 9, 2) as hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot, 
          sum(clk_count) click_sync, 
          sum(imp_count) impression_sync, 
          sum(match_count) match_sync 
        from 
          real_buzz 
        where 
          time_slot >= '{{dateh1}}' 
          and time_slot <= '{{dateh2}}' 
          and offer_pkg = 'com.lazada.android' 
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
          and length(slot)<= 8 
        group by 
          day, 
          hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot
      ) b on (
        a.day = b.day 
        and a.hour = b.hour 
        and a.offer_id = b.offer_id 
        and a.slot = b.slot
      ) 
      
      left JOIN (
        select 
          substring(time_slot, 1, 8) as day, 
          substring(time_slot, 9, 2) as hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot, 
          count(*) click_fenix 
        from 
          real_fenix_click 
        where 
          time_slot >= '{{dateh1}}' 
          and time_slot <= '{{dateh2}}' 
          and offer_pkg = 'com.lazada.android' 
          and click_through = '1' 
          and adtype = '40' 
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
        group by 
          day, 
          hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot
      ) c on (
        a.day = c.day 
        and a.hour = c.hour 
        and a.offer_id = c.offer_id 
        and a.slot = c.slot
      ) 
    union all 
      (
        SELECT 
          b.day as day, 
          b.hour as hour, 
          b.country as country, 
          b.channel as channel, 
          b.offer_id as offer_id, 
          b.slot as slot, 
          (click_fenix + click_sync) as click_all, 
          conv_all, 
          round(
            conv_all * 10000.00 /(
              click_fenix + click_sync 
            ), 
            2
          ) as crx1w_all, 
          click_sync , 
          conv_sync, 
          round(
            conv_sync * 10000.00 / click_sync , 
            2
          ) as crx1w_sync, 
          click_fenix, 
          conv_fenix, 
          round(
            conv_fenix * 100.00 / click_fenix, 2
          ) as crx100_fenix, 
          round(a.revenue, 2) as revenue, 
          round(a.cost, 2) as cost, 
          b.platform as platform, 
          b.offer_pkg as offer_pkg, 
          impression_sync, 
          match_sync   
        from 
          (
            SELECT 
              substring(time_slot, 1, 8) as day, 
              substring(time_slot, 9, 2) as hour, 
              country, 
              channel, 
              offer_id, 
              slot, 
              count(
                case when (
                  adtype in ('40', '20') 
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_all, 
              count(
                case when (
                  adtype = '40' 
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_fenix, 
              count(
                case when (
                  adtype = '20' 
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_sync, 
              sum(payout) revenue, 
              sum(cost) cost, 
              platform, 
              offer_pkg 
            from 
              real_conversion 
            where 
              formatDateTime(utctime, '%Y%m%d%H') >= '{{dateh1}}' 
              AND formatDateTime(utctime, '%Y%m%d%H') <= '{{dateh2}}' 
              and length(slot)<= 8 
              and offer_pkg = 'com.lazada.android' 
              and adtype in ('20', '40') 
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
            group by 
              day, 
              hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot
          ) a 
          right JOIN (
            select 
              substring(time_slot, 1, 8) as day, 
              substring(time_slot, 9, 2) as hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot, 
              sum(clk_count) click_sync, 
              sum(imp_count) impression_sync, 
              sum(match_count) match_sync 
            from 
              real_buzz_sync 
            where 
              time_slot >= '{{dateh1}}' 
              and time_slot <= '{{dateh2}}' 
              and offer_pkg = 'com.lazada.android' 
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
            group by 
              day, 
              hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot
              union all
            select 
          substring(time_slot, 1, 8) as day, 
          substring(time_slot, 9, 2) as hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot, 
          sum(clk_count) click_sync, 
          sum(imp_count) impression_sync, 
          sum(match_count) match_sync 
        from 
          real_buzz 
        where 
          time_slot >= '{{dateh1}}' 
          and time_slot <= '{{dateh2}}' 
          and offer_pkg = 'com.lazada.android' 
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
          and length(slot)<= 8 
        group by 
          day, 
          hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot
          ) b on (
            a.day = b.day 
            and a.hour = b.hour 
            and a.offer_id = b.offer_id 
            and a.slot = b.slot
          ) 
          
          left JOIN (
            select 
              substring(time_slot, 1, 8) as day, 
              substring(time_slot, 9, 2) as hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot, 
              count(*) click_fenix 
            from 
              real_fenix_click 
            where 
              time_slot >= '{{dateh1}}' 
              and time_slot <= '{{dateh2}}' 
              and offer_pkg = 'com.lazada.android' 
              and click_through = '1' 
              and adtype = '40' 
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
            group by 
              day, 
              hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot
          ) c on (
            b.day = c.day 
            and b.hour = c.hour 
            and b.offer_id = c.offer_id 
            and b.slot = c.slot
          ) 
        where 
          a.offer_id = ''
      ) 
    union all 
      (
        SELECT 
          c.day as day, 
          c.hour as hour, 
          c.country as country, 
          c.channel as channel, 
          c.offer_id as offer_id, 
          c.slot as slot, 
          (
            click_fenix + click_sync 
          ) as click_all, 
          conv_all, 
          round(
            conv_all * 10000.00 /(
              click_fenix + click_sync 
            ), 
            2
          ) as crx1w_all, 
          click_sync , 
          conv_sync, 
          round(
            conv_sync * 10000.00 /(click_sync), 
            2
          ) as crx1w_sync, 
          click_fenix, 
          conv_fenix, 
          round(
            conv_fenix * 100.00 / click_fenix, 2
          ) as crx100_fenix, 
          round(a.revenue, 2) as revenue, 
          round(a.cost, 2) as cost, 
          c.platform as platform, 
          c.offer_pkg as offer_pkg, 
          impression_sync, 
          match_sync  
        from 
          (
            SELECT 
              substring(time_slot, 1, 8) as day, 
              substring(time_slot, 9, 2) as hour, 
              country, 
              channel, 
              offer_id, 
              slot, 
              count(
                case when (
                  adtype in ('40', '20') 
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_all, 
              count(
                case when (
                  adtype = '40' 
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_fenix, 
              count(
                case when (
                  adtype = '20' 
                  and event_type = '2'
                ) then 1 else NULL END
              ) as conv_sync, 
              sum(payout) revenue, 
              sum(cost) cost, 
              platform, 
              offer_pkg 
            from 
              real_conversion 
            where 
              formatDateTime(utctime, '%Y%m%d%H') >= '{{dateh1}}' 
              AND formatDateTime(utctime, '%Y%m%d%H') <= '{{dateh2}}' 
              and offer_pkg = 'com.lazada.android' 
              and adtype in ('20', '40') 
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
            group by 
              day, 
              hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot
          ) a 
          right JOIN (
            select 
              substring(time_slot, 1, 8) as day, 
              substring(time_slot, 9, 2) as hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot, 
              count(*) click_fenix 
            from 
              real_fenix_click 
            where 
              time_slot >= '{{dateh1}}' 
              and time_slot <= '{{dateh2}}' 
              and offer_pkg = 'com.lazada.android' 
              and click_through = '1' 
              and adtype = '40' 
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
            group by 
              day, 
              hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot
          ) c on (
            a.day = c.day 
            and a.hour = c.hour 
            and a.offer_id = c.offer_id
          ) 
          left JOIN (
            select 
              substring(time_slot, 1, 8) as day, 
              substring(time_slot, 9, 2) as hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot, 
              sum(clk_count) click_sync, 
              sum(imp_count) impression_sync, 
              sum(match_count) match_sync 
            from 
              real_buzz_sync 
            where 
              time_slot >= '{{dateh1}}' 
              and time_slot <= '{{dateh2}}' 
              and offer_pkg = 'com.lazada.android' 
              and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
              and length(slot)<= 8 
            group by 
              day, 
              hour, 
              platform, 
              country, 
              offer_pkg, 
              channel, 
              offer_id, 
              slot
              union all
            select 
          substring(time_slot, 1, 8) as day, 
          substring(time_slot, 9, 2) as hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot, 
          sum(clk_count) click_sync, 
          sum(imp_count) impression_sync, 
          sum(match_count) match_sync 
        from 
          real_buzz 
        where 
          time_slot >= '{{dateh1}}' 
          and time_slot <= '{{dateh2}}' 
          and offer_pkg = 'com.lazada.android' 
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN') 
          and length(slot)<= 8 
        group by 
          day, 
          hour, 
          platform, 
          country, 
          offer_pkg, 
          channel, 
          offer_id, 
          slot
          ) b on (
            b.day = c.day 
            and b.hour = c.hour 
            and b.offer_id = c.offer_id 
            and b.slot = c.slot
          ) 
        where 
          a.offer_id = '' 
          and b.offer_id = ''
      )
  ) ta 
where 
  length(slot) <= 8 
  and channel='ath2'
  and slot in ('26771578','39853849')
group by 
    slot,day,hour
order by 
    slot,day,hour 
"""