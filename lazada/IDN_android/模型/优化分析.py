"""


SELECT
          substring(time_slot, 1, 8) as day,
          substring(time_slot, 9, 2) as hour,
          country,
          channel,
          offer_id,
          slot,
          platform,
        from
          real_conversion
        where
          formatDateTime(utctime, '%Y%m%d%H') >= '{{dateh1}}'
          AND formatDateTime(utctime, '%Y%m%d%H') <= '{{dateh2}}'
          and offer_pkg = 'com.lazada.android'
          and adtype = '20'
          and event_type = '2'
          and country in('ID', 'PH', 'TH', 'MY', 'SG', 'VN')
          and length(slot)<= 8
          and channel='ath2'
        limit 50
"""