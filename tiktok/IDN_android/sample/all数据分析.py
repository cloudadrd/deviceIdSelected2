df=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/tiktok/IDN_android/sample_ana/20220707_delay_2/")
df.createTempView("df_table")
spark.sql("   \
    select  \
        now_app \
        ,count(device_id) as device_id_cnt  \
    from    \
    (   \
        select  \
            device_id   \
            ,now_app    \
        from    \
        (   \
            select  \
                device_id   \
                ,now_app_set    \
            from    \
                df_table    \
            where   \
                now_label=0 \
                and after_label=1   \
                and after_app_set_size > now_app_set_size   \
                and now_app_set_size<=3 \
        )t1 \
        lateral view    \
            explode(now_app_set) as now_app \
    )t2 \
    group by    \
        now_app \
    order by    \
        device_id_cnt desc  \
").show(100,truncate=False)
"""
+---------------------------------------------------------------------------------------+-------------+
|now_app                                                                                |device_id_cnt|
+---------------------------------------------------------------------------------------+-------------+
|N                                                                                      |30561        |
|com.kwai.bulldog                                                                       |1917         |
|com.playit.videoplayer                                                                 |1040         |
|com.quantum.videoplayer                                                                |397          |
|vinkle.video.editor                                                                    |379          |
|com.quantum.vmplayer                                                                   |371          |
|com.lemon.lvoverseas                                                                   |255          |
|com.nexstreaming.app.kinemasterfree                                                    |178          |
|com.tempo.video.edit                                                                   |178          |
|com.mxtech.videoplayer.ad                                                              |175          |
|instasaver.instagram.video.downloader.photo                                            |146          |
|com.camerasideas.trimmer                                                               |142          |
|videoeditor.videorecorder.screenrecorder                                               |137          |
|free.tube.premium.advanced.tuber                                                       |133          |
|ins.story.unfold                                                                       |115          |
|com.cashtube.ay                                                                        |110          |
|com.popularapp.videodownloaderforinstagram                                             |80           |
|com.coloros.video                                                                      |79           |
|com.biomes.vanced                                                                      |74           |
|facebook.video.downloader.savefrom.fb                                                  |69           |
|mobisocial.arcade                                                                      |64           |
|vidma.screenrecorder.videorecorder.videoeditor.pro                                     |63           |
|com.quvideo.xiaoying                                                                   |59           |
|video.player.videoplayer                                                               |49           |
|com.video.editor.greattalent                                                           |49           |
|glitchvideoeditor.videoeffects.glitchvideoeffect                                       |47           |
|com.story.saver.instagram.video.downloader.repost                                      |45           |
|tiktok.video.downloader.nowatermark.tiktokdownload                                     |44           |
|com.picslab.neon.editor                                                                |42           |
|com.instagram.video.downloader.story.saver.photo.downloader.videodownload.photodownload|42           |
|instake.repost.instagramphotodownloader.instagramvideodownloader                       |41           |
|free.video.downloader.freevideodownloader                                              |36           |
|all.video.downloader.allvideodownloader                                                |35           |
|screenrecorder.recorder.editor                                                         |35           |
|com.alightcreative.motion                                                              |33           |
|com.thinkyeah.galleryvault                                                             |32           |
|downloader.video.download.free                                                         |32           |
|mp3videoconverter.videotomp3.videotomp3converter                                       |31           |
|com.idea.videocompress                                                                 |29           |
|videoeditor.videomaker.videoeditorforyoutube                                           |26           |
|com.pandavideocompressor                                                               |25           |
|com.videohunt.like.app.me                                                              |24           |
|ins.mate.instagram.downloader.repost.hashtag.multiple                                  |21           |
|com.videomaker.photovideo                                                              |21           |
|com.comoon.videoplayer.xnxvideoplayer.allformat.videoplayer                            |21           |
|com.recorder.music.bstech.videoplayer                                                  |20           |
|free.files.downloader.save.video.manager                                               |20           |
|tmate.tiktokvideodownloader.nowatermark.savetiktokvideo                                |19           |
|com.znstudio.instadownload                                                             |17           |
|com.hecorat.screenrecorder.free                                                        |17           |
|com.xvideostudio.videocompress                                                         |17           |
|video.downloader.videodownloader                                                       |17           |
|com.xvideostudio.videoeditor                                                           |17           |
|free.video.downloader.instagram.instake                                                |16           |
|storysaverforinstagram.storydownloader.instastorysaver                                 |16           |
|com.vid007.videobuddy                                                                  |16           |
|com.funcamerastudio.videomaker                                                         |16           |
|com.ai.face.play                                                                       |16           |
|com.cyberlink.powerdirector.DRA140225_01                                               |15           |
|com.hld.anzenbokusucal                                                                 |15           |
|com.music.slideshow.videoeditor.videomaker                                             |15           |
|vidma.screenrecorder.videorecorder.videoeditor.lite                                    |15           |
|videoplayer.videodownloader.hdvideoplayer                                              |14           |
|com.dlvideo.downloader.android                                                         |14           |
|com.mediastudio.allvideodownloader.downloadvideoshd.bestdownloader                     |14           |
|videodownloader.fbvideodownloader.facebookvideodownloader.videodownloaderforfacebook   |13           |
|com.tianxingjian.screenshot                                                            |13           |
|com.videodownloader.instagram.video.downloader                                         |13           |
|com.smartapps.videodownloaderfortiktok                                                 |12           |
|com.video.downloader.snapx                                                             |11           |
|com.utorrent.client                                                                    |11           |
|com.frontrow.vlog                                                                      |11           |
|com.bigtvgo.playerx                                                                    |11           |
|com.vidmix.music.maker                                                                 |11           |
|freemusic.player                                                                       |10           |
|com.saxvideoplayer.newplayerpro                                                        |10           |
|com.rocks.music.videoplayer                                                            |10           |
|com.xx.xxvi.xxvidownloder                                                              |10           |
|instagram.downloader.download.videos.photos                                            |10           |
|com.xvideostudio.videodownload                                                         |10           |
|com.instadownloader.instasave.igsave.ins                                               |10           |
|beatly.lite.tiktok                                                                     |10           |
|com.h20soft.photoeditor.musicvideo.slideshow.videomaker                                |10           |
|com.allvideodownloader.snapsave.videohd4k                                              |10           |
|instasaver.videodownloader.photodownloader.repost                                      |9            |
|com.downloadvideo.videodownloadfree21                                                  |9            |
|videoslideshow.photoedit.videocutter                                                   |9            |
|com.videoeditorpro.android                                                             |9            |
|twimate.tweetdownloader.savetwittergif.twittervideodownloader                          |9            |
|com.snapig.instagramdownloader                                                         |9            |
|com.vaultmicro.camerafi.live                                                           |8            |
|com.mobi.screenrecorder.durecorder                                                     |8            |
|com.free.video.downloader.eeopi                                                        |8            |
|com.videomaker.videoeditor.photos.music                                                |8            |
|facebookvideodownloader.videodownloaderforfacebook                                     |8            |
|com.cocotbosok.videosupsipnewcollection                                                |8            |
|com.movisoftnew.videoeditor                                                            |8            |
|com.yoosee                                                                             |8            |
|com.prime.story.android                                                                |8            |
|hd.video.downloader.app.hdvideodownloaderapp                                           |8            |
+---------------------------------------------------------------------------------------+-------------+
"""

spark.sql("   \
    select  \
        now_app \
        ,count(device_id) as device_id_cnt  \
    from    \
    (   \
        select  \
            device_id   \
            ,now_app    \
        from    \
        (   \
            select  \
                device_id   \
                ,now_app_set    \
            from    \
                df_table    \
            where   \
                now_label=0 \
                and after_label=0   \
                and after_app_set_size > now_app_set_size   \
                and now_app_set_size<=3 \
        )t1 \
        lateral view    \
            explode(now_app_set) as now_app \
    )t2 \
    group by    \
        now_app \
    order by    \
        device_id_cnt desc  \
").show(100,truncate=False)
"""
+---------------------------------------------------------------------------------------+-------------+
|now_app                                                                                |device_id_cnt|
+---------------------------------------------------------------------------------------+-------------+
|N                                                                                      |253095       |
|com.playit.videoplayer                                                                 |43011        |
|com.kwai.bulldog                                                                       |33173        |
|com.quantum.videoplayer                                                                |24633        |
|vinkle.video.editor                                                                    |24605        |
|com.tempo.video.edit                                                                   |14549        |
|com.quantum.vmplayer                                                                   |12752        |
|com.lemon.lvoverseas                                                                   |12107        |
|com.nexstreaming.app.kinemasterfree                                                    |8384         |
|videoeditor.videorecorder.screenrecorder                                               |8017         |
|ins.story.unfold                                                                       |7899         |
|instasaver.instagram.video.downloader.photo                                            |6232         |
|com.coloros.video                                                                      |5599         |
|tiktok.video.downloader.nowatermark.tiktokdownload                                     |5558         |
|com.mxtech.videoplayer.ad                                                              |5526         |
|com.camerasideas.trimmer                                                               |4555         |
|vidma.screenrecorder.videorecorder.videoeditor.pro                                     |4363         |
|com.alightcreative.motion                                                              |4282         |
|com.picslab.neon.editor                                                                |3777         |
|com.cashtube.ay                                                                        |3359         |
|mobisocial.arcade                                                                      |3135         |
|screenrecorder.recorder.editor                                                         |3067         |
|tmate.tiktokvideodownloader.nowatermark.savetiktokvideo                                |3000         |
|com.smartapps.videodownloaderfortiktok                                                 |2977         |
|com.story.saver.instagram.video.downloader.repost                                      |2612         |
|com.popularapp.videodownloaderforinstagram                                             |2539         |
|com.quvideo.xiaoying                                                                   |2507         |
|com.video.editor.greattalent                                                           |2453         |
|com.video.downloader.snapx                                                             |2363         |
|com.instagram.video.downloader.story.saver.photo.downloader.videodownload.photodownload|2276         |
|facebook.video.downloader.savefrom.fb                                                  |2273         |
|free.tube.premium.advanced.tuber                                                       |2256         |
|glitchvideoeditor.videoeffects.glitchvideoeffect                                       |2248         |
|com.pandavideocompressor                                                               |2229         |
|instake.repost.instagramphotodownloader.instagramvideodownloader                       |2204         |
|com.frontrow.vlog                                                                      |2003         |
|free.video.downloader.freevideodownloader                                              |1936         |
|com.videohunt.like.app.me                                                              |1600         |
|video.player.videoplayer                                                               |1523         |
|com.vidmix.music.maker                                                                 |1516         |
|all.video.downloader.allvideodownloader                                                |1480         |
|mp3videoconverter.videotomp3.videotomp3converter                                       |1470         |
|com.ai.face.play                                                                       |1460         |
|vidma.screenrecorder.videorecorder.videoeditor.lite                                    |1399         |
|com.idea.videocompress                                                                 |1369         |
|com.biomes.vanced                                                                      |1208         |
|downloader.video.download.free                                                         |1170         |
|com.xvideostudio.videocompress                                                         |1134         |
|videoeditor.videomaker.videoeditorforyoutube                                           |1112         |
|beatly.lite.tiktok                                                                     |1051         |
|com.xvideostudio.videoeditor                                                           |1009         |
|ins.mate.instagram.downloader.repost.hashtag.multiple                                  |1007         |
|com.vid007.videobuddy                                                                  |992          |
|free.video.downloader.instagram.instake                                                |909          |
|instagram.downloader.download.videos.photos                                            |887          |
|com.funcamerastudio.videomaker                                                         |863          |
|music.video.photo.slideshow.maker                                                      |822          |
|com.vaultmicro.camerafi.live                                                           |815          |
|com.hecorat.screenrecorder.free                                                        |804          |
|videoplayer.videodownloader.hdvideoplayer                                              |784          |
|com.thinkyeah.galleryvault                                                             |751          |
|com.danielkorgel.SmoothActionCamSlowmo                                                 |717          |
|video.downloader.videodownloader                                                       |706          |
|snaptik.app.tiktokdownloader.nowatermark                                               |700          |
|com.hld.anzenbokusucal                                                                 |687          |
|com.h20soft.slideshow.videomaker.videoeditor                                           |673          |
|free.files.downloader.save.video.manager                                               |673          |
|videodownloader.fbvideodownloader.facebookvideodownloader.videodownloaderforfacebook   |667          |
|com.xvideostudio.videodownload                                                         |655          |
|com.recorder.music.bstech.videoplayer                                                  |640          |
|com.znstudio.instadownload                                                             |634          |
|com.videodownloaderfortiktok.withoutwatermark                                          |633          |
|com.cyberlink.powerdirector.DRA140225_01                                               |605          |
|com.tiktokdownloader.downloadnotwatermark                                              |597          |
|com.snapmate.tiktokdownloadernowatermark                                               |594          |
|com.instadownloader.instasave.igsave.ins                                               |593          |
|com.mediastudio.allvideodownloader.downloadvideoshd.bestdownloader                     |561          |
|com.videodownloader.instagram.video.downloader                                         |553          |
|com.sss.video.downloader.tiktok                                                        |553          |
|videoslideshow.photoedit.videocutter                                                   |547          |
|com.videomaker.photovideo                                                              |513          |
|hd.video.downloader.app.hdvideodownloaderapp                                           |509          |
|instasaver.videodownloader.photodownloader.repost                                      |495          |
|com.videoeditorpro.android                                                             |489          |
|com.tianxingjian.screenshot                                                            |489          |
|com.comoon.videoplayer.xnxvideoplayer.allformat.videoplayer                            |482          |
|com.young.simple.player                                                                |478          |
|com.downloadvideotiktok.nowatermark                                                    |476          |
|com.musicvideomaker.slideshow                                                          |451          |
|com.downloadvideo.videodownloadfree21                                                  |445          |
|remove.picture.video.watermark.watermarkremove                                         |442          |
|storysaverforinstagram.storydownloader.instastorysaver                                 |436          |
|com.mobi.screenrecorder.durecorder                                                     |432          |
|com.bigtvgo.playerx                                                                    |409          |
|com.mobile.bizo.slowmotion                                                             |408          |
|com.videomaker.videoeditor.photos.music                                                |400          |
|com.recorder.screenrecorder.capture                                                    |391          |
|com.media.music.videomaker.slideshow                                                   |391          |
|com.rocks.music.videoplayer                                                            |386          |
|com.xx.xxvi.xxvidownloder                                                              |365          |
+---------------------------------------------------------------------------------------+-------------+
"""
spark.sql("   \
select  \
    lastdaygap_label    \
    ,count(device_id) as device_id_cnt  \
from    \
    df_table    \
where   \
    now_label=0 \
    and after_label=1   \
group by    \
    lastdaygap_label    \
order by    \
    device_id_cnt desc  \
").show(100,truncate=False)

"""
+----------------+-------------+                                                
|lastdaygap_label|device_id_cnt|
+----------------+-------------+
|0               |24648        |
|1               |4118         |
|2               |2454         |
|3               |1662         |
|4               |1155         |
|5               |877          |
|6               |644          |
|7               |463          |
|8               |416          |
|9               |198          |
|10              |70           |
|11              |58           |
|13              |56           |
|12              |53           |
|15              |41           |
|14              |34           |
|16              |34           |
|20              |34           |
|17              |31           |
|25              |23           |
|24              |22           |
|21              |21           |
|18              |20           |
|19              |20           |
|26              |20           |
|27              |19           |
|22              |19           |
|28              |19           |
|23              |19           |
|30              |7            |
|29              |6            |
|76              |1            |
|51              |1            |
+----------------+-------------+
"""