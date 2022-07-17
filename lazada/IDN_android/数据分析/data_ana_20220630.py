# # 分析数据
# pyspark \
# --driver-memory 10G \
# --executor-memory 10G \
# --num-executors 5 \
# --executor-cores 8
#
#
# # df=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/20220630_delay_1")
# # df.createTempView("df_table")
# # spark.sql("""desc df_table""").show(500,truncate=False)
# """
# +----------------------+-------------+-------+
# |col_name              |data_type    |comment|
# +----------------------+-------------+-------+
# |device_id             |string       |null   |
# |osversion             |string       |null   |
# |now_label             |int          |null   |
# |after_label           |int          |null   |
# |bundle_app_set        |array<string>|null   |
# |bundle_app_set_size   |int          |null   |
# |activeday_label_1_cnt |bigint       |null   |
# |activeday_label_2_cnt |bigint       |null   |
# |activeday_label_3_cnt |bigint       |null   |
# |activeday_label_4_cnt |bigint       |null   |
# |activeday_label_5_cnt |bigint       |null   |
# |activeday_label_6_cnt |bigint       |null   |
# |activeday_label_7_cnt |bigint       |null   |
# |activeday_label_8_cnt |bigint       |null   |
# |activeday_label_9_cnt |bigint       |null   |
# |activeday_label_10_cnt|bigint       |null   |
# |activeday_label_1_01  |int          |null   |
# |activeday_label_2_01  |int          |null   |
# |activeday_label_3_01  |int          |null   |
# |activeday_label_4_01  |int          |null   |
# |activeday_label_5_01  |int          |null   |
# |activeday_label_6_01  |int          |null   |
# |activeday_label_7_01  |int          |null   |
# |activeday_label_8_01  |int          |null   |
# |activeday_label_9_01  |int          |null   |
# |activeday_label_10_01 |int          |null   |
# |activeday_label_11_cnt|bigint       |null   |
# |activeday_label_12_cnt|bigint       |null   |
# |activeday_label_13_cnt|bigint       |null   |
# |activeday_label_14_cnt|bigint       |null   |
# |activeday_label_15_cnt|bigint       |null   |
# |activeday_label_16_cnt|bigint       |null   |
# |activeday_label_17_cnt|bigint       |null   |
# |activeday_label_18_cnt|bigint       |null   |
# |activeday_label_19_cnt|bigint       |null   |
# |activeday_label_20_cnt|bigint       |null   |
# |activeday_label_11_01 |int          |null   |
# |activeday_label_12_01 |int          |null   |
# |activeday_label_13_01 |int          |null   |
# |activeday_label_14_01 |int          |null   |
# |activeday_label_15_01 |int          |null   |
# |activeday_label_16_01 |int          |null   |
# |activeday_label_17_01 |int          |null   |
# |activeday_label_18_01 |int          |null   |
# |activeday_label_19_01 |int          |null   |
# |activeday_label_20_01 |int          |null   |
# |activeday_label_21_cnt|bigint       |null   |
# |activeday_label_22_cnt|bigint       |null   |
# |activeday_label_23_cnt|bigint       |null   |
# |activeday_label_24_cnt|bigint       |null   |
# |activeday_label_25_cnt|bigint       |null   |
# |activeday_label_26_cnt|bigint       |null   |
# |activeday_label_27_cnt|bigint       |null   |
# |activeday_label_28_cnt|bigint       |null   |
# |activeday_label_29_cnt|bigint       |null   |
# |activeday_label_30_cnt|bigint       |null   |
# |activeday_label_21_01 |int          |null   |
# |activeday_label_22_01 |int          |null   |
# |activeday_label_23_01 |int          |null   |
# |activeday_label_24_01 |int          |null   |
# |activeday_label_25_01 |int          |null   |
# |activeday_label_26_01 |int          |null   |
# |activeday_label_27_01 |int          |null   |
# |activeday_label_28_01 |int          |null   |
# |activeday_label_29_01 |int          |null   |
# |activeday_label_30_01 |int          |null   |
# |bundles_size_label    |int          |null   |
# |lastdaygap_label      |int          |null   |
# +----------------------+-------------+-------+
# """
#
#
# # spark.sql(" \
# #     select  \
# #         count(if(now_label=0 and after_label=1,1,null)) as 0_1  \
# #         ,count(if(now_label=1 and after_label=1,1,null)) as 1_1 \
# #         ,count(if(now_label=0 and after_label=0,1,null)) as 0_0 \
# #         ,count(if(now_label=1 and after_label=0,1,null)) as 1_0 \
# #     from    \
# #         df_table    \
# # ").show()
# """
# +-----+--------+---------+-----+
# |  0_1|     1_1|      0_0|  1_0|
# +-----+--------+---------+-----+
# |10905|22952154|266511861|98087|
# +-----+--------+---------+-----+
# """
# a_1_1 = 22952154
# a_0_1 = 10905
# a_0_0 = 266511861
# a_1_0 = 98087
# b_1= a_1_1 + a_1_0
# print("b_1",b_1)
# b_1_0_precent = a_1_0/b_1
# print("b_1_1_precent",b_1_0_precent)
# b_0 = a_0_0 + a_0_1
# print("b_0",b_0)
# b_0_1_percent = a_0_1 / b_0
# print("b_0_1_percent",b_0_1_percent)
#
#
#
# # spark.sql("   \
# #     select  \
# #         app,now_label,after_label   \
# #         ,count(device_id) as device_id_cnt  \
# #     from    \
# #     (   \
# #         select  \
# #             device_id   \
# #             ,now_label  \
# #             ,after_label    \
# #             ,app    \
# #         from    \
# #             df_table    \
# #         lateral view    \
# #             explode(bundle_app_set) as app  \
# #     ) t1    \
# #     group by    \
# #         app,now_label,after_label   \
# #     order by    \
# #         device_id_cnt desc  \
# # ").show(1000,truncate=False)
#
# """
# +--------------------------------------------------------------------------------------------------+---------+-----------+-------------+
# |app                                                                                               |now_label|after_label|device_id_cnt|
# +--------------------------------------------------------------------------------------------------+---------+-----------+-------------+
# |com.lazada.android                                                                                |1        |1          |22952154     |
# |id.co.shopintar                                                                                   |0        |0          |9971428      |
# |com.app.tokobagus.betterb                                                                         |0        |0          |7314356      |
# |id.co.shopintar                                                                                   |1        |1          |1770511      |
# |com.shopee.id                                                                                     |0        |0          |1686219      |
# |com.app.tokobagus.betterb                                                                         |1        |1          |1511280      |
# |com.alibaba.aliexpresshd                                                                          |0        |0          |1251127      |
# |blibli.mobile.commerce                                                                            |0        |0          |776590       |
# |com.adfone.indosat                                                                                |0        |0          |718997       |
# |com.tokopedia.tkpd                                                                                |0        |0          |428325       |
# |com.adfone.unefon                                                                                 |0        |0          |427936       |
# |com.thecarousell.Carousell                                                                        |0        |0          |305431       |
# |com.alibaba.aliexpresshd                                                                          |1        |1          |246798       |
# |com.shopee.id                                                                                     |1        |1          |230562       |
# |com.bukalapak.mitra                                                                               |0        |0          |228538       |
# |com.alibaba.intl.android.apps.poseidon                                                            |0        |0          |218589       |
# |com.tokopedia.tkpd                                                                                |1        |1          |215526       |
# |blibli.mobile.commerce                                                                            |1        |1          |176084       |
# |io.silvrr.installment                                                                             |0        |0          |117196       |
# |com.newchic.client                                                                                |0        |0          |99807        |
# |com.lazada.android                                                                                |1        |0          |98087        |
# |com.adfone.indosat                                                                                |1        |1          |89854        |
# |com.cari.promo.diskon                                                                             |0        |0          |76514        |
# |com.bukalapak.android                                                                             |0        |0          |71570        |
# |com.thecarousell.Carousell                                                                        |1        |1          |71043        |
# |com.alibaba.intl.android.apps.poseidon                                                            |1        |1          |46197        |
# |com.cari.promo.diskon                                                                             |1        |1          |42184        |
# |indopub.unipin.topup                                                                              |0        |0          |36245        |
# |com.newchic.client                                                                                |1        |1          |35267        |
# |io.silvrr.installment                                                                             |1        |1          |34472        |
# |studio.pvs.t_shirtdesignstudio                                                                    |0        |0          |33417        |
# |com.bukalapak.mitra                                                                               |1        |1          |32079        |
# |com.onlinecoupons.lazada                                                                          |0        |0          |30689        |
# |com.rootbridge.ula                                                                                |0        |0          |25993        |
# |com.application_4u.qrcode.barcode                                                                 |0        |0          |25650        |
# |com.yoinsapp                                                                                      |0        |0          |25603        |
# |com.memopad.for_.writing.babylon                                                                  |0        |0          |24623        |
# |com.bukalapak.android                                                                             |1        |1          |22068        |
# |com.propcoid.propcoid                                                                             |0        |0          |21127        |
# |universal.app.cek.resi.shopee.express                                                             |0        |0          |17014        |
# |com.pim.pulsapedia                                                                                |0        |0          |15355        |
# |com.jollycorp.jollychic                                                                           |0        |0          |14175        |
# |com.pixelkoin.app                                                                                 |0        |0          |13967        |
# |diamond.via.id                                                                                    |0        |0          |13777        |
# |com.Alltor.CodaFree                                                                               |0        |0          |13457        |
# |com.app.offerit                                                                                   |0        |0          |13301        |
# |com.grannyrewards.app                                                                             |0        |0          |12746        |
# |com.kkmart.user                                                                                   |0        |0          |12575        |
# |indopub.codashop.popup                                                                            |0        |0          |12458        |
# |com.brightstripe.parcels                                                                          |0        |0          |12037        |
# |com.geomobile.tiendeo                                                                             |0        |0          |11884        |
# |com.motivs.marketplaceeid                                                                         |0        |0          |11707        |
# |com.ptmarketplaceio.jualxbeli                                                                     |0        |0          |11583        |
# |indopub.unipin.topup                                                                              |1        |1          |11340        |
# |co.tapcart.app.id_wO7BWzmfNM                                                                      |0        |0          |11124        |
# |com.shopee.ph                                                                                     |0        |0          |10127        |
# |app.idcshop.com                                                                                   |0        |0          |9598         |
# |com.livicepat.tokolengkap                                                                         |0        |0          |9531         |
# |com.app.tokobagus.betterb                                                                         |1        |0          |9336         |
# |id.co.shopintar                                                                                   |1        |0          |9318         |
# |com.ajsneakeronline.android                                                                       |0        |0          |9245         |
# |com.application_4u.qrcode.barcode                                                                 |1        |1          |9103         |
# |com.checkoutcharlie.shopmate                                                                      |0        |0          |8490         |
# |com.mrbox.guanfang.large                                                                          |0        |0          |8047         |
# |com.kiodeecfeedgtp.wip                                                                            |0        |0          |8044         |
# |com.primarkshopstore.shopukprimark                                                                |0        |0          |8043         |
# |studio.pvs.t_shirtdesignstudio                                                                    |1        |1          |8035         |
# |com.topsmarkets.app.android                                                                       |0        |0          |7874         |
# |com.gdmagri.app                                                                                   |0        |0          |7874         |
# |freefire.id                                                                                       |0        |0          |7755         |
# |com.voixme.d4d                                                                                    |0        |0          |7401         |
# |com.geekslab.qrbarcodescanner.pro                                                                 |0        |0          |7231         |
# |jd.cdyjy.overseas.market.indonesia                                                                |0        |0          |7219         |
# |com.snkrdunk.en.android                                                                           |0        |0          |7154         |
# |com.nz4.diamondsff                                                                                |0        |0          |7126         |
# |com.shopee.my                                                                                     |0        |0          |7057         |
# |com.kanzengames                                                                                   |0        |0          |6555         |
# |com.agromaret.android.app                                                                         |0        |0          |6480         |
# |evermos.evermos.com.evermos                                                                       |0        |0          |6406         |
# |com.codmobil.mobilcod                                                                             |0        |0          |6179         |
# |kr.co.quicket                                                                                     |0        |0          |5941         |
# |jd.cdyjy.overseas.market.indonesia                                                                |1        |1          |5940         |
# |diamonds.ff.viapulsa                                                                              |0        |0          |5815         |
# |com.yoinsapp                                                                                      |1        |1          |5794         |
# |com.l                                                                                             |0        |0          |5756         |
# |com.zulily.android                                                                                |0        |0          |5705         |
# |com.tycmrelx.cash                                                                                 |0        |0          |5469         |
# |com.asemediatech.cekresiongkir                                                                    |0        |0          |5429         |
# |develop.joo.scanbarcodeharga                                                                      |0        |0          |5341         |
# |indopub.codashop.popup                                                                            |1        |1          |5301         |
# |mmapps.mobile.discount.calculator                                                                 |0        |0          |4966         |
# |com.application_4u.qrcode.barcode.scanner.reader.flashlight                                       |0        |0          |4690         |
# |com.ebay.mobile                                                                                   |0        |0          |4683         |
# |com.airjordanshop.android                                                                         |0        |0          |4636         |
# |com.locket.locketguide                                                                            |0        |0          |4633         |
# |com.cakecodes.bitmaker                                                                            |0        |0          |4412         |
# |com.giftmill.giftmill                                                                             |0        |0          |4398         |
# |com.memopad.for_.writing.babylon                                                                  |1        |1          |4165         |
# |com.iljovita.hargapromokatalog3                                                                   |0        |0          |4116         |
# |com.pixelkoin.app                                                                                 |1        |1          |4080         |
# |com.onlinecoupons.lazada                                                                          |1        |1          |4015         |
# |com.vova.android                                                                                  |1        |1          |3959         |
# |net.tsapps.appsales                                                                               |0        |0          |3952         |
# |com.Expandear.KatalogPromo                                                                        |0        |0          |3887         |
# |com.ILJOVITA.android.universalwebview                                                             |0        |0          |3738         |
# |com.grannyrewards.app                                                                             |1        |1          |3707         |
# |com.mudah.my                                                                                      |0        |0          |3663         |
# |id.allofresh.ecommerce                                                                            |0        |0          |3527         |
# |com.perekrestochek.app                                                                            |0        |0          |3511         |
# |com.jollycorp.jollychic                                                                           |1        |1          |3467         |
# |com.interfocusllc.patpat                                                                          |0        |0          |3432         |
# |com.pim.pulsapedia                                                                                |1        |1          |3417         |
# |com.mygamezone.gamezone                                                                           |0        |0          |3405         |
# |yotcho.shop                                                                                       |0        |0          |3284         |
# |fr.casino.fidelite                                                                                |0        |0          |3241         |
# |com.brightstripe.parcels                                                                          |1        |1          |3235         |
# |com.propcoid.propcoid                                                                             |1        |1          |3052         |
# |com.geomobile.tiendeo                                                                             |1        |1          |3044         |
# |com.tiktokshop.tiktokshop                                                                         |0        |0          |3009         |
# |com.on9store.shopping.allpromotion                                                                |0        |0          |2994         |
# |com.todayapp.cekresi                                                                              |0        |0          |2933         |
# |com.joom                                                                                          |0        |0          |2715         |
# |com.ebay.gumtree.au                                                                               |0        |0          |2695         |
# |com.androidrocker.qrscanner                                                                       |0        |0          |2671         |
# |com.CouponChart                                                                                   |0        |0          |2668         |
# |com.offerup                                                                                       |0        |0          |2643         |
# |com.shoppingapp.shopeeeasa                                                                        |0        |0          |2625         |
# |com.livicepat.tokolengkap                                                                         |1        |1          |2591         |
# |com.tuck.hellomarket                                                                              |0        |0          |2579         |
# |com.whaleshark.retailmenot                                                                        |0        |0          |2523         |
# |com.wKLIKGAMESHOPAdalahMitraAgenChipHiggsDominoIsland_13240116                                    |0        |0          |2462         |
# |com.xbox_deals.sales                                                                              |0        |0          |2419         |
# |com.abtnprojects.ambatana                                                                         |0        |0          |2417         |
# |com.zalora.android                                                                                |0        |0          |2415         |
# |com.geekslab.qrbarcodescanner.pro                                                                 |1        |1          |2361         |
# |shopping.indonesia.com.app                                                                        |0        |0          |2360         |
# |com.globalbusiness.countrychecker                                                                 |0        |0          |2350         |
# |com.shopee.br                                                                                     |0        |0          |2306         |
# |diamonds.ff.viapulsa                                                                              |1        |1          |2269         |
# |com.agromaret.android.app                                                                         |1        |1          |2242         |
# |com.onlineshoppingindonesia.indonesiashopping                                                     |0        |0          |2236         |
# |com.my.exchangecounter                                                                            |0        |0          |2218         |
# |com.biggu.shopsavvy                                                                               |0        |0          |2200         |
# |com.zalora.android                                                                                |1        |1          |2191         |
# |fr.vinted                                                                                         |0        |0          |2107         |
# |com.lootboy.app                                                                                   |0        |0          |2095         |
# |com.cakecodes.bitmaker                                                                            |1        |1          |2074         |
# |com.ptmarketplaceio.jualxbeli                                                                     |1        |1          |2068         |
# |com.nz4.diamondsff                                                                                |1        |1          |1974         |
# |com.olx.southasia                                                                                 |0        |0          |1973         |
# |com.gdmagri.app                                                                                   |1        |1          |1960         |
# |com.rootbridge.ula                                                                                |1        |1          |1950         |
# |com.kohls.mcommerce.opal                                                                          |0        |0          |1940         |
# |com.ebay.kleinanzeigen                                                                            |0        |0          |1929         |
# |com.Alltor.CodaFree                                                                               |1        |1          |1916         |
# |com.asemediatech.cekresiongkir                                                                    |1        |1          |1878         |
# |hajigaming.codashop.malaysia                                                                      |0        |0          |1827         |
# |com.shopee.th                                                                                     |0        |0          |1801         |
# |aw.kiriman                                                                                        |0        |0          |1778         |
# |indopub.unipinpro.topup                                                                           |0        |0          |1742         |
# |com.mi.global.shop                                                                                |1        |1          |1740         |
# |toko.online.terpercaya                                                                            |0        |0          |1704         |
# |com.my.cocpostit                                                                                  |0        |0          |1698         |
# |com.shopback.app                                                                                  |1        |1          |1676         |
# |com.aptivdev.remotecontroltoys                                                                    |0        |0          |1664         |
# |com.andromo.dev786963.app908281                                                                   |0        |0          |1661         |
# |com.dhgate.buyermob                                                                               |0        |0          |1650         |
# |com.mudah.my                                                                                      |1        |1          |1623         |
# |com.appscentral.popitcase                                                                         |0        |0          |1617         |
# |com.tikshop.tikshop                                                                               |0        |0          |1612         |
# |com.motivs.marketplaceeid                                                                         |1        |1          |1589         |
# |com.avito.android                                                                                 |0        |0          |1572         |
# |com.xyz.alihelper                                                                                 |0        |0          |1566         |
# |com.zanyatocorp.checkbarcode                                                                      |0        |0          |1565         |
# |mmapps.mobile.discount.calculator                                                                 |1        |1          |1562         |
# |com.alibaba.aliexpresshd                                                                          |1        |0          |1545         |
# |com.wKUBOIKumpulanBelanjaOnlineIndonesia_9684052                                                  |0        |0          |1512         |
# |com.koinhiggsdomino.id                                                                            |0        |0          |1511         |
# |com.gumtree.android                                                                               |0        |0          |1487         |
# |com.onlineshoppingchina.chinashopping                                                             |0        |0          |1483         |
# |com.trd.lapakproperti                                                                             |0        |0          |1468         |
# |com.l                                                                                             |1        |1          |1459         |
# |com.ILJOVITA.android.universalwebview                                                             |1        |1          |1432         |
# |br.com.cea.appb2c                                                                                 |0        |0          |1387         |
# |com.application_4u.qrcode.barcode.scanner.reader.flashlight                                       |1        |1          |1374         |
# |com.icaali.flashsale                                                                              |0        |0          |1345         |
# |diamond.via.id                                                                                    |1        |1          |1313         |
# |com.appnana.android.giftcardrewards                                                               |0        |0          |1294         |
# |com.tokopedia.tkpd                                                                                |1        |0          |1276         |
# |freefire.id                                                                                       |1        |1          |1252         |
# |com.mygamezone.gamezone                                                                           |1        |1          |1251         |
# |com.shpock.android                                                                                |0        |0          |1243         |
# |com.app.goga                                                                                      |0        |0          |1231         |
# |develop.joo.scanbarcodeharga                                                                      |1        |1          |1231         |
# |com.wCodashopIndonesia_10208434                                                                   |0        |0          |1224         |
# |com.slidejoy                                                                                      |0        |0          |1215         |
# |com.mmb.qtenoffers                                                                                |0        |0          |1189         |
# |id.salestock.mobile                                                                               |0        |0          |1171         |
# |com.nsmobilehub                                                                                   |0        |0          |1169         |
# |com.Expandear.KatalogPromo                                                                        |1        |1          |1169         |
# |nl.marktplaats.android                                                                            |0        |0          |1163         |
# |com.zulily.android                                                                                |1        |1          |1163         |
# |com.contextlogic.wish                                                                             |1        |1          |1162         |
# |online.market                                                                                     |0        |0          |1161         |
# |web.id.isipulsa.appkita                                                                           |0        |0          |1137         |
# |com.mataharimall.mmandroid                                                                        |0        |0          |1134         |
# |evermos.evermos.com.evermos                                                                       |1        |1          |1133         |
# |com.chiphiggsdomino.app                                                                           |0        |0          |1123         |
# |com.alfamart.alfagift                                                                             |1        |1          |1106         |
# |com.myFinderiauOnline.FinderiauOnline                                                             |0        |0          |1082         |
# |id.salestock.mobile                                                                               |1        |1          |1081         |
# |com.itmobix.qataroffers                                                                           |0        |0          |1066         |
# |com.shopee.id                                                                                     |1        |0          |1049         |
# |com.cknb.hiddentagcop                                                                             |0        |0          |1038         |
# |com.aptivdev.carstoys                                                                             |0        |0          |1022         |
# |com.browsershopping.forgrouponcoupons                                                             |0        |0          |1008         |
# |com.mi.global.shop                                                                                |0        |0          |1007         |
# |com.shopee.co                                                                                     |0        |0          |960          |
# |com.supercute.shoppinglist                                                                        |0        |0          |955          |
# |com.ebates                                                                                        |0        |0          |949          |
# |toko.online.bayarditempat                                                                         |0        |0          |938          |
# |com.todayapp.cekresi                                                                              |1        |1          |913          |
# |com.tourbillon.freeappsnow                                                                        |0        |0          |913          |
# |com.bhanu.redeemerfree                                                                            |0        |0          |907          |
# |com.androidrocker.qrscanner                                                                       |1        |1          |905          |
# |com.cancctv.mobile                                                                                |0        |0          |901          |
# |com.anaonlineshopping.kidstoys                                                                    |0        |0          |891          |
# |com.ebay.kijiji.ca                                                                                |0        |0          |885          |
# |com.adfone.aditup                                                                                 |0        |0          |871          |
# |com.metro.foodbasics                                                                              |0        |0          |870          |
# |blibli.mobile.commerce                                                                            |1        |0          |865          |
# |com.iljovita.hargapromokatalog3                                                                   |1        |1          |861          |
# |sheproduction.coda.topup                                                                          |0        |0          |858          |
# |com.codmobil.mobilcod                                                                             |1        |1          |856          |
# |com.girls.fashion.entertainment.School.Girls.Sale.Day.Shopping.Makeup.Adventure                   |0        |0          |855          |
# |fr.leboncoin                                                                                      |0        |0          |840          |
# |it.doveconviene.android                                                                           |0        |0          |830          |
# |net.slickdeals.android                                                                            |0        |0          |820          |
# |com.mygodrivi.godrivi                                                                             |0        |0          |804          |
# |com.ebay.mobile                                                                                   |1        |1          |801          |
# |fr.deebee.calculsoldes                                                                            |0        |0          |791          |
# |com.star5studio.Homedesigner.houseDecor                                                           |0        |0          |771          |
# |com.wNasaStore_8607120                                                                            |0        |0          |765          |
# |com.myoryza.oryza                                                                                 |0        |0          |764          |
# |fashion.style.clothes.cheap                                                                       |0        |0          |763          |
# |com.my.exchangecounter                                                                            |1        |1          |752          |
# |com.newandromo.dev535927.app981756                                                                |0        |0          |744          |
# |com.mataharimall.mmandroid                                                                        |1        |1          |732          |
# |com.histore.indonesia                                                                             |0        |0          |728          |
# |id.yoyo.popslide.app                                                                              |0        |0          |727          |
# |com.bikinaplikasi.beliin.app                                                                      |0        |0          |727          |
# |com.shopee.mx                                                                                     |0        |0          |727          |
# |tw.taoyuan.qrcode.reader.barcode.scanner.flashlight                                               |0        |0          |722          |
# |shopping.indonesia.com.app                                                                        |1        |1          |719          |
# |com.contextlogic.wish                                                                             |0        |0          |710          |
# |cam.gadanie.hiromant                                                                              |0        |0          |699          |
# |com.wCodashopIndonesia_10208434                                                                   |1        |1          |698          |
# |com.kittoboy.shoppingmemo                                                                         |0        |0          |694          |
# |id.co.elevenia                                                                                    |0        |0          |694          |
# |tw.fancyapp.qrcode.barcode.scanner.reader.isbn.flashlight                                         |0        |0          |692          |
# |networld.price.app                                                                                |0        |0          |691          |
# |com.wKUBOIKumpulanBelanjaOnlineIndonesia_9684052                                                  |1        |1          |681          |
# |in.amazon.mShop.android.shopping                                                                  |0        |0          |673          |
# |com.opo.shopping.lazada                                                                           |0        |0          |671          |
# |com.zanyatocorp.checkbarcode                                                                      |1        |1          |671          |
# |com.onlineshoppingindonesia.indonesiashopping                                                     |1        |1          |666          |
# |com.nugros.arenatani                                                                              |0        |0          |665          |
# |id.co.acehardware.acerewards                                                                      |1        |1          |664          |
# |com.mrd.cekresipengiriman                                                                         |0        |0          |652          |
# |com.malaysiashopping.sonu                                                                         |0        |0          |650          |
# |com.sendo                                                                                         |0        |0          |649          |
# |com.alibaba.intl.android.apps.poseidon                                                            |1        |0          |632          |
# |com.amazon.mShop.android.shopping                                                                 |0        |0          |628          |
# |ng.jiji.app                                                                                       |0        |0          |626          |
# |com.oriflame.catalogue.maroc.asha                                                                 |0        |0          |621          |
# |com.joom                                                                                          |1        |1          |615          |
# |com.indopay.unipin                                                                                |0        |0          |611          |
# |com.app.goga                                                                                      |1        |1          |609          |
# |universal.app.cek.resi.shopee.express                                                             |1        |1          |608          |
# |asia.bluepay.clientin                                                                             |1        |1          |608          |
# |net.agusharyanto.catatbelanja                                                                     |0        |0          |607          |
# |com.tokopedia.kelontongapp                                                                        |1        |1          |601          |
# |com.tycmrelx.cash                                                                                 |1        |1          |601          |
# |toko.online.terpercaya                                                                            |1        |1          |595          |
# |com.wKLIKGAMESHOPAdalahMitraAgenChipHiggsDominoIsland_13240116                                    |1        |1          |591          |
# |com.cannotbeundone.amazonbarcodescanner                                                           |0        |0          |584          |
# |com.online.smart.bracelet.watch.shop.oss                                                          |0        |0          |584          |
# |com.tokopedia.sellerapp                                                                           |1        |1          |582          |
# |com.toko.higgsdomino                                                                              |0        |0          |576          |
# |com.thirdrock.fivemiles                                                                           |0        |0          |574          |
# |com.adfone.indosat                                                                                |1        |0          |570          |
# |com.boulla.cellphone                                                                              |0        |0          |568          |
# |com.zilingo.users                                                                                 |1        |1          |567          |
# |com.sahibinden                                                                                    |0        |0          |562          |
# |goosecreekco.android.app                                                                          |0        |0          |560          |
# |com.interfocusllc.patpat                                                                          |1        |1          |557          |
# |com.overstock                                                                                     |0        |0          |557          |
# |com.shopee.th                                                                                     |1        |1          |553          |
# |com.tokopedia.kelontongapp                                                                        |0        |0          |547          |
# |com.onlinecoupons.shopee                                                                          |0        |0          |547          |
# |com.aptivdev.buykidstoys                                                                          |0        |0          |543          |
# |com.cupang.ads                                                                                    |0        |0          |542          |
# |co.kr.ezapp.shoppinglist                                                                          |0        |0          |540          |
# |com.allgoritm.youla                                                                               |0        |0          |539          |
# |com.appscentral.free_toys_online                                                                  |0        |0          |538          |
# |com.maulanatriharja.catatanbelanja                                                                |0        |0          |537          |
# |com.bitwize10.supersimpleshoppinglist                                                             |0        |0          |534          |
# |com.wallapop                                                                                      |0        |0          |533          |
# |com.wajual                                                                                        |0        |0          |532          |
# |com.minhtinh.daftarbelanja                                                                        |0        |0          |532          |
# |com.appscentral.free_anti_stress                                                                  |0        |0          |532          |
# |com.crown.indonesia                                                                               |0        |0          |531          |
# |fr.casino.fidelite                                                                                |1        |1          |526          |
# |co.shopney.lelopak                                                                                |0        |0          |526          |
# |com.jarfernandez.discountcalculator                                                               |0        |0          |524          |
# |com.biggu.shopsavvy                                                                               |1        |1          |523          |
# |com.boulla.comparephones                                                                          |0        |0          |523          |
# |com.pasti.terjual                                                                                 |0        |0          |522          |
# |com.shoppingapp.shopeeeasa                                                                        |1        |1          |515          |
# |com.metalsoft.trackchecker_mobile                                                                 |0        |0          |513          |
# |com.Spark.ESPpark                                                                                 |0        |0          |511          |
# |com.trd.lapakproperti                                                                             |1        |1          |509          |
# |com.koinhiggsdomino.id                                                                            |1        |1          |505          |
# |com.capigami.outofmilk                                                                            |0        |0          |502          |
# |com.alfamart.alfagift                                                                             |0        |0          |502          |
# |jual.belibarangbekas                                                                              |0        |0          |499          |
# |com.andromo.dev786963.app908281                                                                   |1        |1          |497          |
# |com.camera.translate.languages                                                                    |0        |0          |492          |
# |com.OfficialAppsDev.CodanyaShopPro                                                                |0        |0          |489          |
# |indopub.unipinpro.topup                                                                           |1        |1          |489          |
# |com.onlineshoppingchina.chinashopping                                                             |1        |1          |487          |
# |hajigaming.codashop.malaysia                                                                      |1        |1          |480          |
# |com.whaff.whaffapp                                                                                |0        |0          |480          |
# |com.kinoli.couponsherpa                                                                           |0        |0          |480          |
# |id.co.elevenia                                                                                    |1        |1          |477          |
# |ru.auto.ara                                                                                       |0        |0          |477          |
# |com.ww1688ProductsAllLanguage_11962618                                                            |0        |0          |474          |
# |com.georgeparky.thedroplist                                                                       |0        |0          |470          |
# |com.amazon.mShop.android.shopping                                                                 |1        |1          |470          |
# |com.opensooq.OpenSooq                                                                             |0        |0          |468          |
# |com.pozitron.hepsiburada                                                                          |0        |0          |467          |
# |net.tsapps.appsales                                                                               |1        |1          |465          |
# |shopping.express.sales.ali                                                                        |0        |0          |462          |
# |id.com.android.javapay                                                                            |0        |0          |461          |
# |com.tiktokshop.tiktokshop                                                                         |1        |1          |459          |
# |com.vova.android                                                                                  |0        |0          |457          |
# |it.doveconviene.android                                                                           |1        |1          |454          |
# |com.on9store.shopping.allpromotion                                                                |1        |1          |453          |
# |com.shopback.app                                                                                  |0        |0          |452          |
# |tn.websol.adsblackfriday2                                                                         |0        |0          |450          |
# |online.market                                                                                     |1        |1          |450          |
# |com.mhn.ponta                                                                                     |1        |1          |448          |
# |com.disqonin                                                                                      |0        |0          |447          |
# |com.myFinderiauOnline.FinderiauOnline                                                             |1        |1          |444          |
# |online.malaysia.shopping                                                                          |0        |0          |436          |
# |com.globalbusiness.countrychecker                                                                 |1        |1          |430          |
# |com.indomaret.klikindomaret                                                                       |1        |1          |427          |
# |de.autodoc.gmbh                                                                                   |0        |0          |426          |
# |com.wind.shopping.lazada                                                                          |0        |0          |422          |
# |com.thecarousell.Carousell                                                                        |1        |0          |422          |
# |com.dusdusan.katalog                                                                              |0        |0          |419          |
# |com.mob91                                                                                         |0        |0          |417          |
# |app.shopping.ali.express                                                                          |0        |0          |417          |
# |com.dhgate.buyermob                                                                               |1        |1          |413          |
# |app.ndtv.matahari                                                                                 |1        |1          |411          |
# |id.triliun.sobatpromo                                                                             |0        |0          |410          |
# |com.onlineshoppingjapan.japanshopping                                                             |0        |0          |405          |
# |com.itmobix.kwendeals                                                                             |0        |0          |405          |
# |com.trkstudio.currentproducts                                                                     |0        |0          |401          |
# |web.id.isipulsa.appkita                                                                           |1        |1          |398          |
# |com.premiumappsfactory.popitbag                                                                   |0        |0          |397          |
# |com.onlineshoppingsingapore.singaporeshopping                                                     |0        |0          |396          |
# |app.tsumuchan.frima_search                                                                        |0        |0          |394          |
# |paStudio.pharaoh.diamondpuzzlecall                                                                |0        |0          |394          |
# |com.myattausiltransaksional.attausiltransaksional                                                 |0        |0          |394          |
# |com.chinashoping.sonu                                                                             |0        |0          |393          |
# |com.offerup                                                                                       |1        |1          |390          |
# |com.myoryza.oryza                                                                                 |1        |1          |390          |
# |app.marketplace.mokamall                                                                          |0        |0          |389          |
# |com.mangbelanjakeun                                                                               |0        |0          |389          |
# |com.Alltor.YoUnipin                                                                               |0        |0          |385          |
# |com.online.shoppingchina                                                                          |0        |0          |385          |
# |com.myglamm.ecommerce                                                                             |0        |0          |384          |
# |com.enuri.android                                                                                 |0        |0          |384          |
# |com.midasmind.app.mmgocarmm                                                                       |0        |0          |382          |
# |com.ChinaShoppingOnline                                                                           |0        |0          |379          |
# |com.ae.ae                                                                                         |0        |0          |378          |
# |com.chiphiggsdomino.app                                                                           |1        |1          |375          |
# |com.xbox_deals.sales                                                                              |1        |1          |374          |
# |com.ricosti.gazetka                                                                               |0        |0          |374          |
# |com.rucksack.barcodescannerforwalmart                                                             |0        |0          |371          |
# |com.appscentral.glowpopit                                                                         |0        |0          |371          |
# |com.banggood.client                                                                               |1        |1          |368          |
# |com.elz.secondhandstore                                                                           |0        |0          |368          |
# |com.giftmill.giftmill                                                                             |1        |1          |368          |
# |com.histore.indonesia                                                                             |1        |1          |367          |
# |com.geekeryapps.cartoys.remotecontrolcars                                                         |0        |0          |367          |
# |com.appmyshop                                                                                     |0        |0          |353          |
# |com.iassist.nurraysa                                                                              |0        |0          |353          |
# |com.chainreactioncycles.hybrid                                                                    |0        |0          |353          |
# |toko.online.bayarditempat                                                                         |1        |1          |352          |
# |com.applazada.promo                                                                               |0        |0          |351          |
# |gsshop.mobile.v2                                                                                  |0        |0          |349          |
# |mobi.mobileforce.informa                                                                          |1        |1          |347          |
# |com.rucksack.barcodescannerforebay                                                                |0        |0          |342          |
# |com.glitterapps.plushie                                                                           |0        |0          |341          |
# |com.mobile.cover.photo.editor.back.maker                                                          |0        |0          |341          |
# |com.insightquest.snooper                                                                          |0        |0          |338          |
# |best.deals.compare.coupons.discounts.tracker.assistant.alert                                      |0        |0          |336          |
# |com.stGalileo.JewelCurse                                                                          |0        |0          |332          |
# |com.klink.kmart                                                                                   |0        |0          |329          |
# |com.ionicframework.sp262624                                                                       |0        |0          |327          |
# |yotcho.shop                                                                                       |1        |1          |327          |
# |id.co.shopintar                                                                                   |0        |1          |327          |
# |tempat.jual.beli.hewan.peliharaan                                                                 |0        |0          |325          |
# |com.shopee.my                                                                                     |1        |1          |322          |
# |com.olx.pk                                                                                        |0        |0          |322          |
# |com.pocky.ztime                                                                                   |0        |0          |321          |
# |com.wBGShopee_9071949                                                                             |0        |0          |320          |
# |com.myhiggsdominotips.higgsdominotips                                                             |0        |0          |320          |
# |com.summerseger.katalogRabbaniHijabGamis                                                          |0        |0          |318          |
# |com.groupon                                                                                       |0        |0          |317          |
# |com.myntra.android                                                                                |0        |0          |314          |
# |com.schibsted.bomnegocio.androidApp                                                               |0        |0          |313          |
# |com.nicekicks.rn.android                                                                          |0        |0          |311          |
# |com.tokopedia.sellerapp                                                                           |0        |0          |307          |
# |com.mobil123.www                                                                                  |1        |1          |304          |
# |com.icaali.flashsale                                                                              |1        |1          |301          |
# |com.rmtheis.price.comparison.scanner                                                              |0        |0          |298          |
# |com.olxmena.horizontal                                                                            |0        |0          |298          |
# |com.xyz.alihelper                                                                                 |1        |1          |297          |
# |main.ClicFlyer                                                                                    |0        |0          |297          |
# |com.wNasaStore_8607120                                                                            |1        |1          |294          |
# |com.crocs.app                                                                                     |0        |0          |292          |
# |com.xubnaha.florame                                                                               |0        |0          |289          |
# |com.orbstudio.daftartiktokshop                                                                    |0        |0          |289          |
# |com.malaysiashopping.onlineshoppingmalaysiaapp                                                    |0        |0          |288          |
# |com.panagola.app.shopcalc                                                                         |0        |0          |287          |
# |com.flipkart.android                                                                              |0        |0          |287          |
# |com.couponscodes.dealsdiscounts.forLazada                                                         |0        |0          |286          |
# |app.oneprice                                                                                      |0        |0          |284          |
# |com.idbuysell.fff                                                                                 |0        |0          |284          |
# |id.yoyo.popslide.app                                                                              |1        |1          |283          |
# |sheproduction.coda.topup                                                                          |1        |1          |280          |
# |com.cknb.hiddentagcop                                                                             |1        |1          |278          |
# |com.appscentral.popit                                                                             |0        |0          |273          |
# |com.cancctv.mobile                                                                                |1        |1          |271          |
# |com.supercute.shoppinglist                                                                        |1        |1          |271          |
# |my.com.thefoodmerchant.app                                                                        |0        |0          |269          |
# |com.appscentral.sd                                                                                |0        |0          |267          |
# |com.shell.sitibv.shellgoplusindia                                                                 |0        |0          |267          |
# |com.meihillman.qrbarcodescanner                                                                   |0        |0          |266          |
# |com.onlineshoppingthailand.thailandshoppingapp                                                    |0        |0          |266          |
# |id.co.acehardware.acerewards                                                                      |0        |0          |266          |
# |com.ksl.android.classifieds                                                                       |0        |0          |266          |
# |zuper.market13                                                                                    |0        |0          |265          |
# |com.google.zxing.client.android                                                                   |1        |1          |265          |
# |de.super                                                                                          |0        |0          |264          |
# |com.tourbillon.freeappsnow                                                                        |1        |1          |263          |
# |jp.jmty.app2                                                                                      |0        |0          |263          |
# |aw.kiriman                                                                                        |1        |1          |263          |
# |com.digital.shopping.apps.india.lite                                                              |0        |0          |257          |
# |com.alfamart.katalog.flyers                                                                       |0        |0          |255          |
# |com.appsfree.android                                                                              |0        |0          |255          |
# |com.sendo                                                                                         |1        |1          |252          |
# |nz.co.trademe.trademe                                                                             |0        |0          |251          |
# |com.codified.hipyard                                                                              |0        |0          |251          |
# |lk.ikman                                                                                          |0        |0          |247          |
# |com.newandromo.dev535927.app1163168                                                               |0        |0          |247          |
# |mypoin.indomaret.android                                                                          |1        |1          |246          |
# |com.oriflame.catalogue.maroc.asha                                                                 |1        |1          |244          |
# |com.korchix.chineoapp                                                                             |0        |0          |243          |
# |com.lootboy.app                                                                                   |1        |1          |242          |
# |com.appscentral.squishy_shop                                                                      |0        |0          |242          |
# |ru.ifsoft.mymarketplace                                                                           |0        |0          |242          |
# |com.barcodelookup                                                                                 |0        |0          |236          |
# |com.koindominoisland.app                                                                          |0        |0          |236          |
# |com.newandromo.dev535927.app777705                                                                |0        |0          |236          |
# |com.ozen.alisverislistesi                                                                         |0        |0          |234          |
# |com.nugros.arenatani                                                                              |1        |1          |233          |
# |epasar.ecommerce.app                                                                              |0        |0          |232          |
# |com.whaff.whaffapp                                                                                |1        |1          |232          |
# |com.adfone.aditup                                                                                 |1        |1          |232          |
# |com.toko.higgsdomino                                                                              |1        |1          |231          |
# |com.appscentral.popitcase                                                                         |1        |1          |230          |
# |com.ibotta.android                                                                                |0        |0          |228          |
# |com.univplay.appreward                                                                            |0        |0          |228          |
# |com.c51                                                                                           |0        |0          |227          |
# |com.snapdeal.main                                                                                 |0        |0          |227          |
# |tw.taoyuan.qrcode.reader.barcode.scanner.flashlight                                               |1        |1          |227          |
# |com.girls.fashion.entertainment.School.Girls.Sale.Day.Shopping.Makeup.Adventure                   |1        |1          |226          |
# |com.tikshop.tikshop                                                                               |1        |1          |223          |
# |com.yoshop                                                                                        |0        |0          |223          |
# |id.com.android.nikimobile                                                                         |0        |0          |222          |
# |fr.deebee.calculsoldes                                                                            |1        |1          |220          |
# |com.cari.promo.diskon                                                                             |1        |0          |219          |
# |com.bikinaplikasi.beliin.app                                                                      |1        |1          |218          |
# |com.dusdusan.katalog                                                                              |1        |1          |218          |
# |com.rucksack.pricecomparisonforamazonebay                                                         |0        |0          |217          |
# |com.light.paidappssales                                                                           |0        |0          |217          |
# |com.browsershopping.forgrouponcoupons                                                             |1        |1          |216          |
# |com.appnana.android.giftcardrewards                                                               |1        |1          |216          |
# |ua.slando                                                                                         |0        |0          |216          |
# |com.radipgo.katalogdiskon                                                                         |0        |0          |216          |
# |com.thekrazycouponlady.kcl                                                                        |0        |0          |216          |
# |com.listonic.sales.deals.weekly.ads                                                               |0        |0          |215          |
# |chinese.goods.online.cheap                                                                        |0        |0          |214          |
# |com.bikinaplikasi.bjcell                                                                          |0        |0          |213          |
# |com.mobileshop.usa                                                                                |0        |0          |212          |
# |com.tmon                                                                                          |0        |0          |212          |
# |ru.ozon.app.android                                                                               |0        |0          |211          |
# |com.douya.e_commercepart_time                                                                     |0        |0          |210          |
# |com.toycar_rccars_shoppingonline.carstoys                                                         |0        |0          |209          |
# |com.onlineshopping.malaysiashopping                                                               |0        |0          |209          |
# |com.numatams.numatamsapps.discounttaxcalc                                                         |0        |0          |207          |
# |com.star5studio.Homedesigner.houseDecor                                                           |1        |1          |206          |
# |com.vybesxapp                                                                                     |0        |0          |206          |
# |com.malaysiashopping.sonu                                                                         |1        |1          |205          |
# |br.com.lardev.android.rastreiocorreios                                                            |0        |0          |205          |
# |com.biglotsshop.online                                                                            |0        |0          |205          |
# |io.silvrr.installment                                                                             |1        |0          |205          |
# |com.asyncbyte.wishlist                                                                            |0        |0          |205          |
# |com.gazetki.gazetki                                                                               |0        |0          |200          |
# |com.amazon.aa                                                                                     |1        |1          |200          |
# |com.anindyacode.cekharga                                                                          |0        |0          |198          |
# |com.codylab.halfprice                                                                             |0        |0          |198          |
# |app1688.apyuw                                                                                     |0        |0          |197          |
# |asia.bluepay.clientin                                                                             |0        |0          |197          |
# |com.goodbarber.seniorfree                                                                         |0        |0          |195          |
# |com.appscentral.free_squishy_shop                                                                 |0        |0          |195          |
# |com.uniqlo.id.catalogue                                                                           |1        |1          |195          |
# |com.newchic.client                                                                                |1        |0          |192          |
# |com.newandromo.dev535927.app836388                                                                |0        |0          |192          |
# |com.alfacart.apps                                                                                 |1        |1          |192          |
# |com.oliks.G.til.freo                                                                              |0        |0          |191          |
# |tw.fancyapp.qrcode.barcode.scanner.reader.isbn.flashlight                                         |1        |1          |191          |
# |com.roblox.robux.appreward.robux                                                                  |0        |0          |190          |
# |fashion.shop.clothes.dresses                                                                      |0        |0          |190          |
# |prices.in.china                                                                                   |0        |0          |190          |
# |com.ayopop                                                                                        |1        |1          |190          |
# |com.brandicorp.brandi3                                                                            |0        |0          |189          |
# |com.aptivdev.remotecontroltoys                                                                    |1        |1          |189          |
# |com.mycikalongwetanmarkets.cikalongwetanmarkets                                                   |0        |0          |189          |
# |com.tksolution.einkaufszettelmitspracheingabe                                                     |0        |0          |188          |
# |com.roidtechnologies.appbrowzer                                                                   |0        |0          |187          |
# |com.bunniimarketplace                                                                             |0        |0          |186          |
# |com.bikinaplikasi.gadgetgrosir                                                                    |0        |0          |186          |
# |in.uknow.onlineshopping                                                                           |0        |0          |186          |
# |com.icedblueberry.shoppinglisteasy                                                                |0        |0          |186          |
# |com.kohls.mcommerce.opal                                                                          |1        |1          |186          |
# |de.roller.twa                                                                                     |0        |0          |185          |
# |com.onlineshoppingphilippines.philippinesshopping                                                 |0        |0          |185          |
# |com.moramsoft.ppomppualarm                                                                        |0        |0          |183          |
# |ru.grocerylist.android                                                                            |0        |0          |183          |
# |com.teqnidev.paidappsfree.pro                                                                     |0        |0          |182          |
# |com.whaleshark.retailmenot                                                                        |1        |1          |182          |
# |com.militarycompany.helicoptermod                                                                 |0        |0          |181          |
# |com.shopping.online.id                                                                            |0        |0          |181          |
# |com.myattausiltransaksional.attausiltransaksional                                                 |1        |1          |181          |
# |com.costco.app.android                                                                            |0        |0          |180          |
# |com.dadidoo.catatanbelanja                                                                        |0        |0          |179          |
# |com.my.codaccounts                                                                                |0        |0          |178          |
# |com.cupang.ads                                                                                    |1        |1          |178          |
# |com.ebates                                                                                        |1        |1          |175          |
# |tempat.jual.beli.hewan.peliharaan                                                                 |1        |1          |174          |
# |com.b2w.americanas                                                                                |0        |0          |174          |
# |com.ebay.gumtree.au                                                                               |1        |1          |174          |
# |com.sinworldnlineshop.shop                                                                        |0        |0          |172          |
# |com.jaknot                                                                                        |1        |1          |172          |
# |com.my.cocpostit                                                                                  |1        |1          |172          |
# |com.mygodrivi.godrivi                                                                             |1        |1          |171          |
# |com.titansModforMcpe.AttackonTitanmod                                                             |0        |0          |171          |
# |com.sayurbox                                                                                      |1        |1          |170          |
# |online.malaysia.shopping                                                                          |1        |1          |170          |
# |net.agusharyanto.catatbelanja                                                                     |1        |1          |169          |
# |com.anaonlineshopping.bra_womenunderwear                                                          |0        |0          |168          |
# |com.froogloid.kring.google.zxing.client.android                                                   |0        |0          |168          |
# |com.biglotsapp.shop                                                                               |0        |0          |168          |
# |com.pasti.terjual                                                                                 |1        |1          |167          |
# |com.shopee.in                                                                                     |0        |0          |167          |
# |com.navigationproap.drivingvalleyrouteap.navigationrouteap                                        |0        |0          |166          |
# |ru.auto.ara                                                                                       |1        |1          |165          |
# |com.shipt.groceries                                                                               |0        |0          |164          |
# |com.glitterapps.popitcase                                                                         |0        |0          |164          |
# |com.andromo.dev630323.app739765                                                                   |0        |0          |164          |
# |com.malaysiashopping.onlineshoppingmalaysiaapp                                                    |1        |1          |163          |
# |barcode.qr.scanner                                                                                |0        |0          |163          |
# |com.bikinaplikasi.yandhika                                                                        |0        |0          |163          |
# |com.geekonweb.MalaysiaShoppingApp                                                                 |0        |0          |162          |
# |com.wToyStoryToys_12242673                                                                        |0        |0          |162          |
# |com.fivejack.itemku                                                                               |1        |1          |161          |
# |com.mymofi.mofi                                                                                   |0        |0          |160          |
# |com.bukalapak.mitra                                                                               |1        |0          |160          |
# |com.glitterapps.stress                                                                            |0        |0          |160          |
# |com.ebay.kleinanzeigen                                                                            |1        |1          |159          |
# |com.newandromo.dev535927.app834897                                                                |0        |0          |158          |
# |appinventor.ai_lightworld813.bodysize                                                             |0        |0          |158          |
# |com.amazon.aa                                                                                     |0        |0          |157          |
# |com.promocodes.couponsforlazada                                                                   |0        |0          |156          |
# |com.rmystudio.budlist                                                                             |0        |0          |155          |
# |com.mattceonzo.ultimatewishlist                                                                   |0        |0          |155          |
# |com.camera.translate.languages                                                                    |1        |1          |154          |
# |com.andromo.dev276588.app308783                                                                   |0        |0          |154          |
# |com.JOS.Japan.Online.Shopping                                                                     |0        |0          |154          |
# |com.snapcart.android                                                                              |0        |0          |152          |
# |com.kittoboy.shoppingmemo                                                                         |1        |1          |152          |
# |club.fromfactory                                                                                  |0        |0          |152          |
# |com.glitterapps.popitfree                                                                         |0        |0          |152          |
# |com.prolight.prolight                                                                             |0        |0          |151          |
# |com.minhtinh.daftarbelanja                                                                        |1        |1          |150          |
# |com.summerseger.katalogRabbaniHijabGamis                                                          |1        |1          |150          |
# |com.memiles.app                                                                                   |0        |0          |149          |
# |it.subito                                                                                         |0        |0          |147          |
# |com.agusharyanto.net.discountcalculator                                                           |0        |0          |147          |
# |com.mrd.cekresipengiriman                                                                         |1        |1          |146          |
# |id.kitabeli.mitra                                                                                 |0        |0          |146          |
# |com.onlineshoppingthailand.thailandshoppingapp                                                    |1        |1          |146          |
# |com.kolaku.android                                                                                |0        |0          |145          |
# |com.merchant.histore                                                                              |0        |0          |145          |
# |com.onlineshoppingjapan.japanshopping                                                             |1        |1          |144          |
# |com.snapcart.android                                                                              |1        |1          |144          |
# |com.myshopedia.shopediamarket                                                                     |0        |0          |144          |
# |com.bikroy                                                                                        |0        |0          |144          |
# |com.glitterapps.simple                                                                            |0        |0          |144          |
# |com.inovasiteknologi.a101.onglaistore                                                             |0        |0          |143          |
# |com.aptivdev.carstoys                                                                             |1        |1          |142          |
# |com.supercute.rumahmurah                                                                          |0        |0          |142          |
# |ng.jiji.app                                                                                       |1        |1          |141          |
# |com.oriflame.oriflame                                                                             |1        |1          |141          |
# |com.rusbuket                                                                                      |0        |0          |141          |
# |com.onlineshoppingsingapore.singaporeshopping                                                     |1        |1          |140          |
# |jp.co.unisys.android.yamadamobile                                                                 |0        |0          |139          |
# |net.slickdeals.android                                                                            |1        |1          |139          |
# |usbobble.ssgame.fruitMatchRealizeDream                                                            |0        |0          |138          |
# |asia.acommerce.adidasid                                                                           |1        |1          |138          |
# |com.g2018.hfcoupons                                                                               |0        |0          |138          |
# |com.maulanatriharja.catatanbelanja                                                                |1        |1          |136          |
# |com.mhn.ponta                                                                                     |0        |0          |135          |
# |com.barcodescanner.qrreader                                                                       |0        |0          |135          |
# |kr.co.dreamshopping.mcapp                                                                         |0        |0          |135          |
# |com.infohargamobil                                                                                |0        |0          |133          |
# |benefitmedia.android.sparpionier                                                                  |0        |0          |133          |
# |com.shopclues                                                                                     |0        |0          |133          |
# |com.sayuran.online                                                                                |0        |0          |133          |
# |com.sonyericsson.pontofrio                                                                        |0        |0          |133          |
# |com.ZAM.AgenKosmetikMurahIndonesia                                                                |0        |0          |132          |
# |com.zeptoconsumerapp                                                                              |0        |0          |131          |
# |com.visionsmarts.pic2shop                                                                         |0        |0          |131          |
# |com.pricecheckscanner                                                                             |0        |0          |131          |
# |com.ChinaShoppingOnline                                                                           |1        |1          |130          |
# |uk.vinted                                                                                         |0        |0          |130          |
# |com.sugar_lipstick_makeup_set_makeup_brushes_sale.cosmetics_makeup_face_powder_compact_powder_sale|0        |0          |130          |
# |com.anaonlineshopping.kidstoys                                                                    |1        |1          |130          |
# |in.amazon.mShop.android.shopping                                                                  |1        |1          |129          |
# |womens.cheap.clothes                                                                              |0        |0          |129          |
# |com.machtwatch_mobile                                                                             |1        |1          |129          |
# |com.taobao.htao.android                                                                           |0        |0          |129          |
# |com.blanja.apps.android                                                                           |1        |1          |128          |
# |com.bitwize10.supersimpleshoppinglist                                                             |1        |1          |126          |
# |com.fgf.shopping.game                                                                             |0        |0          |126          |
# |com.toko.online.net                                                                               |0        |0          |126          |
# |com.buymeapie.bmap                                                                                |0        |0          |126          |
# |com.univplay.appreward                                                                            |1        |1          |125          |
# |com.app.tokobagus.betterb                                                                         |0        |1          |124          |
# |id.triliun.sobatpromo                                                                             |1        |1          |123          |
# |jual.belibarangbekas                                                                              |1        |1          |123          |
# |com.glitterapps.fidget                                                                            |0        |0          |123          |
# |com.wLapakLaris_13337038                                                                          |0        |0          |123          |
# |com.wBGShopee_9071949                                                                             |1        |1          |123          |
# |com.ionicframework.sp262624                                                                       |1        |1          |123          |
# |com.disqonin                                                                                      |1        |1          |122          |
# |com.stGalileo.JewelCurse                                                                          |1        |1          |122          |
# |com.bukalapak.android                                                                             |1        |0          |122          |
# |com.wajual                                                                                        |1        |1          |122          |
# |com.komorebi.shoppinglist                                                                         |0        |0          |121          |
# |com.rexsolution.golfreshmarket                                                                    |0        |0          |121          |
# |com.memiles.app                                                                                   |1        |1          |121          |
# |nl.marktplaats.android                                                                            |1        |1          |119          |
# |com.kaskus.fjb                                                                                    |1        |1          |119          |
# |com.chinashoping.sonu                                                                             |1        |1          |118          |
# |ba.pik.v2.android                                                                                 |0        |0          |118          |
# |com.stylish.font.free                                                                             |0        |0          |118          |
# |one.track.app                                                                                     |0        |0          |118          |
# |com.barcode.scanner.qr.scanner.app.qr.code.reader                                                 |0        |0          |117          |
# |com.google.zxing.client.android                                                                   |0        |0          |115          |
# |com.wkendarijualbeli_9915700                                                                      |0        |0          |115          |
# |id.co.ikea.android                                                                                |1        |1          |115          |
# |kids.onlineshopping                                                                               |0        |0          |115          |
# |com.jakmall                                                                                       |1        |1          |114          |
# |mobi.mobileforce.informa                                                                          |0        |0          |114          |
# |com.indonesiashopping.sonu                                                                        |0        |0          |114          |
# |air.tMinis                                                                                        |0        |0          |114          |
# |com.hartono.elektronika                                                                           |1        |1          |113          |
# |com.onlineshoppingkorea.koreashopping                                                             |0        |0          |112          |
# |com.vybesxapp                                                                                     |1        |1          |112          |
# |com.gsretail.android.smapp                                                                        |0        |0          |111          |
# |com.mourjan.classifieds                                                                           |0        |0          |111          |
# |com.GomoGameDev.TopUpdanCaraBayarCodashop                                                         |0        |0          |111          |
# |pl.tablica                                                                                        |0        |0          |111          |
# |com.withsellit.app.sellit                                                                         |0        |0          |110          |
# |com.alimede.easyshopping                                                                          |0        |0          |110          |
# |com.handyapps.discountcalc                                                                        |0        |0          |110          |
# |com.onlineshopping.malaysiashopping                                                               |1        |1          |110          |
# |com.star5studio.HouseDecorationPlan.diamondHSLegend                                               |0        |0          |109          |
# |chinese.goods.online.cheap                                                                        |1        |1          |107          |
# |com.wSIPLAHKamdikbud_10759627                                                                     |0        |0          |107          |
# |com.newandromo.dev535927.app981756                                                                |1        |1          |107          |
# |com.ww1688ProductsAllLanguage_11962618                                                            |1        |1          |107          |
# |jp.co.fablic.fril                                                                                 |0        |0          |106          |
# |com.selloship.thedopeshop                                                                         |0        |0          |106          |
# |com.wKSMForMember_13916114                                                                        |0        |0          |106          |
# |com.ionicframework.kalkulatorbelanja439506                                                        |0        |0          |106          |
# |com.situs.kuliner                                                                                 |0        |0          |106          |
# |com.bhanu.redeemerfree                                                                            |1        |1          |106          |
# |shopping.mobile.com.products                                                                      |0        |0          |104          |
# |com.erafone.bct.lk6                                                                               |1        |1          |103          |
# |com.wESGOKAJEN_12352637                                                                           |0        |0          |103          |
# |com.mybandungbarat.bandungbarat                                                                   |0        |0          |102          |
# |com.crown.indonesia                                                                               |1        |1          |102          |
# |com.myway.omr4                                                                                    |0        |0          |102          |
# |com.myhighdominojackpotberuntung.highdominojackpotberuntung                                       |0        |0          |101          |
# |com.banggood.client                                                                               |0        |0          |100          |
# |com.headcode.ourgroceries                                                                         |0        |0          |100          |
# |com.mobile.cover.photo.editor.back.maker                                                          |1        |1          |99           |
# |com.andromo.dev707753.app828918                                                                   |0        |0          |99           |
# |com.chotot.vn                                                                                     |0        |0          |99           |
# |philippines.shopping.online.app                                                                   |0        |0          |99           |
# |com.airjordandeals.android                                                                        |0        |0          |98           |
# |online.thailand.shopping                                                                          |0        |0          |98           |
# |com.toreast.app                                                                                   |0        |0          |98           |
# |com.rodsapps.poptoshop.app                                                                        |0        |0          |98           |
# |com.online.shoppingchina                                                                          |1        |1          |98           |
# |ru.ifsoft.mymarketplace                                                                           |1        |1          |97           |
# |com.indopay.unipin                                                                                |1        |1          |96           |
# |com.bf.app0def94                                                                                  |0        |0          |96           |
# |com.roblox.robux.appreward.robux                                                                  |1        |1          |95           |
# |com.boulla.comparephones                                                                          |1        |1          |95           |
# |online.shoppingmarketplace                                                                        |0        |0          |95           |
# |com.appnew.ubery2019orderinginstructions                                                          |0        |0          |95           |
# |com.swiftspeedappcreator.android5ec91dd9d0622                                                     |0        |0          |94           |
# |com.B2B.Marketplace.PasarDigital.UMKMBUMN                                                         |0        |0          |94           |
# |com.manash.purplle                                                                                |0        |0          |94           |
# |com.wTopkoin_12421106                                                                             |0        |0          |93           |
# |com.indomaret.klikindomaret                                                                       |0        |0          |92           |
# |com.lelong.buyer                                                                                  |0        |0          |92           |
# |com.mmstore.apps                                                                                  |0        |0          |92           |
# |com.mobileappruparupa                                                                             |1        |1          |92           |
# |com.summerseger.katalogZoyaHijabGamis                                                             |0        |0          |91           |
# |shopping.list.free.lista.compra.gratis.liston                                                     |0        |0          |91           |
# |com.cannotbeundone.amazonbarcodescanner                                                           |1        |1          |91           |
# |com.tribunnews.tribunjualbeli                                                                     |0        |0          |90           |
# |app.marketplace.mokamall                                                                          |1        |1          |89           |
# |com.opo.shopping.lazada                                                                           |1        |1          |89           |
# |com.dolap.android                                                                                 |0        |0          |89           |
# |com.trd.lapakmobkas                                                                               |0        |0          |89           |
# |com.stickman.spider.rope.hero.gangstar.city                                                       |0        |0          |89           |
# |com.tokohiggs.domino                                                                              |0        |0          |88           |
# |com.trkstudio.currentproducts                                                                     |1        |1          |88           |
# |com.ionicframework.avsi204446                                                                     |0        |0          |88           |
# |mypoin.indomaret.android                                                                          |0        |0          |88           |
# |co.kr.ezapp.shoppinglist                                                                          |1        |1          |88           |
# |f2game.gemsRoad.pirateDiamondPuzzle                                                               |0        |0          |87           |
# |com.crispysoft.deliverycheck                                                                      |0        |0          |87           |
# |com.ZAM.AgenKosmetikTermurahJakarta                                                               |0        |0          |86           |
# |ru.zenden.android                                                                                 |0        |0          |86           |
# |com.slidejoy                                                                                      |1        |1          |86           |
# |com.glitterapps.glow                                                                              |0        |0          |86           |
# |com.jasonmg.simsale                                                                               |0        |0          |86           |
# |com.ballbek.flying.shopping.mall.driver                                                           |0        |0          |86           |
# |com.toys_kids_cheap.remotecontroltoys                                                             |0        |0          |85           |
# |com.mobil123.www                                                                                  |0        |0          |85           |
# |com.zzkko                                                                                         |0        |0          |84           |
# |com.merchant.histore                                                                              |1        |1          |84           |
# |test.aplivjl                                                                                      |0        |0          |84           |
# |com.summerseger.daftarhargaayam                                                                   |0        |0          |84           |
# |com.luck_d                                                                                        |0        |0          |83           |
# |com.georgeparky.thedroplist                                                                       |1        |1          |83           |
# |com.allinonestudio.chinedoapp                                                                     |0        |0          |83           |
# |com.coupang.mobile                                                                                |0        |0          |83           |
# |com.oliks.G.til.freo                                                                              |1        |1          |83           |
# |com.noel.shop                                                                                     |0        |0          |83           |
# |app.brickseek.com.brickseekapp                                                                    |0        |0          |82           |
# |com.bikinaplikasi.bjcell                                                                          |1        |1          |82           |
# |com.umjjal.gif.maker                                                                              |0        |0          |81           |
# |com.chestersw.foodlist                                                                            |0        |0          |81           |
# |com.itmobix.qataroffers                                                                           |1        |1          |81           |
# |web.id.isipulsa.data                                                                              |0        |0          |80           |
# |com.koindominoisland.app                                                                          |1        |1          |80           |
# |com.mangbelanjakeun                                                                               |1        |1          |80           |
# |cardgame.pokerasia.fourofakind.capsa.susun                                                        |0        |0          |80           |
# |com.rucksack.barcodescannerforebay                                                                |1        |1          |80           |
# |com.asemediatech.resiongkir                                                                       |0        |0          |80           |
# |com.teqnidev.paidappsfree.pro                                                                     |1        |1          |79           |
# |com.intersport.app.de                                                                             |0        |0          |79           |
# |com.hl.media.barcode.reader.free                                                                  |0        |0          |78           |
# |com.moneydolly                                                                                    |0        |0          |78           |
# |com.ryananggowo.kumpulantokoonlineindonesiadalamsatuaplikasi                                      |0        |0          |77           |
# |de.gavitec.android                                                                                |0        |0          |77           |
# |com.panagola.app.shopcalc                                                                         |1        |1          |77           |
# |app.ndtv.matahari                                                                                 |0        |0          |77           |
# |com.treasurelistings.gsalr                                                                        |0        |0          |77           |
# |com.ayopop                                                                                        |0        |0          |77           |
# |com.online.smart.bracelet.watch.shop.oss                                                          |1        |1          |77           |
# |com.real.wallpaper                                                                                |0        |0          |76           |
# |kr.defind.shoepik.users                                                                           |0        |0          |76           |
# |app.tsumuchan.frima_search                                                                        |1        |1          |76           |
# |com.olx.southasia                                                                                 |1        |1          |76           |
# |com.appscentral.free_anti_stress                                                                  |1        |1          |76           |
# |com.blanja.apps.android                                                                           |0        |0          |76           |
# |com.watsons.id.android                                                                            |1        |1          |76           |
# |com.myfkpntutorial.fkpntutorial                                                                   |0        |0          |75           |
# |com.onlinecoupons.shopee                                                                          |1        |1          |75           |
# |com.codified.hipyard                                                                              |1        |1          |75           |
# |com.clytie.app                                                                                    |0        |0          |75           |
# |com.holashp.purchasebate                                                                          |0        |0          |75           |
# |com.gloriajeans.mobile                                                                            |0        |0          |75           |
# |com.izzedam.weedvva                                                                               |0        |0          |75           |
# |com.appscentral.free_toys_online                                                                  |1        |1          |74           |
# |com.onlineuae.shopping                                                                            |0        |0          |74           |
# |com.mobincube.hk_coupons.sc_E5UB92                                                                |0        |0          |74           |
# |net.cumart.ccw                                                                                    |1        |1          |74           |
# |com.big.shopapp                                                                                   |0        |0          |74           |
# |com.overstock                                                                                     |1        |1          |73           |
# |kr.co.camtalk                                                                                     |0        |0          |73           |
# |com.myBaubauJB.BaubauJB                                                                           |0        |0          |73           |
# |best.home.compare.coupons.discounts.tracker.assistant.alert                                       |0        |0          |73           |
# |com.levistrauss.customer                                                                          |0        |0          |73           |
# |com.online.smart.bracelet.watch.shopping.oss                                                      |0        |0          |73           |
# |id.jasamitra.app                                                                                  |0        |0          |73           |
# |com.boulla.cellphone                                                                              |1        |1          |72           |
# |com.shopkick.app                                                                                  |0        |0          |72           |
# |com.kinoli.couponsherpa                                                                           |1        |1          |72           |
# |nl.onlineretailservice.reclamefolderandroid                                                       |0        |0          |72           |
# |com.hudsonsbay.android                                                                            |0        |0          |72           |
# |com.elevenst                                                                                      |0        |0          |72           |
# |com.zilingo.users                                                                                 |0        |0          |71           |
# |com.simple_memo_pad_notes_app.babylon                                                             |0        |0          |71           |
# |networld.price.app                                                                                |1        |1          |70           |
# |com.mygshp.gshp                                                                                   |0        |0          |70           |
# |id.toco                                                                                           |0        |0          |70           |
# |thecouponsapp.coupon                                                                              |0        |0          |70           |
# |my.com.nineyi.shop.s200074                                                                        |0        |0          |70           |
# |com.appscentral.glowpopit                                                                         |1        |1          |70           |
# |com.appybuilder.dhirajanand2009.OnlineToysShop                                                    |0        |0          |70           |
# |com.newandromo.dev535927.app804717                                                                |0        |0          |70           |
# |com.quikr                                                                                         |0        |0          |70           |
# |com.fixeads.coisas                                                                                |0        |0          |70           |
# |com.smokeeffect.nameart                                                                           |0        |0          |70           |
# |com.fingogroup.fingo                                                                              |1        |1          |69           |
# |com.shopee.mitra.id                                                                               |1        |1          |69           |
# |com.adfone.unefon                                                                                 |1        |1          |69           |
# |com.FurnitureOnlineShopping                                                                       |0        |0          |69           |
# |cimasv.appsaptod.guideforappmarketaptoide.tiosd                                                   |0        |0          |69           |
# |app.anakoslab.cekmurah                                                                            |0        |0          |69           |
# |com.mymofi.mofi                                                                                   |1        |1          |68           |
# |com.ilotte.mc.ilotte                                                                              |1        |1          |68           |
# |com.jarfernandez.discountcalculator                                                               |1        |1          |68           |
# |com.Konoha.NarutoShippundenMod                                                                    |0        |0          |68           |
# |com.tanihub.vaesdothrak                                                                           |1        |1          |68           |
# |com.carsome.customer                                                                              |0        |0          |67           |
# |com.premiumappsfactory.drone                                                                      |0        |0          |67           |
# |com.fivejack.itemku                                                                               |0        |0          |67           |
# |com.yoshop                                                                                        |1        |1          |67           |
# |com.lyst.lystapp                                                                                  |0        |0          |67           |
# |com.bikinaplikasi.yandhika                                                                        |1        |1          |67           |
# |com.ClothingShoppingOnline                                                                        |0        |0          |66           |
# |best.apparel.compare.coupons.discounts.tracker.assistant.alert                                    |0        |0          |66           |
# |com.taobao.taobao                                                                                 |1        |1          |66           |
# |com.berrybenka.android                                                                            |1        |1          |66           |
# |fashion.style.clothes.cheap                                                                       |1        |1          |66           |
# |com.indopub.codapayment                                                                           |0        |0          |66           |
# |com.asyncbyte.wishlist                                                                            |1        |1          |66           |
# |com.uniqlo.id.catalogue                                                                           |0        |0          |66           |
# |com.mfar.flyerify                                                                                 |0        |0          |66           |
# |com.onlineshoppingvietnam.vietnamshopping                                                         |0        |0          |66           |
# |id.compro.kartikaaccessories                                                                      |1        |1          |65           |
# |com.fineappstudio.android.petfriends                                                              |0        |0          |65           |
# |com.promomobil.promomobil                                                                         |0        |0          |65           |
# |com.bras.panty.nightwere.shop                                                                     |0        |0          |65           |
# |com.topshop.shopping                                                                              |0        |0          |65           |
# |cdiscount.mobile                                                                                  |0        |0          |65           |
# |javasign.com.cekrekening                                                                          |0        |0          |65           |
# |app1688.apyuw                                                                                     |1        |1          |65           |
# |com.meihillman.qrbarcodescanner                                                                   |1        |1          |64           |
# |com.priceminister.buyerapp                                                                        |0        |0          |64           |
# |com.wAntaraMotor_8841334                                                                          |0        |0          |64           |
# |com.ranjith888999.onlinefurniturestore                                                            |0        |0          |64           |
# |net.giosis.shopping.jp                                                                            |0        |0          |64           |
# |com.onlinecoupons.bathandbody                                                                     |0        |0          |64           |
# |com.septiancell.site                                                                              |0        |0          |64           |
# |com.xubnaha.florame                                                                               |1        |1          |63           |
# |com.appapps.homeplus                                                                           |0        |0          |63           |
# |com.bestappsale                                                                                   |0        |0          |63           |
# |com.bikinaplikasi.cantikshop24                                                                    |0        |0          |63           |
# |com.pinteng.lovelywholesale                                                                       |0        |0          |63           |
# |com.thailand.onlineshopping                                                                       |0        |0          |62           |
# |studio.pvs.t_shirtdesignstudio                                                                    |1        |0          |62           |
# |com.applazada.promo                                                                               |1        |1          |62           |
# |epasar.ecommerce.app                                                                              |1        |1          |62           |
# |web.id.isipulsa.app                                                                               |0        |0          |62           |
# |com.onlineshoppingphilippines.philippinesshopping                                                 |1        |1          |62           |
# |genius.trade2                                                                                     |0        |0          |61           |
# |ru.puma.android                                                                                   |0        |0          |61           |
# |com.indoarjuna.gudangtasgrosir                                                                    |0        |0          |61           |
# |com.wind.shopping.lazada                                                                          |1        |1          |61           |
# |us.tokoonline.gratisongkir.bayarditempat                                                          |0        |0          |61           |
# |com.mcraftmaps.parkour                                                                            |0        |0          |61           |
# |com.ZAM.AgenKosmetikMurahIndonesia                                                                |1        |1          |60           |
# |com.myayudidesign.ayudidesign                                                                     |0        |0          |60           |
# |com.couponscodes.dealsdiscounts.forLazada                                                         |1        |1          |60           |
# |com.onlineksa.shopping                                                                            |0        |0          |60           |
# |com.mob91                                                                                         |1        |1          |60           |
# |com.barcodelookup                                                                                 |1        |1          |60           |
# |com.bikinaplikasi.rsfashiongrosir                                                                 |0        |0          |60           |
# |com.bikinaplikasi.brosayur                                                                        |0        |0          |60           |
# |id.allofresh.ecommerce                                                                            |1        |1          |59           |
# |com.sahibinden                                                                                    |1        |1          |59           |
# |com.faunesia.apk                                                                                  |0        |0          |59           |
# |com.zoltopshopla.shop                                                                             |0        |0          |59           |
# |at.willhaben                                                                                      |0        |0          |59           |
# |co.shopney.lelopak                                                                                |1        |1          |59           |
# |shopping.app.offer.deals.us.shopping                                                              |0        |0          |58           |
# |com.airjordanoutlet.android                                                                       |0        |0          |58           |
# |fr.vinted                                                                                         |1        |1          |58           |
# |com.mmb.qtenoffers                                                                                |1        |1          |58           |
# |com.icedblueberry.shoppinglisteasy                                                                |1        |1          |58           |
# |com.glitterapps.near                                                                              |0        |0          |58           |
# |com.rkskdldl.smartstore_forseller                                                                 |0        |0          |58           |
# |co.fusionweb.blackfriday                                                                          |0        |0          |58           |
# |com.mobappsbaker.kwaroffers                                                                       |0        |0          |58           |
# |right.apps.gleamaways                                                                             |0        |0          |58           |
# |com.mymamabelanjaindonesia.mamabelanjaindonesia                                                   |0        |0          |57           |
# |com.Alltor.YoUnipin                                                                               |1        |1          |57           |
# |com.ingonan.store                                                                                 |0        |0          |57           |
# |com.ebay.kr.gmarket                                                                               |0        |0          |57           |
# |com.OfficialAppsDev.CodanyaShopPro                                                                |1        |1          |57           |
# |com.realmestore.app                                                                               |0        |0          |57           |
# |web.id.isipulsa.agenkuota                                                                         |0        |0          |57           |
# |com.ria.auto                                                                                      |0        |0          |57           |
# |com.summerseger.katalogerhadaftarharga                                                            |0        |0          |56           |
# |com.premiumappsfactory.popitbag                                                                   |1        |1          |56           |
# |com.wemakeprice                                                                                   |0        |0          |56           |
# |tukutuku.sn                                                                                       |0        |0          |56           |
# |com.indonesia_shopping.zakir                                                                      |0        |0          |56           |
# |com.langital.app                                                                                  |1        |1          |56           |
# |com.amazon_online_mobile_shopping.flipkart_mobile_sale                                            |0        |0          |56           |
# |alejo.converse                                                                                    |0        |0          |56           |
# |com.Spark.ESPpark                                                                                 |1        |1          |55           |
# |com.mymindoapplication.mindoapplication                                                           |0        |0          |55           |
# |com.jockey_women_underwear_clovia_bra.online_jockey_sexy_nighties_hot_bra_lingeries               |0        |0          |55           |
# |com.jumia.android                                                                                 |0        |0          |55           |
# |com.gramedia.retail                                                                               |1        |1          |55           |
# |com.wSARAVANATRADERS_9366627                                                                      |0        |0          |55           |
# |com.summerseger.katalogZoyaHijabGamis                                                             |1        |1          |54           |
# |com.hypermart.mobile                                                                              |1        |1          |54           |
# |indopub.unipin.topup                                                                              |1        |0          |54           |
# |com.wCodashopIndonesia_10155135                                                                   |0        |0          |54           |
# |com.souq.app                                                                                      |0        |0          |53           |
# |com.aptivdev.buykidstoys                                                                          |1        |1          |53           |
# |com.globalegrow.app.gearbest                                                                      |1        |1          |53           |
# |gr.playstore.studio.android5b8fc185820c2                                                          |0        |0          |53           |
# |com.newandromo.dev535927.app1241888                                                               |0        |0          |53           |
# |com.korchix.chineoapp                                                                             |1        |1          |53           |
# |za.co.meeser.inglenook                                                                            |0        |0          |53           |
# |womens.cheap.clothes                                                                              |1        |1          |53           |
# |id.serpul.sp3576                                                                                  |0        |0          |52           |
# |com.ozen.alisverislistesi                                                                         |1        |1          |52           |
# |id.co.mncshop.android                                                                             |1        |1          |52           |
# |com.yanedev.browserforlazadacoupons                                                               |0        |0          |52           |
# |be.tweedehands.m                                                                                  |0        |0          |52           |
# |com.hartono.elektronika                                                                           |0        |0          |52           |
# |com.onlineshopping.thailandshopping                                                               |0        |0          |52           |
# |com.myglamm.ecommerce                                                                             |1        |1          |52           |
# |com.taobao.taobao                                                                                 |0        |0          |52           |
# |com.olx.olx                                                                                       |0        |0          |52           |
# |com.iqouz.gameo                                                                                   |0        |0          |51           |
# |com.anaonlineshopping.babytoys                                                                    |0        |0          |51           |
# |com.topindo.android                                                                               |1        |1          |51           |
# |com.dinomarket.app                                                                                |1        |1          |51           |
# |com.wkendarijualbeli_9915700                                                                      |1        |1          |51           |
# |appinventor.ai_lightworld813.bodysize                                                             |1        |1          |51           |
# |com.bengkeldroid.sophie                                                                           |0        |0          |51           |
# |com.application_4u.qrcode.barcode                                                                 |1        |0          |51           |
# |com.mobile.pomelo                                                                                 |1        |1          |51           |
# |com.indopub.codapayment                                                                           |1        |1          |51           |
# |com.Spark.Skillz                                                                                  |0        |0          |50           |
# |bra.shoppingapp                                                                                   |0        |0          |50           |
# |com.laku6webviewapp                                                                               |1        |1          |50           |
# |se.scmv.belarus                                                                                   |0        |0          |50           |
# |com.optikmelawai.optikmelawai                                                                     |1        |1          |50           |
# |com.newandromo.dev535927.app834897                                                                |1        |1          |50           |
# |com.abtnprojects.ambatana                                                                         |1        |1          |50           |
# |com.droneflight_shoppingonline.drone                                                              |0        |0          |50           |
# |bmd.android.apps                                                                                  |1        |1          |49           |
# |com.metalsoft.trackchecker_mobile                                                                 |1        |1          |49           |
# |com.riverview.opticseis                                                                           |1        |1          |49           |
# |pl.vinted                                                                                         |0        |0          |49           |
# |com.app.offerit                                                                                   |1        |1          |49           |
# |com.capigami.outofmilk                                                                            |1        |1          |49           |
# |com.kickavenue.androidshop                                                                        |1        |1          |49           |
# +--------------------------------------------------------------------------------------------------+---------+-----------+-------------+
# """
#
#
# # spark.sql(" \
# #     select  \
# #         now_label,after_label,bundle_app_set_size_label \
# #         ,count(device_id) as device_id_cnt  \
# #     from    \
# #     (   \
# #     select  \
# #         device_id   \
# #         ,now_label  \
# #         ,after_label    \
# #         ,case   \
# #         when bundle_app_set_size is null then 0 \
# #         when bundle_app_set_size=1 then 1   \
# #         when bundle_app_set_size=2 then 2   \
# #         when bundle_app_set_size=3 then 3   \
# #         when bundle_app_set_size=4 then 4   \
# #         when bundle_app_set_size=5 then 5   \
# #         when bundle_app_set_size=6 then 6   \
# #         when bundle_app_set_size=7 then 7   \
# #         when bundle_app_set_size=8 then 8   \
# #         when bundle_app_set_size=9 then 9   \
# #         when bundle_app_set_size=10 then 10 \
# #         else  11    \
# #         end as bundle_app_set_size_label    \
# #     from    \
# #         df_table    \
# #     ) t1    \
# #     group by    \
# #         now_label,after_label,bundle_app_set_size_label \
# #     order by    \
# #         device_id_cnt desc  \
# # ").show(500,truncate=False)
#
# """
# +---------+-----------+-------------------------+-------------+
# |now_label|after_label|bundle_app_set_size_label|device_id_cnt|
# +---------+-----------+-------------------------+-------------+
# |0        |0          |0                        |243605187    |
# |0        |0          |1                        |21329407     |
# |1        |1          |1                        |18702534     |
# |1        |1          |2                        |3852945      |
# |0        |0          |2                        |1438535      |
# |1        |1          |3                        |350393       |
# |0        |0          |3                        |121207       |
# |1        |0          |1                        |74311        |
# |1        |1          |4                        |34413        |
# |1        |0          |2                        |20938        |
# |0        |0          |4                        |12621        |
# |0        |1          |0                        |10394        |
# |1        |1          |5                        |5418         |
# |0        |0          |5                        |2490         |
# |1        |0          |3                        |2465         |
# |1        |1          |6                        |2197         |
# |1        |1          |7                        |1340         |
# |1        |1          |11                       |990          |
# |0        |0          |6                        |949          |
# |1        |1          |8                        |900          |
# |1        |1          |9                        |591          |
# |0        |0          |7                        |520          |
# |0        |1          |1                        |497          |
# |1        |1          |10                       |433          |
# |0        |0          |8                        |321          |
# |0        |0          |11                       |312          |
# |1        |0          |4                        |275          |
# |0        |0          |9                        |185          |
# |0        |0          |10                       |127          |
# |1        |0          |5                        |44           |
# |1        |0          |6                        |20           |
# |0        |1          |2                        |14           |
# |1        |0          |7                        |14           |
# |1        |0          |11                       |7            |
# |1        |0          |8                        |6            |
# |1        |0          |9                        |4            |
# |1        |0          |10                       |3            |
# +---------+-----------+-------------------------+-------------+
# """
# device_cnt_all = 289573007
# app_size_0 =   243615581
# app_size_0_percent =  app_size_0/ device_cnt_all
# print(app_size_0_percent)
#
# app_size_1 =   40106749
# app_size_1_percent =  app_size_1/ device_cnt_all
# print(app_size_1_percent)
#
# app_0_0 = 266511861
# app_0_0_percent =  app_0_0/ device_cnt_all
# print(app_0_0_percent)
#
# app_0_1 = 10905
# app_1_1 = 23050241
# print(app_1_1/device_cnt_all)
#
# import pyspark.sql.functions as F
# df=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/20220630_delay_1").where("now_label=0")
# bundle_app_set_default = F.array(F.lit("N"))
# fill_rule = F.when(F.col("bundle_app_set").isNull(), bundle_app_set_default).otherwise(F.col("bundle_app_set"))
# df_2 = df.fillna("N",subset=["osversion"]).fillna(0, subset=["bundle_app_set_size"]).withColumn("bundle_app_set_temp", fill_rule).drop("bundle_app_set").withColumnRenamed("bundle_app_set_temp","bundle_app_set").persist()
# df_2.createTempView("df_2_table")
#
# spark.sql(" \
#     select  \
#         after_label,app \
#         ,count(device_id) as device_id_cnt   \
#     from    \
#     (   \
#         select  \
#             device_id   \
#             ,after_label    \
#             ,app    \
#         from    \
#             df_2_table  \
#         lateral view    \
#             explode(bundle_app_set) as app  \
#     ) t1    \
#     group by    \
#         after_label,app \
#     order by    \
#         device_id_cnt desc  \
# ").show(1000,truncate=False)
#
# """
# +-----------+--------------------------------------------------------------------------------------------------+-------------+
# |after_label|app                                                                                               |device_id_cnt|
# +-----------+--------------------------------------------------------------------------------------------------+-------------+
# |0          |N                                                                                                 |243605187    |
# |0          |id.co.shopintar                                                                                   |9971428      |
# |0          |com.app.tokobagus.betterb                                                                         |7314356      |
# |0          |com.shopee.id                                                                                     |1686219      |
# |0          |com.alibaba.aliexpresshd                                                                          |1251127      |
# |0          |blibli.mobile.commerce                                                                            |776590       |
# |0          |com.adfone.indosat                                                                                |718997       |
# |0          |com.tokopedia.tkpd                                                                                |428325       |
# |0          |com.adfone.unefon                                                                                 |427936       |
# |0          |com.thecarousell.Carousell                                                                        |305431       |
# |0          |com.bukalapak.mitra                                                                               |228538       |
# |0          |com.alibaba.intl.android.apps.poseidon                                                            |218589       |
# |0          |io.silvrr.installment                                                                             |117196       |
# |0          |com.newchic.client                                                                                |99807        |
# |0          |com.cari.promo.diskon                                                                             |76514        |
# |0          |com.bukalapak.android                                                                             |71570        |
# |0          |indopub.unipin.topup                                                                              |36245        |
# |0          |studio.pvs.t_shirtdesignstudio                                                                    |33417        |
# |0          |com.onlinecoupons.lazada                                                                          |30689        |
# |0          |com.rootbridge.ula                                                                                |25993        |
# |0          |com.application_4u.qrcode.barcode                                                                 |25650        |
# |0          |com.yoinsapp                                                                                      |25603        |
# |0          |com.memopad.for_.writing.babylon                                                                  |24623        |
# |0          |com.propcoid.propcoid                                                                             |21127        |
# |0          |universal.app.cek.resi.shopee.express                                                             |17014        |
# |0          |com.pim.pulsapedia                                                                                |15355        |
# |0          |com.jollycorp.jollychic                                                                           |14175        |
# |0          |com.pixelkoin.app                                                                                 |13967        |
# |0          |diamond.via.id                                                                                    |13777        |
# |0          |com.Alltor.CodaFree                                                                               |13457        |
# |0          |com.app.offerit                                                                                   |13301        |
# |0          |com.grannyrewards.app                                                                             |12746        |
# |0          |com.kkmart.user                                                                                   |12575        |
# |0          |indopub.codashop.popup                                                                            |12458        |
# |0          |com.brightstripe.parcels                                                                          |12037        |
# |0          |com.geomobile.tiendeo                                                                             |11884        |
# |0          |com.motivs.marketplaceeid                                                                         |11707        |
# |0          |com.ptmarketplaceio.jualxbeli                                                                     |11583        |
# |0          |co.tapcart.app.id_wO7BWzmfNM                                                                      |11124        |
# |1          |N                                                                                                 |10394        |
# |0          |com.shopee.ph                                                                                     |10127        |
# |0          |app.idcshop.com                                                                                   |9598         |
# |0          |com.livicepat.tokolengkap                                                                         |9531         |
# |0          |com.ajsneakeronline.android                                                                       |9245         |
# |0          |com.checkoutcharlie.shopmate                                                                      |8490         |
# |0          |com.mrbox.guanfang.large                                                                          |8047         |
# |0          |com.kiodeecfeedgtp.wip                                                                            |8044         |
# |0          |com.primarkshopstore.shopukprimark                                                                |8043         |
# |0          |com.gdmagri.app                                                                                   |7874         |
# |0          |com.topsmarkets.app.android                                                                       |7874         |
# |0          |freefire.id                                                                                       |7755         |
# |0          |com.voixme.d4d                                                                                    |7401         |
# |0          |com.geekslab.qrbarcodescanner.pro                                                                 |7231         |
# |0          |jd.cdyjy.overseas.market.indonesia                                                                |7219         |
# |0          |com.snkrdunk.en.android                                                                           |7154         |
# |0          |com.nz4.diamondsff                                                                                |7126         |
# |0          |com.shopee.my                                                                                     |7057         |
# |0          |com.kanzengames                                                                                   |6555         |
# |0          |com.agromaret.android.app                                                                         |6480         |
# |0          |evermos.evermos.com.evermos                                                                       |6406         |
# |0          |com.codmobil.mobilcod                                                                             |6179         |
# |0          |kr.co.quicket                                                                                     |5941         |
# |0          |diamonds.ff.viapulsa                                                                              |5815         |
# |0          |com.l                                                                                             |5756         |
# |0          |com.zulily.android                                                                                |5705         |
# |0          |com.tycmrelx.cash                                                                                 |5469         |
# |0          |com.asemediatech.cekresiongkir                                                                    |5429         |
# |0          |develop.joo.scanbarcodeharga                                                                      |5341         |
# |0          |mmapps.mobile.discount.calculator                                                                 |4966         |
# |0          |com.application_4u.qrcode.barcode.scanner.reader.flashlight                                       |4690         |
# |0          |com.ebay.mobile                                                                                   |4683         |
# |0          |com.airjordanshop.android                                                                         |4636         |
# |0          |com.locket.locketguide                                                                            |4633         |
# |0          |com.cakecodes.bitmaker                                                                            |4412         |
# |0          |com.giftmill.giftmill                                                                             |4398         |
# |0          |com.iljovita.hargapromokatalog3                                                                   |4116         |
# |0          |net.tsapps.appsales                                                                               |3952         |
# |0          |com.Expandear.KatalogPromo                                                                        |3887         |
# |0          |com.ILJOVITA.android.universalwebview                                                             |3738         |
# |0          |com.mudah.my                                                                                      |3663         |
# |0          |id.allofresh.ecommerce                                                                            |3527         |
# |0          |com.perekrestochek.app                                                                            |3511         |
# |0          |com.interfocusllc.patpat                                                                          |3432         |
# |0          |com.mygamezone.gamezone                                                                           |3405         |
# |0          |yotcho.shop                                                                                       |3284         |
# |0          |fr.casino.fidelite                                                                                |3241         |
# |0          |com.tiktokshop.tiktokshop                                                                         |3009         |
# |0          |com.on9store.shopping.allpromotion                                                                |2994         |
# |0          |com.todayapp.cekresi                                                                              |2933         |
# |0          |com.joom                                                                                          |2715         |
# |0          |com.ebay.gumtree.au                                                                               |2695         |
# |0          |com.androidrocker.qrscanner                                                                       |2671         |
# |0          |com.CouponChart                                                                                   |2668         |
# |0          |com.offerup                                                                                       |2643         |
# |0          |com.shoppingapp.shopeeeasa                                                                        |2625         |
# |0          |com.tuck.hellomarket                                                                              |2579         |
# |0          |com.whaleshark.retailmenot                                                                        |2523         |
# |0          |com.wKLIKGAMESHOPAdalahMitraAgenChipHiggsDominoIsland_13240116                                    |2462         |
# |0          |com.xbox_deals.sales                                                                              |2419         |
# |0          |com.abtnprojects.ambatana                                                                         |2417         |
# |0          |com.zalora.android                                                                                |2415         |
# |0          |shopping.indonesia.com.app                                                                        |2360         |
# |0          |com.globalbusiness.countrychecker                                                                 |2350         |
# |0          |com.shopee.br                                                                                     |2306         |
# |0          |com.onlineshoppingindonesia.indonesiashopping                                                     |2236         |
# |0          |com.my.exchangecounter                                                                            |2218         |
# |0          |com.biggu.shopsavvy                                                                               |2200         |
# |0          |fr.vinted                                                                                         |2107         |
# |0          |com.lootboy.app                                                                                   |2095         |
# |0          |com.olx.southasia                                                                                 |1973         |
# |0          |com.kohls.mcommerce.opal                                                                          |1940         |
# |0          |com.ebay.kleinanzeigen                                                                            |1929         |
# |0          |hajigaming.codashop.malaysia                                                                      |1827         |
# |0          |com.shopee.th                                                                                     |1801         |
# |0          |aw.kiriman                                                                                        |1778         |
# |0          |indopub.unipinpro.topup                                                                           |1742         |
# |0          |toko.online.terpercaya                                                                            |1704         |
# |0          |com.my.cocpostit                                                                                  |1698         |
# |0          |com.aptivdev.remotecontroltoys                                                                    |1664         |
# |0          |com.andromo.dev786963.app908281                                                                   |1661         |
# |0          |com.dhgate.buyermob                                                                               |1650         |
# |0          |com.appscentral.popitcase                                                                         |1617         |
# |0          |com.tikshop.tikshop                                                                               |1612         |
# |0          |com.avito.android                                                                                 |1572         |
# |0          |com.xyz.alihelper                                                                                 |1566         |
# |0          |com.zanyatocorp.checkbarcode                                                                      |1565         |
# |0          |com.wKUBOIKumpulanBelanjaOnlineIndonesia_9684052                                                  |1512         |
# |0          |com.koinhiggsdomino.id                                                                            |1511         |
# |0          |com.gumtree.android                                                                               |1487         |
# |0          |com.onlineshoppingchina.chinashopping                                                             |1483         |
# |0          |com.trd.lapakproperti                                                                             |1468         |
# |0          |br.com.cea.appb2c                                                                                 |1387         |
# |0          |com.icaali.flashsale                                                                              |1345         |
# |0          |com.appnana.android.giftcardrewards                                                               |1294         |
# |0          |com.shpock.android                                                                                |1243         |
# |0          |com.app.goga                                                                                      |1231         |
# |0          |com.wCodashopIndonesia_10208434                                                                   |1224         |
# |0          |com.slidejoy                                                                                      |1215         |
# |0          |com.mmb.qtenoffers                                                                                |1189         |
# |0          |id.salestock.mobile                                                                               |1171         |
# |0          |com.nsmobilehub                                                                                   |1169         |
# |0          |nl.marktplaats.android                                                                            |1163         |
# |0          |online.market                                                                                     |1161         |
# |0          |web.id.isipulsa.appkita                                                                           |1137         |
# |0          |com.mataharimall.mmandroid                                                                        |1134         |
# |0          |com.chiphiggsdomino.app                                                                           |1123         |
# |0          |com.myFinderiauOnline.FinderiauOnline                                                             |1082         |
# |0          |com.itmobix.qataroffers                                                                           |1066         |
# |0          |com.cknb.hiddentagcop                                                                             |1038         |
# |0          |com.aptivdev.carstoys                                                                             |1022         |
# |0          |com.browsershopping.forgrouponcoupons                                                             |1008         |
# |0          |com.mi.global.shop                                                                                |1007         |
# |0          |com.shopee.co                                                                                     |960          |
# |0          |com.supercute.shoppinglist                                                                        |955          |
# |0          |com.ebates                                                                                        |949          |
# |0          |toko.online.bayarditempat                                                                         |938          |
# |0          |com.tourbillon.freeappsnow                                                                        |913          |
# |0          |com.bhanu.redeemerfree                                                                            |907          |
# |0          |com.cancctv.mobile                                                                                |901          |
# |0          |com.anaonlineshopping.kidstoys                                                                    |891          |
# |0          |com.ebay.kijiji.ca                                                                                |885          |
# |0          |com.adfone.aditup                                                                                 |871          |
# |0          |com.metro.foodbasics                                                                              |870          |
# |0          |sheproduction.coda.topup                                                                          |858          |
# |0          |com.girls.fashion.entertainment.School.Girls.Sale.Day.Shopping.Makeup.Adventure                   |855          |
# |0          |fr.leboncoin                                                                                      |840          |
# |0          |it.doveconviene.android                                                                           |830          |
# |0          |net.slickdeals.android                                                                            |820          |
# |0          |com.mygodrivi.godrivi                                                                             |804          |
# |0          |fr.deebee.calculsoldes                                                                            |791          |
# |0          |com.star5studio.Homedesigner.houseDecor                                                           |771          |
# |0          |com.wNasaStore_8607120                                                                            |765          |
# |0          |com.myoryza.oryza                                                                                 |764          |
# |0          |fashion.style.clothes.cheap                                                                       |763          |
# |0          |com.newandromo.dev535927.app981756                                                                |744          |
# |0          |com.histore.indonesia                                                                             |728          |
# |0          |com.shopee.mx                                                                                     |727          |
# |0          |id.yoyo.popslide.app                                                                              |727          |
# |0          |com.bikinaplikasi.beliin.app                                                                      |727          |
# |0          |tw.taoyuan.qrcode.reader.barcode.scanner.flashlight                                               |722          |
# |0          |com.contextlogic.wish                                                                             |710          |
# |0          |cam.gadanie.hiromant                                                                              |699          |
# |0          |id.co.elevenia                                                                                    |694          |
# |0          |com.kittoboy.shoppingmemo                                                                         |694          |
# |0          |tw.fancyapp.qrcode.barcode.scanner.reader.isbn.flashlight                                         |692          |
# |0          |networld.price.app                                                                                |691          |
# |0          |in.amazon.mShop.android.shopping                                                                  |673          |
# |0          |com.opo.shopping.lazada                                                                           |671          |
# |0          |com.nugros.arenatani                                                                              |665          |
# |0          |com.mrd.cekresipengiriman                                                                         |652          |
# |0          |com.malaysiashopping.sonu                                                                         |650          |
# |0          |com.sendo                                                                                         |649          |
# |0          |com.amazon.mShop.android.shopping                                                                 |628          |
# |0          |ng.jiji.app                                                                                       |626          |
# |0          |com.oriflame.catalogue.maroc.asha                                                                 |621          |
# |0          |com.indopay.unipin                                                                                |611          |
# |0          |net.agusharyanto.catatbelanja                                                                     |607          |
# |0          |com.online.smart.bracelet.watch.shop.oss                                                          |584          |
# |0          |com.cannotbeundone.amazonbarcodescanner                                                           |584          |
# |0          |com.toko.higgsdomino                                                                              |576          |
# |0          |com.thirdrock.fivemiles                                                                           |574          |
# |0          |com.boulla.cellphone                                                                              |568          |
# |0          |com.sahibinden                                                                                    |562          |
# |0          |goosecreekco.android.app                                                                          |560          |
# |0          |com.overstock                                                                                     |557          |
# |0          |com.onlinecoupons.shopee                                                                          |547          |
# |0          |com.tokopedia.kelontongapp                                                                        |547          |
# |0          |com.aptivdev.buykidstoys                                                                          |543          |
# |0          |com.cupang.ads                                                                                    |542          |
# |0          |co.kr.ezapp.shoppinglist                                                                          |540          |
# |0          |com.allgoritm.youla                                                                               |539          |
# |0          |com.appscentral.free_toys_online                                                                  |538          |
# |0          |com.maulanatriharja.catatanbelanja                                                                |537          |
# |0          |com.bitwize10.supersimpleshoppinglist                                                             |534          |
# |0          |com.wallapop                                                                                      |533          |
# |0          |com.minhtinh.daftarbelanja                                                                        |532          |
# |0          |com.wajual                                                                                        |532          |
# |0          |com.appscentral.free_anti_stress                                                                  |532          |
# |0          |com.crown.indonesia                                                                               |531          |
# |0          |co.shopney.lelopak                                                                                |526          |
# |0          |com.jarfernandez.discountcalculator                                                               |524          |
# |0          |com.boulla.comparephones                                                                          |523          |
# |0          |com.pasti.terjual                                                                                 |522          |
# |0          |com.metalsoft.trackchecker_mobile                                                                 |513          |
# |0          |com.Spark.ESPpark                                                                                 |511          |
# |0          |com.alfamart.alfagift                                                                             |502          |
# |0          |com.capigami.outofmilk                                                                            |502          |
# |0          |jual.belibarangbekas                                                                              |499          |
# |0          |com.camera.translate.languages                                                                    |492          |
# |0          |com.OfficialAppsDev.CodanyaShopPro                                                                |489          |
# |0          |com.whaff.whaffapp                                                                                |480          |
# |0          |com.kinoli.couponsherpa                                                                           |480          |
# |0          |ru.auto.ara                                                                                       |477          |
# |0          |com.ww1688ProductsAllLanguage_11962618                                                            |474          |
# |0          |com.georgeparky.thedroplist                                                                       |470          |
# |0          |com.opensooq.OpenSooq                                                                             |468          |
# |0          |com.pozitron.hepsiburada                                                                          |467          |
# |0          |shopping.express.sales.ali                                                                        |462          |
# |0          |id.com.android.javapay                                                                            |461          |
# |0          |com.vova.android                                                                                  |457          |
# |0          |com.shopback.app                                                                                  |452          |
# |0          |tn.websol.adsblackfriday2                                                                         |450          |
# |0          |com.disqonin                                                                                      |447          |
# |0          |online.malaysia.shopping                                                                          |436          |
# |0          |de.autodoc.gmbh                                                                                   |426          |
# |0          |com.wind.shopping.lazada                                                                          |422          |
# |0          |com.dusdusan.katalog                                                                              |419          |
# |0          |com.mob91                                                                                         |417          |
# |0          |app.shopping.ali.express                                                                          |417          |
# |0          |id.triliun.sobatpromo                                                                             |410          |
# |0          |com.itmobix.kwendeals                                                                             |405          |
# |0          |com.onlineshoppingjapan.japanshopping                                                             |405          |
# |0          |com.trkstudio.currentproducts                                                                     |401          |
# |0          |com.premiumappsfactory.popitbag                                                                   |397          |
# |0          |com.onlineshoppingsingapore.singaporeshopping                                                     |396          |
# |0          |com.myattausiltransaksional.attausiltransaksional                                                 |394          |
# |0          |app.tsumuchan.frima_search                                                                        |394          |
# |0          |paStudio.pharaoh.diamondpuzzlecall                                                                |394          |
# |0          |com.chinashoping.sonu                                                                             |393          |
# |0          |com.mangbelanjakeun                                                                               |389          |
# |0          |app.marketplace.mokamall                                                                          |389          |
# |0          |com.Alltor.YoUnipin                                                                               |385          |
# |0          |com.online.shoppingchina                                                                          |385          |
# |0          |com.enuri.android                                                                                 |384          |
# |0          |com.myglamm.ecommerce                                                                             |384          |
# |0          |com.midasmind.app.mmgocarmm                                                                       |382          |
# |0          |com.ChinaShoppingOnline                                                                           |379          |
# |0          |com.ae.ae                                                                                         |378          |
# |0          |com.ricosti.gazetka                                                                               |374          |
# |0          |com.rucksack.barcodescannerforwalmart                                                             |371          |
# |0          |com.appscentral.glowpopit                                                                         |371          |
# |0          |com.elz.secondhandstore                                                                           |368          |
# |0          |com.geekeryapps.cartoys.remotecontrolcars                                                         |367          |
# |0          |com.iassist.nurraysa                                                                              |353          |
# |0          |com.appmyshop                                                                                     |353          |
# |0          |com.chainreactioncycles.hybrid                                                                    |353          |
# |0          |com.applazada.promo                                                                               |351          |
# |0          |gsshop.mobile.v2                                                                                  |349          |
# |0          |com.rucksack.barcodescannerforebay                                                                |342          |
# |0          |com.glitterapps.plushie                                                                           |341          |
# |0          |com.mobile.cover.photo.editor.back.maker                                                          |341          |
# |0          |com.insightquest.snooper                                                                          |338          |
# |0          |best.deals.compare.coupons.discounts.tracker.assistant.alert                                      |336          |
# |0          |com.stGalileo.JewelCurse                                                                          |332          |
# |0          |com.klink.kmart                                                                                   |329          |
# |0          |com.ionicframework.sp262624                                                                       |327          |
# |1          |id.co.shopintar                                                                                   |327          |
# |0          |tempat.jual.beli.hewan.peliharaan                                                                 |325          |
# |0          |com.olx.pk                                                                                        |322          |
# |0          |com.pocky.ztime                                                                                   |321          |
# |0          |com.myhiggsdominotips.higgsdominotips                                                             |320          |
# |0          |com.wBGShopee_9071949                                                                             |320          |
# |0          |com.summerseger.katalogRabbaniHijabGamis                                                          |318          |
# |0          |com.groupon                                                                                       |317          |
# |0          |com.myntra.android                                                                                |314          |
# |0          |com.schibsted.bomnegocio.androidApp                                                               |313          |
# |0          |com.nicekicks.rn.android                                                                          |311          |
# |0          |com.tokopedia.sellerapp                                                                           |307          |
# |0          |com.rmtheis.price.comparison.scanner                                                              |298          |
# |0          |com.olxmena.horizontal                                                                            |298          |
# |0          |main.ClicFlyer                                                                                    |297          |
# |0          |com.crocs.app                                                                                     |292          |
# |0          |com.xubnaha.florame                                                                               |289          |
# |0          |com.orbstudio.daftartiktokshop                                                                    |289          |
# |0          |com.malaysiashopping.onlineshoppingmalaysiaapp                                                    |288          |
# |0          |com.flipkart.android                                                                              |287          |
# |0          |com.panagola.app.shopcalc                                                                         |287          |
# |0          |com.couponscodes.dealsdiscounts.forLazada                                                         |286          |
# |0          |app.oneprice                                                                                      |284          |
# |0          |com.idbuysell.fff                                                                                 |284          |
# |0          |com.appscentral.popit                                                                             |273          |
# |0          |my.com.thefoodmerchant.app                                                                        |269          |
# |0          |com.shell.sitibv.shellgoplusindia                                                                 |267          |
# |0          |com.appscentral.sd                                                                                |267          |
# |0          |id.co.acehardware.acerewards                                                                      |266          |
# |0          |com.onlineshoppingthailand.thailandshoppingapp                                                    |266          |
# |0          |com.meihillman.qrbarcodescanner                                                                   |266          |
# |0          |com.ksl.android.classifieds                                                                       |266          |
# |0          |zuper.market13                                                                                    |265          |
# |0          |de.super                                                                                          |264          |
# |0          |jp.jmty.app2                                                                                      |263          |
# |0          |com.digital.shopping.apps.india.lite                                                              |257          |
# |0          |com.appsfree.android                                                                              |255          |
# |0          |com.alfamart.katalog.flyers                                                                       |255          |
# |0          |nz.co.trademe.trademe                                                                             |251          |
# |0          |com.codified.hipyard                                                                              |251          |
# |0          |lk.ikman                                                                                          |247          |
# |0          |com.newandromo.dev535927.app1163168                                                               |247          |
# |0          |com.korchix.chineoapp                                                                             |243          |
# |0          |ru.ifsoft.mymarketplace                                                                           |242          |
# |0          |com.appscentral.squishy_shop                                                                      |242          |
# |0          |com.newandromo.dev535927.app777705                                                                |236          |
# |0          |com.barcodelookup                                                                                 |236          |
# |0          |com.koindominoisland.app                                                                          |236          |
# |0          |com.ozen.alisverislistesi                                                                         |234          |
# |0          |epasar.ecommerce.app                                                                              |232          |
# |0          |com.univplay.appreward                                                                            |228          |
# |0          |com.ibotta.android                                                                                |228          |
# |0          |com.snapdeal.main                                                                                 |227          |
# |0          |com.c51                                                                                           |227          |
# |0          |com.yoshop                                                                                        |223          |
# |0          |id.com.android.nikimobile                                                                         |222          |
# |0          |com.light.paidappssales                                                                           |217          |
# |0          |com.rucksack.pricecomparisonforamazonebay                                                         |217          |
# |0          |com.radipgo.katalogdiskon                                                                         |216          |
# |0          |com.thekrazycouponlady.kcl                                                                        |216          |
# |0          |ua.slando                                                                                         |216          |
# |0          |com.listonic.sales.deals.weekly.ads                                                               |215          |
# |0          |chinese.goods.online.cheap                                                                        |214          |
# |0          |com.bikinaplikasi.bjcell                                                                          |213          |
# |0          |com.mobileshop.usa                                                                                |212          |
# |0          |com.tmon                                                                                          |212          |
# |0          |ru.ozon.app.android                                                                               |211          |
# |0          |com.douya.e_commercepart_time                                                                     |210          |
# |0          |com.toycar_rccars_shoppingonline.carstoys                                                         |209          |
# |0          |com.onlineshopping.malaysiashopping                                                               |209          |
# |0          |com.numatams.numatamsapps.discounttaxcalc                                                         |207          |
# |0          |com.vybesxapp                                                                                     |206          |
# |0          |br.com.lardev.android.rastreiocorreios                                                            |205          |
# |0          |com.biglotsshop.online                                                                            |205          |
# |0          |com.asyncbyte.wishlist                                                                            |205          |
# |0          |com.gazetki.gazetki                                                                               |200          |
# |0          |com.anindyacode.cekharga                                                                          |198          |
# |0          |com.codylab.halfprice                                                                             |198          |
# |0          |app1688.apyuw                                                                                     |197          |
# |0          |asia.bluepay.clientin                                                                             |197          |
# |0          |com.appscentral.free_squishy_shop                                                                 |195          |
# |0          |com.goodbarber.seniorfree                                                                         |195          |
# |0          |com.newandromo.dev535927.app836388                                                                |192          |
# |0          |com.oliks.G.til.freo                                                                              |191          |
# |0          |com.roblox.robux.appreward.robux                                                                  |190          |
# |0          |prices.in.china                                                                                   |190          |
# |0          |fashion.shop.clothes.dresses                                                                      |190          |
# |0          |com.mycikalongwetanmarkets.cikalongwetanmarkets                                                   |189          |
# |0          |com.brandicorp.brandi3                                                                            |189          |
# |0          |com.tksolution.einkaufszettelmitspracheingabe                                                     |188          |
# |0          |com.roidtechnologies.appbrowzer                                                                   |187          |
# |0          |in.uknow.onlineshopping                                                                           |186          |
# |0          |com.icedblueberry.shoppinglisteasy                                                                |186          |
# |0          |com.bikinaplikasi.gadgetgrosir                                                                    |186          |
# |0          |com.bunniimarketplace                                                                             |186          |
# |0          |com.onlineshoppingphilippines.philippinesshopping                                                 |185          |
# |0          |de.roller.twa                                                                                     |185          |
# |0          |com.moramsoft.ppomppualarm                                                                        |183          |
# |0          |ru.grocerylist.android                                                                            |183          |
# |0          |com.teqnidev.paidappsfree.pro                                                                     |182          |
# |0          |com.militarycompany.helicoptermod                                                                 |181          |
# |0          |com.shopping.online.id                                                                            |181          |
# |0          |com.costco.app.android                                                                            |180          |
# |0          |com.dadidoo.catatanbelanja                                                                        |179          |
# |0          |com.my.codaccounts                                                                                |178          |
# |0          |com.b2w.americanas                                                                                |174          |
# |0          |com.sinworldnlineshop.shop                                                                        |172          |
# |0          |com.titansModforMcpe.AttackonTitanmod                                                             |171          |
# |0          |com.froogloid.kring.google.zxing.client.android                                                   |168          |
# |0          |com.biglotsapp.shop                                                                               |168          |
# |0          |com.anaonlineshopping.bra_womenunderwear                                                          |168          |
# |0          |com.shopee.in                                                                                     |167          |
# |0          |com.navigationproap.drivingvalleyrouteap.navigationrouteap                                        |166          |
# |0          |com.shipt.groceries                                                                               |164          |
# |0          |com.glitterapps.popitcase                                                                         |164          |
# |0          |com.andromo.dev630323.app739765                                                                   |164          |
# |0          |com.bikinaplikasi.yandhika                                                                        |163          |
# |0          |barcode.qr.scanner                                                                                |163          |
# |0          |com.wToyStoryToys_12242673                                                                        |162          |
# |0          |com.geekonweb.MalaysiaShoppingApp                                                                 |162          |
# |0          |com.mymofi.mofi                                                                                   |160          |
# |0          |com.glitterapps.stress                                                                            |160          |
# |0          |com.newandromo.dev535927.app834897                                                                |158          |
# |0          |appinventor.ai_lightworld813.bodysize                                                             |158          |
# |0          |com.amazon.aa                                                                                     |157          |
# |0          |com.promocodes.couponsforlazada                                                                   |156          |
# |0          |com.rmystudio.budlist                                                                             |155          |
# |0          |com.mattceonzo.ultimatewishlist                                                                   |155          |
# |0          |com.JOS.Japan.Online.Shopping                                                                     |154          |
# |0          |com.andromo.dev276588.app308783                                                                   |154          |
# |0          |com.glitterapps.popitfree                                                                         |152          |
# |0          |club.fromfactory                                                                                  |152          |
# |0          |com.snapcart.android                                                                              |152          |
# |0          |com.prolight.prolight                                                                             |151          |
# |0          |com.memiles.app                                                                                   |149          |
# |0          |com.agusharyanto.net.discountcalculator                                                           |147          |
# |0          |it.subito                                                                                         |147          |
# |0          |id.kitabeli.mitra                                                                                 |146          |
# |0          |com.kolaku.android                                                                                |145          |
# |0          |com.merchant.histore                                                                              |145          |
# |0          |com.bikroy                                                                                        |144          |
# |0          |com.glitterapps.simple                                                                            |144          |
# |0          |com.myshopedia.shopediamarket                                                                     |144          |
# |0          |com.inovasiteknologi.a101.onglaistore                                                             |143          |
# |0          |com.supercute.rumahmurah                                                                          |142          |
# |0          |com.rusbuket                                                                                      |141          |
# |0          |jp.co.unisys.android.yamadamobile                                                                 |139          |
# |0          |com.g2018.hfcoupons                                                                               |138          |
# |0          |usbobble.ssgame.fruitMatchRealizeDream                                                            |138          |
# |0          |com.barcodescanner.qrreader                                                                       |135          |
# |0          |kr.co.dreamshopping.mcapp                                                                         |135          |
# |0          |com.mhn.ponta                                                                                     |135          |
# |0          |benefitmedia.android.sparpionier                                                                  |133          |
# |0          |com.sayuran.online                                                                                |133          |
# |0          |com.sonyericsson.pontofrio                                                                        |133          |
# |0          |com.infohargamobil                                                                                |133          |
# |0          |com.shopclues                                                                                     |133          |
# |0          |com.ZAM.AgenKosmetikMurahIndonesia                                                                |132          |
# |0          |com.zeptoconsumerapp                                                                              |131          |
# |0          |com.pricecheckscanner                                                                             |131          |
# |0          |com.visionsmarts.pic2shop                                                                         |131          |
# |0          |com.sugar_lipstick_makeup_set_makeup_brushes_sale.cosmetics_makeup_face_powder_compact_powder_sale|130          |
# |0          |uk.vinted                                                                                         |130          |
# |0          |womens.cheap.clothes                                                                              |129          |
# |0          |com.taobao.htao.android                                                                           |129          |
# |0          |com.toko.online.net                                                                               |126          |
# |0          |com.fgf.shopping.game                                                                             |126          |
# |0          |com.buymeapie.bmap                                                                                |126          |
# |1          |com.app.tokobagus.betterb                                                                         |124          |
# |0          |com.glitterapps.fidget                                                                            |123          |
# |0          |com.wLapakLaris_13337038                                                                          |123          |
# |0          |com.rexsolution.golfreshmarket                                                                    |121          |
# |0          |com.komorebi.shoppinglist                                                                         |121          |
# |0          |ba.pik.v2.android                                                                                 |118          |
# |0          |one.track.app                                                                                     |118          |
# |0          |com.stylish.font.free                                                                             |118          |
# |0          |com.barcode.scanner.qr.scanner.app.qr.code.reader                                                 |117          |
# |0          |com.wkendarijualbeli_9915700                                                                      |115          |
# |0          |kids.onlineshopping                                                                               |115          |
# |0          |com.google.zxing.client.android                                                                   |115          |
# |0          |mobi.mobileforce.informa                                                                          |114          |
# |0          |air.tMinis                                                                                        |114          |
# |0          |com.indonesiashopping.sonu                                                                        |114          |
# |0          |com.onlineshoppingkorea.koreashopping                                                             |112          |
# |0          |com.gsretail.android.smapp                                                                        |111          |
# |0          |pl.tablica                                                                                        |111          |
# |0          |com.mourjan.classifieds                                                                           |111          |
# |0          |com.GomoGameDev.TopUpdanCaraBayarCodashop                                                         |111          |
# |0          |com.alimede.easyshopping                                                                          |110          |
# |0          |com.withsellit.app.sellit                                                                         |110          |
# |0          |com.handyapps.discountcalc                                                                        |110          |
# |0          |com.star5studio.HouseDecorationPlan.diamondHSLegend                                               |109          |
# |0          |com.wSIPLAHKamdikbud_10759627                                                                     |107          |
# |0          |com.situs.kuliner                                                                                 |106          |
# |0          |com.ionicframework.kalkulatorbelanja439506                                                        |106          |
# |0          |com.wKSMForMember_13916114                                                                        |106          |
# |0          |com.selloship.thedopeshop                                                                         |106          |
# |0          |jp.co.fablic.fril                                                                                 |106          |
# |0          |shopping.mobile.com.products                                                                      |104          |
# |0          |com.wESGOKAJEN_12352637                                                                           |103          |
# |0          |com.mybandungbarat.bandungbarat                                                                   |102          |
# |0          |com.myway.omr4                                                                                    |102          |
# |0          |com.myhighdominojackpotberuntung.highdominojackpotberuntung                                       |101          |
# |0          |com.headcode.ourgroceries                                                                         |100          |
# |0          |com.banggood.client                                                                               |100          |
# |0          |philippines.shopping.online.app                                                                   |99           |
# |0          |com.chotot.vn                                                                                     |99           |
# |0          |com.andromo.dev707753.app828918                                                                   |99           |
# |0          |com.airjordandeals.android                                                                        |98           |
# |0          |online.thailand.shopping                                                                          |98           |
# |0          |com.rodsapps.poptoshop.app                                                                        |98           |
# |0          |com.toreast.app                                                                                   |98           |
# |0          |com.bf.app0def94                                                                                  |96           |
# |0          |online.shoppingmarketplace                                                                        |95           |
# |0          |com.appnew.ubery2019orderinginstructions                                                          |95           |
# |0          |com.manash.purplle                                                                                |94           |
# |0          |com.B2B.Marketplace.PasarDigital.UMKMBUMN                                                         |94           |
# |0          |com.swiftspeedappcreator.android5ec91dd9d0622                                                     |94           |
# |0          |com.wTopkoin_12421106                                                                             |93           |
# |0          |com.lelong.buyer                                                                                  |92           |
# |0          |com.indomaret.klikindomaret                                                                       |92           |
# |0          |com.mmstore.apps                                                                                  |92           |
# |0          |shopping.list.free.lista.compra.gratis.liston                                                     |91           |
# |0          |com.summerseger.katalogZoyaHijabGamis                                                             |91           |
# |0          |com.tribunnews.tribunjualbeli                                                                     |90           |
# |0          |com.trd.lapakmobkas                                                                               |89           |
# |0          |com.dolap.android                                                                                 |89           |
# |0          |com.stickman.spider.rope.hero.gangstar.city                                                       |89           |
# |0          |com.ionicframework.avsi204446                                                                     |88           |
# |0          |mypoin.indomaret.android                                                                          |88           |
# |0          |com.tokohiggs.domino                                                                              |88           |
# |0          |f2game.gemsRoad.pirateDiamondPuzzle                                                               |87           |
# |0          |com.crispysoft.deliverycheck                                                                      |87           |
# |0          |com.ballbek.flying.shopping.mall.driver                                                           |86           |
# |0          |com.ZAM.AgenKosmetikTermurahJakarta                                                               |86           |
# |0          |com.glitterapps.glow                                                                              |86           |
# |0          |ru.zenden.android                                                                                 |86           |
# |0          |com.jasonmg.simsale                                                                               |86           |
# |0          |com.toys_kids_cheap.remotecontroltoys                                                             |85           |
# |0          |com.mobil123.www                                                                                  |85           |
# |0          |com.zzkko                                                                                         |84           |
# |0          |test.aplivjl                                                                                      |84           |
# |0          |com.summerseger.daftarhargaayam                                                                   |84           |
# |0          |com.luck_d                                                                                        |83           |
# |0          |com.coupang.mobile                                                                                |83           |
# |0          |com.noel.shop                                                                                     |83           |
# |0          |com.allinonestudio.chinedoapp                                                                     |83           |
# |0          |app.brickseek.com.brickseekapp                                                                    |82           |
# |0          |com.umjjal.gif.maker                                                                              |81           |
# |0          |com.chestersw.foodlist                                                                            |81           |
# |0          |web.id.isipulsa.data                                                                              |80           |
# |0          |cardgame.pokerasia.fourofakind.capsa.susun                                                        |80           |
# |0          |com.asemediatech.resiongkir                                                                       |80           |
# |0          |com.intersport.app.de                                                                             |79           |
# |0          |com.moneydolly                                                                                    |78           |
# |0          |com.hl.media.barcode.reader.free                                                                  |78           |
# |0          |com.treasurelistings.gsalr                                                                        |77           |
# |0          |app.ndtv.matahari                                                                                 |77           |
# |0          |com.ryananggowo.kumpulantokoonlineindonesiadalamsatuaplikasi                                      |77           |
# |0          |com.ayopop                                                                                        |77           |
# |0          |de.gavitec.android                                                                                |77           |
# |0          |com.blanja.apps.android                                                                           |76           |
# |0          |com.real.wallpaper                                                                                |76           |
# |0          |kr.defind.shoepik.users                                                                           |76           |
# |0          |com.izzedam.weedvva                                                                               |75           |
# |0          |com.myfkpntutorial.fkpntutorial                                                                   |75           |
# |0          |com.gloriajeans.mobile                                                                            |75           |
# |0          |com.clytie.app                                                                                    |75           |
# |0          |com.holashp.purchasebate                                                                          |75           |
# |0          |com.mobincube.hk_coupons.sc_E5UB92                                                                |74           |
# |0          |com.big.shopapp                                                                                   |74           |
# |0          |com.onlineuae.shopping                                                                            |74           |
# |0          |kr.co.camtalk                                                                                     |73           |
# |0          |id.jasamitra.app                                                                                  |73           |
# |0          |com.levistrauss.customer                                                                          |73           |
# |0          |best.home.compare.coupons.discounts.tracker.assistant.alert                                       |73           |
# |0          |com.online.smart.bracelet.watch.shopping.oss                                                      |73           |
# |0          |com.myBaubauJB.BaubauJB                                                                           |73           |
# |0          |com.shopkick.app                                                                                  |72           |
# |0          |nl.onlineretailservice.reclamefolderandroid                                                       |72           |
# |0          |com.elevenst                                                                                      |72           |
# |0          |com.hudsonsbay.android                                                                            |72           |
# |0          |com.zilingo.users                                                                                 |71           |
# |0          |com.simple_memo_pad_notes_app.babylon                                                             |71           |
# |0          |com.quikr                                                                                         |70           |
# |0          |com.smokeeffect.nameart                                                                           |70           |
# |0          |thecouponsapp.coupon                                                                              |70           |
# |0          |com.appybuilder.dhirajanand2009.OnlineToysShop                                                    |70           |
# |0          |com.mygshp.gshp                                                                                   |70           |
# |0          |id.toco                                                                                           |70           |
# |0          |my.com.nineyi.shop.s200074                                                                        |70           |
# |0          |com.fixeads.coisas                                                                                |70           |
# |0          |com.newandromo.dev535927.app804717                                                                |70           |
# |0          |com.FurnitureOnlineShopping                                                                       |69           |
# |0          |cimasv.appsaptod.guideforappmarketaptoide.tiosd                                                   |69           |
# |0          |app.anakoslab.cekmurah                                                                            |69           |
# |0          |com.Konoha.NarutoShippundenMod                                                                    |68           |
# |0          |com.premiumappsfactory.drone                                                                      |67           |
# |0          |com.fivejack.itemku                                                                               |67           |
# |0          |com.lyst.lystapp                                                                                  |67           |
# |0          |com.carsome.customer                                                                              |67           |
# |0          |best.apparel.compare.coupons.discounts.tracker.assistant.alert                                    |66           |
# |0          |com.indopub.codapayment                                                                           |66           |
# |0          |com.ClothingShoppingOnline                                                                        |66           |
# |0          |com.uniqlo.id.catalogue                                                                           |66           |
# |0          |com.mfar.flyerify                                                                                 |66           |
# |0          |com.onlineshoppingvietnam.vietnamshopping                                                         |66           |
# |0          |com.fineappstudio.android.petfriends                                                              |65           |
# |0          |com.promomobil.promomobil                                                                         |65           |
# |0          |javasign.com.cekrekening                                                                          |65           |
# |0          |com.topshop.shopping                                                                              |65           |
# |0          |com.bras.panty.nightwere.shop                                                                     |65           |
# |0          |cdiscount.mobile                                                                                  |65           |
# |0          |com.priceminister.buyerapp                                                                        |64           |
# |0          |com.onlinecoupons.bathandbody                                                                     |64           |
# |0          |net.giosis.shopping.jp                                                                            |64           |
# |0          |com.wAntaraMotor_8841334                                                                          |64           |
# |0          |com.septiancell.site                                                                              |64           |
# |0          |com.ranjith888999.onlinefurniturestore                                                            |64           |
# |0          |com.pinteng.lovelywholesale                                                                       |63           |
# |0          |com.bikinaplikasi.cantikshop24                                                                    |63           |
# |0          |com.socialapps.homeplus                                                                           |63           |
# |0          |com.bestappsale                                                                                   |63           |
# |0          |com.thailand.onlineshopping                                                                       |62           |
# |0          |web.id.isipulsa.app                                                                               |62           |
# |0          |ru.puma.android                                                                                   |61           |
# |0          |genius.trade2                                                                                     |61           |
# |0          |com.mcraftmaps.parkour                                                                            |61           |
# |0          |com.indoarjuna.gudangtasgrosir                                                                    |61           |
# |0          |us.tokoonline.gratisongkir.bayarditempat                                                          |61           |
# |0          |com.bikinaplikasi.rsfashiongrosir                                                                 |60           |
# |0          |com.onlineksa.shopping                                                                            |60           |
# |0          |com.myayudidesign.ayudidesign                                                                     |60           |
# |0          |com.bikinaplikasi.brosayur                                                                        |60           |
# |0          |at.willhaben                                                                                      |59           |
# |0          |com.zoltopshopla.shop                                                                             |59           |
# |0          |com.faunesia.apk                                                                                  |59           |
# |0          |com.glitterapps.near                                                                              |58           |
# |0          |com.mobappsbaker.kwaroffers                                                                       |58           |
# |0          |shopping.app.offer.deals.us.shopping                                                              |58           |
# |0          |right.apps.gleamaways                                                                             |58           |
# |0          |com.rkskdldl.smartstore_forseller                                                                 |58           |
# |0          |co.fusionweb.blackfriday                                                                          |58           |
# |0          |com.airjordanoutlet.android                                                                       |58           |
# |0          |com.ebay.kr.gmarket                                                                               |57           |
# |0          |web.id.isipulsa.agenkuota                                                                         |57           |
# |0          |com.ingonan.store                                                                                 |57           |
# |0          |com.mymamabelanjaindonesia.mamabelanjaindonesia                                                   |57           |
# |0          |com.realmestore.app                                                                               |57           |
# |0          |com.ria.auto                                                                                      |57           |
# |0          |com.amazon_online_mobile_shopping.flipkart_mobile_sale                                            |56           |
# |0          |com.wemakeprice                                                                                   |56           |
# |0          |com.indonesia_shopping.zakir                                                                      |56           |
# |0          |alejo.converse                                                                                    |56           |
# |0          |tukutuku.sn                                                                                       |56           |
# |0          |com.summerseger.katalogerhadaftarharga                                                            |56           |
# |0          |com.jockey_women_underwear_clovia_bra.online_jockey_sexy_nighties_hot_bra_lingeries               |55           |
# |0          |com.mymindoapplication.mindoapplication                                                           |55           |
# |0          |com.jumia.android                                                                                 |55           |
# |0          |com.wSARAVANATRADERS_9366627                                                                      |55           |
# |0          |com.wCodashopIndonesia_10155135                                                                   |54           |
# |0          |com.souq.app                                                                                      |53           |
# |0          |com.newandromo.dev535927.app1241888                                                               |53           |
# |0          |gr.playstore.studio.android5b8fc185820c2                                                          |53           |
# |0          |za.co.meeser.inglenook                                                                            |53           |
# |0          |com.hartono.elektronika                                                                           |52           |
# |0          |com.olx.olx                                                                                       |52           |
# |0          |com.onlineshopping.thailandshopping                                                               |52           |
# |0          |com.yanedev.browserforlazadacoupons                                                               |52           |
# |0          |id.serpul.sp3576                                                                                  |52           |
# |0          |be.tweedehands.m                                                                                  |52           |
# |0          |com.taobao.taobao                                                                                 |52           |
# |0          |com.iqouz.gameo                                                                                   |51           |
# |0          |com.anaonlineshopping.babytoys                                                                    |51           |
# |0          |com.bengkeldroid.sophie                                                                           |51           |
# |0          |com.droneflight_shoppingonline.drone                                                              |50           |
# |0          |se.scmv.belarus                                                                                   |50           |
# |0          |bra.shoppingapp                                                                                   |50           |
# |0          |com.Spark.Skillz                                                                                  |50           |
# |0          |pl.vinted                                                                                         |49           |
# |0          |com.wsayurQu_10842373                                                                             |49           |
# |0          |com.oriflame.oriflame                                                                             |49           |
# |0          |com.japanshopping.sonu                                                                            |49           |
# |0          |com.mygarutfood.garutfood                                                                         |49           |
# |0          |com.insurance.guide2019                                                                           |49           |
# |0          |com.chloesoft.pbapp                                                                               |48           |
# |0          |bima101sok.java                                                                                   |48           |
# |0          |com.olshop.cekharga                                                                               |48           |
# |0          |com.anaonlineshopping.makeupshopping                                                              |48           |
# |0          |app.goubba.com                                                                                    |48           |
# |0          |com.appstrdesign.buysell                                                                          |48           |
# |0          |codashop.apubp                                                                                    |47           |
# |0          |ch.anibis.anibis                                                                                  |47           |
# |0          |com.sayurbox                                                                                      |47           |
# |0          |shopping.list.grocery.recipes.coupons                                                             |47           |
# |0          |indonesia.id.indonesiaonlineshops                                                                 |47           |
# |0          |kz.slando                                                                                         |47           |
# |0          |com.wBluetoothSpeaker_7669770                                                                     |47           |
# |0          |com.glitterapps.menunderwearstore                                                                 |46           |
# |0          |com.dualgram.multiple.record.camera.guide                                                         |46           |
# |0          |com.endless.myshoppinglist                                                                        |46           |
# |0          |com.mm.views                                                                                      |46           |
# |0          |com.pisgo.oneplusone                                                                              |46           |
# |0          |com.DramaProductions.Einkaufen5                                                                   |46           |
# |0          |indopub.topup.upoint                                                                              |46           |
# |0          |com.ONLINESHOPINDONESIA_8626456                                                                   |45           |
# |0          |com.MCPEMOD.MilitarymodforMCPE                                                                    |45           |
# |0          |com.coruscate.organichome                                                                         |45           |
# |0          |asia.grosirpakaian.onlineshop                                                                     |45           |
# |0          |de.kleiderkreisel                                                                                 |45           |
# |0          |sales.cashback.shopping.master                                                                    |45           |
# |0          |com.qxl.Client                                                                                    |45           |
# |0          |ch.tutti                                                                                          |45           |
# |0          |com.jafolders.allefolders                                                                         |44           |
# |0          |com.ujudebug.organicbazaar                                                                        |44           |
# |0          |com.carstoys.onlineshopping_toycar                                                                |44           |
# |0          |com.club.dx.gb.luckygift                                                                          |44           |
# |0          |com.newandromo.dev969116.app1191840                                                               |44           |
# |0          |com.premiumappsfactory.lust                                                                       |44           |
# |0          |com.muba.anuncios                                                                                 |43           |
# |0          |com.apprancher.slime                                                                              |43           |
# |0          |code.expert.lulumarketpromotions                                                                  |43           |
# |0          |com.machtwatch_mobile                                                                             |43           |
# |0          |com.wWAYGO_13944726                                                                               |43           |
# |0          |io.oskm.hotdeals                                                                                  |43           |
# |0          |show.grip                                                                                         |43           |
# |0          |com.wBarangRongsok_12719277                                                                       |43           |
# |0          |com.zaful                                                                                         |42           |
# |0          |com.lymin.codashopfree                                                                            |42           |
# |0          |asia.acommerce.adidasid                                                                           |42           |
# |0          |com.jaknot                                                                                        |42           |
# |0          |com.wSHOP101APPONLINESHOPPINGINDIA_9585106                                                        |42           |
# |0          |com.brashoppingapp                                                                                |41           |
# |0          |com.ashby.everykart                                                                               |41           |
# |0          |com.itmobix.ksaendeals                                                                            |41           |
# |0          |net.furimawatch.fmw                                                                               |41           |
# |0          |com.apps.globalmobileprices.mobile                                                                |41           |
# |0          |com.mygacek.gacek                                                                                 |41           |
# |0          |com.kakaku.kakakucomapp                                                                           |41           |
# |0          |com.app.dealfish.main                                                                             |40           |
# |0          |com.poolballcoins                                                                                 |40           |
# |0          |com.jasonmg.market09                                                                              |40           |
# |0          |com.aptivdev.bra                                                                                  |40           |
# |0          |com.nttdocomo.android.dpoint                                                                      |39           |
# |0          |com.danimaster.reviews.hot.deals                                                                  |39           |
# |0          |com.tise.tise                                                                                     |39           |
# |0          |com.alfacart.apps                                                                                 |39           |
# |0          |com.ZAM.AGENKOSMETIKSURABAYA                                                                      |39           |
# |0          |com.tcm.supertreasure                                                                             |38           |
# |0          |web.id.isipulsa.appku                                                                             |38           |
# |0          |io.makeroid.bsdeora55520.usa                                                                      |38           |
# |0          |com.bikinaplikasi.wosupplier                                                                      |38           |
# |0          |com.anaonlineshopping.weddingdress                                                                |38           |
# |0          |com.mobappsbaker.uaeoffers                                                                        |38           |
# |0          |online.thailand.shopping.app                                                                      |38           |
# |0          |com.maxxum.server.distributoraccessories2                                                         |38           |
# |0          |id.agenkosmetik.store                                                                             |38           |
# |0          |com.androidapp.mblif                                                                              |37           |
# |0          |jewelry.bijouterie.cheap                                                                          |37           |
# |0          |com.lottemart.shopping                                                                            |37           |
# |0          |com.shop.uzumarket                                                                                |37           |
# |0          |com.checkpoints.app                                                                               |37           |
# |0          |com.manggaduajakarta.onlineshop                                                                   |37           |
# |0          |com.shooperapp                                                                                    |37           |
# |0          |com.powergirdcar.egyptWitch.TreasureHunter                                                        |37           |
# |0          |com.razerzone.cortex.dealsv2                                                                      |37           |
# |0          |com.shoesshoppingapp                                                                              |36           |
# |0          |com.erafone.bct.lk6                                                                               |36           |
# |0          |com.niapp.cekresiallinone                                                                         |36           |
# |0          |com.caracaracaramudah.caracarakredithponlinetanpakartukredit                                      |36           |
# |0          |company.wishupon                                                                                  |36           |
# |0          |com.kiwi.customer                                                                                 |36           |
# |0          |com.galaxyfirsatlari                                                                              |35           |
# |0          |com.flipkart.shopsy                                                                               |35           |
# |0          |com.nogamelabs.parcel.tracker                                                                     |35           |
# |0          |com.wTrackingAliexpress_12686734                                                                  |35           |
# |0          |com.jasonmg.salepoison                                                                            |35           |
# |0          |com.empg.olxom                                                                                    |35           |
# |0          |com.cupshe.cupshe                                                                                 |35           |
# |0          |com.wCuelinksAffiliate_12013676                                                                   |34           |
# |0          |jp.id_credit_sp.android                                                                           |34           |
# |0          |com.watsons.id.android                                                                            |34           |
# |0          |com.ZAM.AgenJilbabMurah                                                                           |34           |
# |0          |com.diamond.freefireindo                                                                          |34           |
# |0          |com.wBeiShopping_8806468                                                                          |34           |
# |0          |com.wUMKMOKE_13060134                                                                             |34           |
# |0          |com.mor.ntnu.appstore                                                                             |34           |
# |0          |com.abmo.diamonshop                                                                               |34           |
# |0          |com.allshopping.aio                                                                               |34           |
# |0          |com.studiodevs.womensintimates_bra                                                                |34           |
# |0          |com.fly.chat.messenger                                                                            |33           |
# |0          |com.tippingcanoe.promodescuentos                                                                  |33           |
# |0          |com.onlineshoppingksa.saudiarabiashopping                                                         |33           |
# |0          |com.ff.diamond.zato                                                                               |33           |
# |0          |br.com.dafiti                                                                                     |33           |
# |0          |com.woodoo                                                                                        |33           |
# |0          |com.forsale.forsale                                                                               |33           |
# |0          |kgkgkg.hong.hscanner                                                                              |33           |
# |0          |com.smarttech.catalogs                                                                            |32           |
# |0          |com.devbigl.apkproj                                                                               |32           |
# |0          |ru.sunlight.sunlight                                                                              |32           |
# |0          |solusi.mediapanel                                                                                 |32           |
# |0          |com.anaonlineshopping.beautystore                                                                 |31           |
# |0          |fr.snapp.fidme                                                                                    |31           |
# |0          |com.onlineshopping.IndonesiaShopping                                                              |31           |
# |0          |com.playstation.com                                                                               |31           |
# |0          |com.arata1972.yoshinoya                                                                           |31           |
# |0          |com.glitterapps.toy                                                                               |31           |
# |0          |com.studioapps.chinashoping_onlineshopping                                                        |30           |
# |0          |com.gameskharido.co.in                                                                            |30           |
# |0          |com.poppyplaytimetips.mommylong.legsberuntung                                                     |30           |
# |0          |com.thailandshopping.sonu                                                                         |30           |
# |0          |singapore.shopping.app                                                                            |30           |
# |0          |id.co.ikea.android                                                                                |29           |
# |0          |com.higgsdominostore.apps                                                                         |29           |
# |0          |com.bird.kaichon                                                                                  |29           |
# |0          |com.bradsdeals                                                                                    |29           |
# |0          |jb.hifi2                                                                                          |29           |
# |0          |us.tokoonline.alatpancing                                                                         |29           |
# |0          |com.bikinaplikasi.kusone                                                                          |29           |
# |0          |com.wFAUZANSHOP_13513459                                                                          |29           |
# |0          |com.wAppleStore_13962473                                                                          |29           |
# |0          |com.shoppinglist                                                                                  |28           |
# |0          |ru.cardsmobile.mw3                                                                                |28           |
# |0          |com.on9store.shopping.tokoonlinepromo                                                             |28           |
# |0          |com.htechnology.heriyanto_sirait.kedeid                                                           |28           |
# |0          |com.viphashtags.app                                                                               |28           |
# |0          |ssense.android.prod                                                                               |28           |
# |0          |yqtrack.app                                                                                       |28           |
# |0          |com.wRKONLYBOZ_9243961                                                                            |28           |
# |0          |com.gumtree.android.beta                                                                          |28           |
# |0          |com.kame3.apps.calcforshopping                                                                    |28           |
# |0          |com.kt.accessory                                                                                  |28           |
# |0          |com.letyshops                                                                                     |27           |
# |0          |com.vietnam.onlineshopping                                                                        |27           |
# |0          |sorisoft.co.kr.convgs                                                                             |27           |
# |0          |com.aio.allinone.onlineshopping                                                                   |27           |
# |0          |com.buyfansonline.fansandcoolersonline                                                            |27           |
# |0          |com.twelvestoreez                                                                                 |27           |
# |0          |com.xmaslist                                                                                      |27           |
# |0          |com.phone.shop.app                                                                                |27           |
# |0          |com.online.shoppingjapan                                                                          |27           |
# |0          |com.mobile.pomelo                                                                                 |27           |
# |0          |com.glitterapps.drones                                                                            |27           |
# |0          |pk.com.whatmobile.whatmobile                                                                      |26           |
# |0          |com.glitterapps.phonecaseshop                                                                     |26           |
# |0          |com.shoesdeals.app                                                                                |26           |
# |0          |id.compro.kartikaaccessories                                                                      |26           |
# |0          |com.nlsavenger.royalDiamond.homeRenovateHouseDecorate                                             |26           |
# |0          |com.appoler.nisnokeed                                                                             |26           |
# |0          |com.mylapaktrans.lapaktrans                                                                       |26           |
# |0          |com.coolapps.miband.hulyaapp                                                                      |26           |
# |0          |com.linonlineshopping.remotecontrolcar                                                            |26           |
# |0          |com.rdev.rajapediadigital                                                                         |26           |
# |0          |com.menshoesshoppingapps                                                                          |26           |
# |0          |net.bbnapp.shopping.letgo                                                                         |26           |
# |0          |com.msint.myshopping.list                                                                         |26           |
# |0          |oskm.io.eventopia                                                                                 |26           |
# |0          |com.mobeasyapp.app414331889627                                                                    |25           |
# |0          |cl.yapo                                                                                           |25           |
# |0          |the.semicolon.geantmarketpromotions                                                               |25           |
# |0          |com.si.simembers                                                                                  |25           |
# |0          |kr.co.lgfashion.lgfashionshop.v28                                                                 |25           |
# |0          |com.sharpninja.ninja2                                                                             |25           |
# |0          |id.esoft.bajojo                                                                                   |25           |
# |0          |online.marketplace.ae                                                                             |25           |
# |0          |com.whitelabel.putravoucher                                                                       |25           |
# |0          |com.mdkuriruser.pelanggan                                                                         |25           |
# |0          |com.vatsap.whats.fm                                                                               |25           |
# |0          |br.com.encomendas                                                                                 |24           |
# |0          |dk.dba.android                                                                                    |24           |
# |0          |fragrancebuy.android.app                                                                          |24           |
# |0          |com.sdgcode.discountcalculator                                                                    |24           |
# |0          |com.wMultiKreditBandung_11656246                                                                  |24           |
# |0          |com.elevenst.deals                                                                                |24           |
# |0          |com.borneosell.kalimantan                                                                         |24           |
# |0          |com.wKidsShop_12842603                                                                            |24           |
# |0          |com.noon.buyerapp                                                                                 |24           |
# |0          |com.glitterapps.womenunderwearonlinestore                                                         |24           |
# |0          |by.uniq.package_tracker                                                                           |24           |
# |0          |com.styleshare.android                                                                            |24           |
# |0          |com.luizalabs.mlapp                                                                               |23           |
# |0          |com.jakmall                                                                                       |23           |
# |0          |com.kaskus.fjb                                                                                    |23           |
# |0          |com.optikmelawai.optikmelawai                                                                     |23           |
# |0          |com.eastpal.android.mnuri                                                                         |23           |
# |0          |com.ebay.gumtree.za                                                                               |23           |
# |0          |com.dai.giftcard.viewer                                                                           |23           |
# |0          |us.toko.bagus                                                                                     |23           |
# |0          |com.gramedia.retail                                                                               |23           |
# |0          |com.ril.ajio                                                                                      |23           |
# |0          |vietnam.shopping.online.app                                                                       |23           |
# |0          |com.imedicalapps.discountcalculator                                                               |23           |
# |0          |com.toptoshirou.toplandarea                                                                       |23           |
# |0          |com.bigsaving.shopeesale                                                                          |23           |
# |0          |com.winds.dcfeverreader                                                                           |23           |
# |0          |shopping.app14                                                                                    |23           |
# |0          |com.bigbasket.mobileapp                                                                           |22           |
# |0          |com.dapurmancing.android                                                                          |22           |
# |0          |com.oblivion.tokoonline                                                                           |22           |
# |0          |org.tokoonlineindonesia.apps.tokoonlinebersama                                                    |22           |
# |0          |com.katalogpromoupdate.katalogpromoupdate                                                         |22           |
# |0          |tool.qrbarcodemaker.qrbarcodeScanner                                                              |22           |
# |0          |com.fsn.nykaa                                                                                     |22           |
# |0          |com.landwirt                                                                                      |22           |
# |0          |com.anaonlineshopping.mensjeans                                                                   |22           |
# |0          |com.mobileappruparupa                                                                             |22           |
# |0          |redeemapp.card                                                                                    |22           |
# |0          |com.anaonlineshopping.womenjeans                                                                  |22           |
# |0          |jewel.match.bmc                                                                                   |22           |
# |0          |com.anaonlineshopping.babyclothes_shoppingapps                                                    |22           |
# |0          |com.fivefly.android.shoppinglist                                                                  |22           |
# |0          |ro.mercador                                                                                       |22           |
# |0          |com.ePositiveApps.olxx                                                                            |22           |
# |0          |com.hongkong.onlineshopping                                                                       |22           |
# |0          |com.mandarin.android                                                                              |22           |
# |0          |com.ZAM.SupplierKosmetikKorea                                                                     |22           |
# |0          |com.goodbarber.starbucksmenu                                                                      |22           |
# |0          |com.kioser.app                                                                                    |22           |
# |0          |com.samsungmall                                                                                   |22           |
# |0          |com.grape.software.offcoupon                                                                      |22           |
# |0          |tv.jamlive                                                                                        |22           |
# |0          |pk.com.vpro.mybirds                                                                               |22           |
# |0          |com.empg.olxsa                                                                                    |22           |
# |0          |com.tradera                                                                                       |22           |
# |0          |co.alfataqu.id                                                                                    |21           |
# |0          |com.my.ffaccounts                                                                                 |21           |
# |0          |com.xunmeng.pinduoduo                                                                             |21           |
# |0          |com.tul.tatacliq                                                                                  |21           |
# |0          |com.onlinecoupons.wish                                                                            |21           |
# |0          |com.gardrops                                                                                      |21           |
# |0          |de.quoka.kleinanzeigen                                                                            |21           |
# |0          |com.numatams.numatamsapps.doublediscountcalc                                                      |21           |
# |0          |shopfreeship.onlineshopping                                                                       |21           |
# |0          |com.wBaleStartupTukang_13653202                                                                   |21           |
# |0          |com.newandromo.dev500606.app698315                                                                |21           |
# |0          |com.ernestoyaquello.lista.de.la.compra                                                            |21           |
# |0          |kr.co.ssg                                                                                         |21           |
# |0          |offerup.letgousedstuff.wishshopping.guideforofferupbuysell.walmartcouponsdiscounts5miles          |20           |
# |0          |com.moblyapp                                                                                      |20           |
# |0          |com.anaonlineshopping.handbags_women                                                              |20           |
# |0          |com.swaypay.swaypay                                                                               |20           |
# |0          |com.dealsali.alideals                                                                             |20           |
# |0          |com.wRUBYMINIGOLDSUBDEALER_14995213                                                               |20           |
# |0          |com.appybuilder.ajayhi0009.SIBrowser                                                              |20           |
# |0          |com.onlineshopping.kidstoys                                                                       |20           |
# |0          |com.xiaomi.shop                                                                                   |20           |
# |0          |au.com.oo                                                                                         |20           |
# |0          |com.onlinecoupons.victoriassecret                                                                 |20           |
# |0          |com.bikinaplikasi.grosirtaswanita                                                                 |20           |
# |0          |com.dev.demidust.promoapp                                                                         |20           |
# |0          |net.cumart.ccw                                                                                    |20           |
# |0          |online.turkey.shopping                                                                            |20           |
# |0          |com.pmi.limited.pmiappm05728                                                                      |19           |
# |0          |com.yaari                                                                                         |19           |
# |0          |hawkvladz.com.rcmart                                                                              |19           |
# |0          |com.appscentral.free_slime_shop                                                                   |19           |
# |0          |com.appyacenter.babyclothes                                                                       |19           |
# |0          |com.jabong.android                                                                                |19           |
# |0          |ru.filit.mvideo.b2c                                                                               |19           |
# |0          |com.samsung.ecomm.global.in                                                                       |19           |
# |0          |earn.moneyplayandwin                                                                              |19           |
# |0          |com.empg.olxkw                                                                                    |19           |
# |0          |com.lenskart.app                                                                                  |19           |
# |0          |com.hnsmall                                                                                       |19           |
# |0          |com.devplank.rastreiocorreios                                                                     |19           |
# |0          |online.shopping.philippines                                                                       |19           |
# |0          |com.mygrownewsa.grownewsa                                                                         |19           |
# |0          |lazada.deals.vouchers                                                                             |19           |
# |0          |com.topindo.android                                                                               |19           |
# |0          |com.mgs.onlineshoppingjapan                                                                       |19           |
# |0          |com.dubarter.androidApp                                                                           |18           |
# |0          |com.summerseger.daftarhargacabe                                                                   |18           |
# |0          |com.tokyocoldstudio.WitchDiamondBlitz                                                             |18           |
# |0          |app.creator203                                                                                    |18           |
# |0          |com.paybright.portal                                                                              |18           |
# |0          |com.listia.Listia                                                                                 |18           |
# |0          |com.ebay.global.gmarket                                                                           |18           |
# |0          |tj.somon.somontj                                                                                  |18           |
# |1          |com.adfone.indosat                                                                                |18           |
# |0          |com.cyber_monday.cybermonday2015                                                                  |18           |
# |0          |com.wSlimeShop_12288272                                                                           |18           |
# |0          |com.allinone.mobile_recharge.mobilerechargeapp                                                    |18           |
# |0          |com.ccode.coupon                                                                                  |18           |
# |0          |apps.gmart                                                                                        |18           |
# |0          |il.co.PriceZ.android                                                                              |18           |
# |0          |com.oxigen.futurepay                                                                              |18           |
# |0          |com.rabbit.bakpaorabbit                                                                           |18           |
# |0          |com.mocasa.ph                                                                                     |18           |
# |0          |com.langital.app                                                                                  |18           |
# |0          |com.wTopChineseOnlineShopping_12625474                                                            |18           |
# |0          |com.airjordans.android                                                                            |18           |
# |0          |com.trixiesoft.clapp                                                                              |18           |
# |0          |furniture.online.shopping                                                                         |18           |
# |0          |com.korchix.sales                                                                                 |18           |
# |0          |com.reserves.shop                                                                                 |18           |
# |0          |com.glitterapps.popit                                                                             |18           |
# |0          |com.olx.ssa.gh                                                                                    |18           |
# |0          |com.bunz                                                                                          |18           |
# |0          |com.riverview.opticseis                                                                           |18           |
# |0          |com.turboodev.vetements                                                                           |18           |
# |0          |com.bewakoof.bewakoof                                                                             |17           |
# |0          |com.pradiste.app                                                                                  |17           |
# |0          |com.tophatter                                                                                     |17           |
# |0          |com.onlineshoppinggermany.germanyshopping                                                         |17           |
# |0          |com.simplesmartsoft.mylist                                                                        |17           |
# |0          |com.buyrefrigeratorssonline.buyfridgessonline                                                     |17           |
# |0          |com.apprancher.freeantistresstoys                                                                 |17           |
# |0          |com.online.kitchen.appliances.shops                                                               |17           |
# |0          |id.histore                                                                                        |17           |
# |0          |com.wSHOP101CODSHOPPINGINDIA_12256769                                                             |17           |
# |0          |com.wNikeExpressFactoryDirect_8704826                                                             |17           |
# |0          |com.mykadaionline.kadaionline                                                                     |17           |
# |0          |com.inditex.pullandbear                                                                           |17           |
# +-----------+--------------------------------------------------------------------------------------------------+-------------+
# """
#
#
# """
# after_label	app	device_id_cnt
# 1	N                                                                                                 	10394
# 1	id.co.shopintar                                                                                   	327
# 1	com.app.tokobagus.betterb                                                                         	124
# 1	com.adfone.indosat                                                                                	18
# """
#
# """
# after_label	app	device_id_cnt
# 0	N                                                                                                 	243605187
# 0	id.co.shopintar                                                                                   	9971428
# 0	com.app.tokobagus.betterb                                                                         	7314356
# 0	com.shopee.id                                                                                     	1686219
# 0	com.alibaba.aliexpresshd                                                                          	1251127
# 0	blibli.mobile.commerce                                                                            	776590
# 0	com.adfone.indosat                                                                                	718997
# 0	com.tokopedia.tkpd                                                                                	428325
# 0	com.adfone.unefon                                                                                 	427936
# 0	com.thecarousell.Carousell                                                                        	305431
# 0	com.bukalapak.mitra                                                                               	228538
# 0	com.alibaba.intl.android.apps.poseidon                                                            	218589
# 0	io.silvrr.installment                                                                             	117196
# 0	com.newchic.client                                                                                	99807
# 0	com.cari.promo.diskon                                                                             	76514
# 0	com.bukalapak.android                                                                             	71570
# 0	indopub.unipin.topup                                                                              	36245
# """
#
# df_3=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/20220701_delay_1").where("now_label=0")
# bundle_app_set_default = F.array(F.lit("N"))
# fill_rule = F.when(F.col("bundle_app_set").isNull(), bundle_app_set_default).otherwise(F.col("bundle_app_set"))
# df_4 = df.fillna("N",subset=["osversion"]).fillna(0, subset=["bundle_app_set_size"]).withColumn("bundle_app_set_temp", fill_rule).drop("bundle_app_set").withColumnRenamed("bundle_app_set_temp","bundle_app_set").persist()
# df_4.createTempView("df_4_table")
#
# spark.sql(" \
#     select  \
#         after_label,app \
#         ,count(device_id) as device_id_cnt   \
#     from    \
#     (   \
#         select  \
#             device_id   \
#             ,after_label    \
#             ,app    \
#         from    \
#             df_4_table  \
#         lateral view    \
#             explode(bundle_app_set) as app  \
#     ) t1    \
#     group by    \
#         after_label,app \
#     order by    \
#         device_id_cnt desc  \
# ").show(1000,truncate=False)
#
# """
# +-----------+--------------------------------------------------------------------------------------------------+-------------+
# |after_label|app                                                                                               |device_id_cnt|
# +-----------+--------------------------------------------------------------------------------------------------+-------------+
# |0          |N                                                                                                 |243986363    |
# |0          |id.co.shopintar                                                                                   |9979770      |
# |0          |com.app.tokobagus.betterb                                                                         |7317355      |
# |0          |com.shopee.id                                                                                     |1683703      |
# |0          |com.alibaba.aliexpresshd                                                                          |1251939      |
# |0          |blibli.mobile.commerce                                                                            |777269       |
# |0          |com.adfone.indosat                                                                                |719198       |
# |0          |com.adfone.unefon                                                                                 |480932       |
# |0          |com.tokopedia.tkpd                                                                                |427759       |
# |0          |com.thecarousell.Carousell                                                                        |305711       |
# |0          |com.bukalapak.mitra                                                                               |228097       |
# |0          |com.alibaba.intl.android.apps.poseidon                                                            |218727       |
# |0          |io.silvrr.installment                                                                             |117031       |
# |0          |com.newchic.client                                                                                |99694        |
# |0          |com.cari.promo.diskon                                                                             |76395        |
# |0          |com.bukalapak.android                                                                             |71562        |
# |0          |indopub.unipin.topup                                                                              |36198        |
# |0          |studio.pvs.t_shirtdesignstudio                                                                    |33396        |
# |0          |com.onlinecoupons.lazada                                                                          |30730        |
# |0          |com.rootbridge.ula                                                                                |25984        |
# |0          |com.yoinsapp                                                                                      |25617        |
# |0          |com.application_4u.qrcode.barcode                                                                 |25616        |
# |0          |com.memopad.for_.writing.babylon                                                                  |24622        |
# |0          |com.propcoid.propcoid                                                                             |21182        |
# |0          |universal.app.cek.resi.shopee.express                                                             |17016        |
# |0          |com.pim.pulsapedia                                                                                |15368        |
# |0          |com.jollycorp.jollychic                                                                           |14242        |
# |0          |diamond.via.id                                                                                    |14071        |
# |0          |com.pixelkoin.app                                                                                 |13951        |
# |0          |com.Alltor.CodaFree                                                                               |13466        |
# |0          |com.app.offerit                                                                                   |13307        |
# |0          |com.grannyrewards.app                                                                             |12732        |
# |0          |com.kkmart.user                                                                                   |12573        |
# |0          |indopub.codashop.popup                                                                            |12439        |
# |0          |com.brightstripe.parcels                                                                          |12045        |
# |0          |com.geomobile.tiendeo                                                                             |11873        |
# |0          |com.motivs.marketplaceeid                                                                         |11749        |
# |0          |com.ptmarketplaceio.jualxbeli                                                                     |11556        |
# |0          |co.tapcart.app.id_wO7BWzmfNM                                                                      |11123        |
# |0          |com.shopee.ph                                                                                     |10131        |
# |1          |N                                                                                                 |10085        |
# |0          |app.idcshop.com                                                                                   |9601         |
# |0          |com.livicepat.tokolengkap                                                                         |9503         |
# |0          |com.ajsneakeronline.android                                                                       |9250         |
# |0          |com.checkoutcharlie.shopmate                                                                      |8492         |
# |0          |com.primarkshopstore.shopukprimark                                                                |8050         |
# |0          |com.mrbox.guanfang.large                                                                          |8047         |
# |0          |com.kiodeecfeedgtp.wip                                                                            |8045         |
# |0          |com.topsmarkets.app.android                                                                       |7890         |
# |0          |com.gdmagri.app                                                                                   |7868         |
# |0          |freefire.id                                                                                       |7742         |
# |0          |com.voixme.d4d                                                                                    |7419         |
# |0          |com.geekslab.qrbarcodescanner.pro                                                                 |7246         |
# |0          |jd.cdyjy.overseas.market.indonesia                                                                |7228         |
# |0          |com.snkrdunk.en.android                                                                           |7159         |
# |0          |com.nz4.diamondsff                                                                                |7106         |
# |0          |com.shopee.my                                                                                     |6974         |
# |0          |com.tycmrelx.cash                                                                                 |6595         |
# |0          |com.kanzengames                                                                                   |6558         |
# |0          |com.agromaret.android.app                                                                         |6466         |
# |0          |com.codmobil.mobilcod                                                                             |6463         |
# |0          |evermos.evermos.com.evermos                                                                       |6394         |
# |0          |kr.co.quicket                                                                                     |5928         |
# |0          |diamonds.ff.viapulsa                                                                              |5799         |
# |0          |com.l                                                                                             |5737         |
# |0          |com.zulily.android                                                                                |5718         |
# |0          |com.asemediatech.cekresiongkir                                                                    |5424         |
# |0          |develop.joo.scanbarcodeharga                                                                      |5338         |
# |0          |mmapps.mobile.discount.calculator                                                                 |4941         |
# |0          |com.application_4u.qrcode.barcode.scanner.reader.flashlight                                       |4696         |
# |0          |com.ebay.mobile                                                                                   |4652         |
# |0          |com.airjordanshop.android                                                                         |4640         |
# |0          |com.locket.locketguide                                                                            |4634         |
# |0          |com.giftmill.giftmill                                                                             |4411         |
# |0          |com.cakecodes.bitmaker                                                                            |4403         |
# |0          |com.iljovita.hargapromokatalog3                                                                   |4118         |
# |0          |net.tsapps.appsales                                                                               |3960         |
# |0          |com.Expandear.KatalogPromo                                                                        |3883         |
# |0          |com.ILJOVITA.android.universalwebview                                                             |3735         |
# |0          |com.mudah.my                                                                                      |3662         |
# |0          |id.allofresh.ecommerce                                                                            |3522         |
# |0          |com.perekrestochek.app                                                                            |3509         |
# |0          |com.interfocusllc.patpat                                                                          |3486         |
# |0          |com.mygamezone.gamezone                                                                           |3399         |
# |0          |yotcho.shop                                                                                       |3323         |
# |0          |fr.casino.fidelite                                                                                |3232         |
# |0          |com.tiktokshop.tiktokshop                                                                         |3012         |
# |0          |com.on9store.shopping.allpromotion                                                                |3012         |
# |0          |com.todayapp.cekresi                                                                              |2929         |
# |0          |com.ebay.gumtree.au                                                                               |2712         |
# |0          |com.joom                                                                                          |2701         |
# |0          |com.androidrocker.qrscanner                                                                       |2676         |
# |0          |com.CouponChart                                                                                   |2670         |
# |0          |com.offerup                                                                                       |2662         |
# |0          |com.shoppingapp.shopeeeasa                                                                        |2626         |
# |0          |com.tuck.hellomarket                                                                              |2572         |
# |0          |com.whaleshark.retailmenot                                                                        |2520         |
# |0          |com.wKLIKGAMESHOPAdalahMitraAgenChipHiggsDominoIsland_13240116                                    |2463         |
# |0          |com.xbox_deals.sales                                                                              |2423         |
# |0          |com.zalora.android                                                                                |2419         |
# |0          |com.abtnprojects.ambatana                                                                         |2418         |
# |0          |shopping.indonesia.com.app                                                                        |2360         |
# |0          |com.globalbusiness.countrychecker                                                                 |2353         |
# |0          |com.shopee.br                                                                                     |2312         |
# |0          |com.onlineshoppingindonesia.indonesiashopping                                                     |2237         |
# |0          |com.biggu.shopsavvy                                                                               |2209         |
# |0          |com.my.exchangecounter                                                                            |2206         |
# |0          |fr.vinted                                                                                         |2104         |
# |0          |com.lootboy.app                                                                                   |2069         |
# |0          |com.olx.southasia                                                                                 |1978         |
# |0          |com.ebay.kleinanzeigen                                                                            |1953         |
# |0          |com.kohls.mcommerce.opal                                                                          |1930         |
# |0          |hajigaming.codashop.malaysia                                                                      |1823         |
# |0          |com.shopee.th                                                                                     |1809         |
# |0          |aw.kiriman                                                                                        |1781         |
# |0          |indopub.unipinpro.topup                                                                           |1744         |
# |0          |com.my.cocpostit                                                                                  |1731         |
# |0          |toko.online.terpercaya                                                                            |1707         |
# |0          |com.aptivdev.remotecontroltoys                                                                    |1676         |
# |0          |com.tikshop.tikshop                                                                               |1662         |
# |0          |com.andromo.dev786963.app908281                                                                   |1658         |
# |0          |com.dhgate.buyermob                                                                               |1636         |
# |0          |com.appscentral.popitcase                                                                         |1613         |
# |0          |com.avito.android                                                                                 |1574         |
# |0          |com.zanyatocorp.checkbarcode                                                                      |1567         |
# |0          |com.xyz.alihelper                                                                                 |1567         |
# |0          |com.wKUBOIKumpulanBelanjaOnlineIndonesia_9684052                                                  |1514         |
# |0          |com.koinhiggsdomino.id                                                                            |1513         |
# |0          |com.onlineshoppingchina.chinashopping                                                             |1486         |
# |0          |com.trd.lapakproperti                                                                             |1471         |
# |0          |com.gumtree.android                                                                               |1452         |
# |0          |br.com.cea.appb2c                                                                                 |1397         |
# |0          |com.icaali.flashsale                                                                              |1344         |
# |0          |com.appnana.android.giftcardrewards                                                               |1295         |
# |0          |com.app.goga                                                                                      |1238         |
# |0          |com.wCodashopIndonesia_10208434                                                                   |1219         |
# |0          |com.slidejoy                                                                                      |1218         |
# |0          |com.shpock.android                                                                                |1208         |
# |0          |com.mmb.qtenoffers                                                                                |1182         |
# |0          |com.nsmobilehub                                                                                   |1171         |
# |0          |id.salestock.mobile                                                                               |1168         |
# |0          |nl.marktplaats.android                                                                            |1159         |
# |0          |online.market                                                                                     |1156         |
# |0          |com.mataharimall.mmandroid                                                                        |1132         |
# |0          |web.id.isipulsa.appkita                                                                           |1131         |
# |0          |com.chiphiggsdomino.app                                                                           |1123         |
# |0          |com.myFinderiauOnline.FinderiauOnline                                                             |1079         |
# |0          |com.itmobix.qataroffers                                                                           |1049         |
# |0          |com.cknb.hiddentagcop                                                                             |1038         |
# |0          |com.aptivdev.carstoys                                                                             |1024         |
# |0          |com.mi.global.shop                                                                                |1009         |
# |0          |com.browsershopping.forgrouponcoupons                                                             |1006         |
# |0          |com.shopee.co                                                                                     |961          |
# |0          |com.supercute.shoppinglist                                                                        |954          |
# |0          |com.ebates                                                                                        |953          |
# |0          |toko.online.bayarditempat                                                                         |938          |
# |0          |com.tourbillon.freeappsnow                                                                        |911          |
# |0          |com.bhanu.redeemerfree                                                                            |907          |
# |0          |com.cancctv.mobile                                                                                |904          |
# |0          |com.anaonlineshopping.kidstoys                                                                    |895          |
# |0          |com.ebay.kijiji.ca                                                                                |885          |
# |0          |com.adfone.aditup                                                                                 |877          |
# |0          |com.metro.foodbasics                                                                              |873          |
# |0          |sheproduction.coda.topup                                                                          |855          |
# |0          |com.girls.fashion.entertainment.School.Girls.Sale.Day.Shopping.Makeup.Adventure                   |852          |
# |0          |fr.leboncoin                                                                                      |839          |
# |0          |it.doveconviene.android                                                                           |828          |
# |0          |net.slickdeals.android                                                                            |827          |
# |0          |com.mygodrivi.godrivi                                                                             |801          |
# |0          |fr.deebee.calculsoldes                                                                            |790          |
# |0          |com.star5studio.Homedesigner.houseDecor                                                           |772          |
# |0          |com.myoryza.oryza                                                                                 |766          |
# |0          |fashion.style.clothes.cheap                                                                       |765          |
# |0          |com.wNasaStore_8607120                                                                            |765          |
# |0          |com.newandromo.dev535927.app981756                                                                |744          |
# |0          |com.shopee.mx                                                                                     |729          |
# |0          |com.bikinaplikasi.beliin.app                                                                      |727          |
# |0          |com.histore.indonesia                                                                             |724          |
# |0          |id.yoyo.popslide.app                                                                              |724          |
# |0          |tw.taoyuan.qrcode.reader.barcode.scanner.flashlight                                               |719          |
# |0          |com.contextlogic.wish                                                                             |716          |
# |0          |cam.gadanie.hiromant                                                                              |701          |
# |0          |in.amazon.mShop.android.shopping                                                                  |700          |
# |0          |networld.price.app                                                                                |699          |
# |0          |com.kittoboy.shoppingmemo                                                                         |694          |
# |0          |id.co.elevenia                                                                                    |693          |
# |0          |tw.fancyapp.qrcode.barcode.scanner.reader.isbn.flashlight                                         |689          |
# |0          |com.opo.shopping.lazada                                                                           |669          |
# |0          |com.nugros.arenatani                                                                              |661          |
# |0          |com.mrd.cekresipengiriman                                                                         |651          |
# |0          |com.malaysiashopping.sonu                                                                         |649          |
# |0          |com.sendo                                                                                         |648          |
# |0          |com.amazon.mShop.android.shopping                                                                 |630          |
# |0          |com.oriflame.catalogue.maroc.asha                                                                 |620          |
# |0          |ng.jiji.app                                                                                       |618          |
# |0          |com.indopay.unipin                                                                                |608          |
# |0          |net.agusharyanto.catatbelanja                                                                     |606          |
# |0          |com.online.smart.bracelet.watch.shop.oss                                                          |587          |
# |0          |com.toko.higgsdomino                                                                              |573          |
# |0          |com.wajual                                                                                        |571          |
# |0          |com.thirdrock.fivemiles                                                                           |570          |
# |0          |com.boulla.cellphone                                                                              |568          |
# |0          |com.sahibinden                                                                                    |565          |
# |0          |com.cannotbeundone.amazonbarcodescanner                                                           |565          |
# |0          |goosecreekco.android.app                                                                          |560          |
# |0          |com.overstock                                                                                     |559          |
# |0          |com.onlinecoupons.shopee                                                                          |549          |
# |0          |com.aptivdev.buykidstoys                                                                          |548          |
# |0          |com.tokopedia.kelontongapp                                                                        |547          |
# |0          |com.cupang.ads                                                                                    |546          |
# |0          |co.kr.ezapp.shoppinglist                                                                          |541          |
# |0          |com.appscentral.free_toys_online                                                                  |539          |
# |0          |com.allgoritm.youla                                                                               |534          |
# |0          |com.maulanatriharja.catatanbelanja                                                                |533          |
# |0          |com.crown.indonesia                                                                               |533          |
# |0          |com.wallapop                                                                                      |531          |
# |0          |com.appscentral.free_anti_stress                                                                  |531          |
# |0          |com.bitwize10.supersimpleshoppinglist                                                             |531          |
# |0          |co.shopney.lelopak                                                                                |530          |
# |0          |com.minhtinh.daftarbelanja                                                                        |529          |
# |0          |com.boulla.comparephones                                                                          |521          |
# |0          |com.pasti.terjual                                                                                 |520          |
# |0          |com.Spark.ESPpark                                                                                 |520          |
# |0          |com.jarfernandez.discountcalculator                                                               |517          |
# |0          |com.metalsoft.trackchecker_mobile                                                                 |517          |
# |0          |com.alfamart.alfagift                                                                             |507          |
# |0          |com.capigami.outofmilk                                                                            |505          |
# |0          |jual.belibarangbekas                                                                              |497          |
# |0          |com.OfficialAppsDev.CodanyaShopPro                                                                |493          |
# |0          |com.camera.translate.languages                                                                    |488          |
# |0          |com.whaff.whaffapp                                                                                |480          |
# |0          |com.kinoli.couponsherpa                                                                           |480          |
# |0          |ru.auto.ara                                                                                       |476          |
# |0          |com.pozitron.hepsiburada                                                                          |476          |
# |0          |com.ww1688ProductsAllLanguage_11962618                                                            |475          |
# |0          |com.georgeparky.thedroplist                                                                       |473          |
# |0          |shopping.express.sales.ali                                                                        |462          |
# |0          |com.opensooq.OpenSooq                                                                             |459          |
# |0          |id.com.android.javapay                                                                            |457          |
# |0          |com.vova.android                                                                                  |455          |
# |0          |tn.websol.adsblackfriday2                                                                         |452          |
# |0          |com.disqonin                                                                                      |450          |
# |0          |com.shopback.app                                                                                  |449          |
# |0          |online.malaysia.shopping                                                                          |437          |
# |0          |de.autodoc.gmbh                                                                                   |427          |
# |0          |com.wind.shopping.lazada                                                                          |421          |
# |0          |app.shopping.ali.express                                                                          |417          |
# |0          |com.dusdusan.katalog                                                                              |417          |
# |0          |com.mob91                                                                                         |416          |
# |0          |id.triliun.sobatpromo                                                                             |406          |
# |0          |com.onlineshoppingjapan.japanshopping                                                             |405          |
# |0          |com.itmobix.kwendeals                                                                             |402          |
# |0          |com.premiumappsfactory.popitbag                                                                   |398          |
# |0          |com.trkstudio.currentproducts                                                                     |397          |
# |0          |com.chinashoping.sonu                                                                             |396          |
# |0          |com.onlineshoppingsingapore.singaporeshopping                                                     |396          |
# |0          |com.myattausiltransaksional.attausiltransaksional                                                 |395          |
# |0          |paStudio.pharaoh.diamondpuzzlecall                                                                |394          |
# |0          |app.tsumuchan.frima_search                                                                        |394          |
# |0          |com.mangbelanjakeun                                                                               |390          |
# |0          |app.marketplace.mokamall                                                                          |388          |
# |0          |com.enuri.android                                                                                 |386          |
# |0          |com.Alltor.YoUnipin                                                                               |385          |
# |0          |com.online.shoppingchina                                                                          |384          |
# |0          |com.midasmind.app.mmgocarmm                                                                       |382          |
# |0          |com.myglamm.ecommerce                                                                             |382          |
# |0          |com.ae.ae                                                                                         |379          |
# |0          |com.ChinaShoppingOnline                                                                           |379          |
# |0          |com.geekeryapps.cartoys.remotecontrolcars                                                         |374          |
# |0          |com.rucksack.barcodescannerforwalmart                                                             |374          |
# |0          |com.ricosti.gazetka                                                                               |373          |
# |0          |com.appscentral.glowpopit                                                                         |369          |
# |0          |com.elz.secondhandstore                                                                           |365          |
# |0          |com.iassist.nurraysa                                                                              |353          |
# |0          |com.chainreactioncycles.hybrid                                                                    |353          |
# |0          |com.appmyshop                                                                                     |352          |
# |0          |com.applazada.promo                                                                               |352          |
# |0          |gsshop.mobile.v2                                                                                  |350          |
# |0          |com.glitterapps.plushie                                                                           |341          |
# |0          |com.rucksack.barcodescannerforebay                                                                |340          |
# |0          |com.mobile.cover.photo.editor.back.maker                                                          |339          |
# |0          |com.insightquest.snooper                                                                          |338          |
# |0          |best.deals.compare.coupons.discounts.tracker.assistant.alert                                      |336          |
# |0          |com.klink.kmart                                                                                   |333          |
# |0          |com.stGalileo.JewelCurse                                                                          |331          |
# |1          |id.co.shopintar                                                                                   |327          |
# |0          |com.ionicframework.sp262624                                                                       |326          |
# |0          |com.groupon                                                                                       |324          |
# |0          |tempat.jual.beli.hewan.peliharaan                                                                 |322          |
# |0          |com.pocky.ztime                                                                                   |322          |
# |0          |com.wBGShopee_9071949                                                                             |320          |
# |0          |com.myhiggsdominotips.higgsdominotips                                                             |320          |
# |0          |com.summerseger.katalogRabbaniHijabGamis                                                          |319          |
# |0          |com.myntra.android                                                                                |318          |
# |0          |com.schibsted.bomnegocio.androidApp                                                               |312          |
# |0          |com.nicekicks.rn.android                                                                          |310          |
# |0          |main.ClicFlyer                                                                                    |307          |
# |0          |com.tokopedia.sellerapp                                                                           |306          |
# |0          |com.olx.pk                                                                                        |301          |
# |0          |com.rmtheis.price.comparison.scanner                                                              |298          |
# |0          |com.olxmena.horizontal                                                                            |293          |
# |0          |com.flipkart.android                                                                              |293          |
# |0          |com.crocs.app                                                                                     |291          |
# |0          |com.xubnaha.florame                                                                               |288          |
# |0          |com.couponscodes.dealsdiscounts.forLazada                                                         |288          |
# |0          |com.orbstudio.daftartiktokshop                                                                    |288          |
# |0          |com.panagola.app.shopcalc                                                                         |285          |
# |0          |com.malaysiashopping.onlineshoppingmalaysiaapp                                                    |285          |
# |0          |com.idbuysell.fff                                                                                 |284          |
# |0          |app.oneprice                                                                                      |279          |
# |0          |com.appscentral.popit                                                                             |275          |
# |0          |id.kitabeli.mitra                                                                                 |271          |
# |0          |my.com.thefoodmerchant.app                                                                        |271          |
# |0          |zuper.market13                                                                                    |268          |
# |0          |com.appscentral.sd                                                                                |268          |
# |0          |com.ksl.android.classifieds                                                                       |267          |
# |0          |id.co.acehardware.acerewards                                                                      |266          |
# |0          |com.shell.sitibv.shellgoplusindia                                                                 |266          |
# |0          |com.onlineshoppingthailand.thailandshoppingapp                                                    |266          |
# |0          |com.meihillman.qrbarcodescanner                                                                   |266          |
# |0          |jp.jmty.app2                                                                                      |265          |
# |0          |de.super                                                                                          |262          |
# |0          |com.digital.shopping.apps.india.lite                                                              |258          |
# |0          |com.rucksack.pricecomparisonforamazonebay                                                         |254          |
# |0          |com.alfamart.katalog.flyers                                                                       |254          |
# |0          |com.appsfree.android                                                                              |254          |
# |0          |com.snapdeal.main                                                                                 |252          |
# |0          |nz.co.trademe.trademe                                                                             |251          |
# |0          |com.codified.hipyard                                                                              |249          |
# |0          |com.newandromo.dev535927.app1163168                                                               |247          |
# |0          |lk.ikman                                                                                          |244          |
# |0          |ru.ifsoft.mymarketplace                                                                           |243          |
# |0          |com.appscentral.squishy_shop                                                                      |242          |
# |0          |com.korchix.chineoapp                                                                             |242          |
# |0          |com.barcodelookup                                                                                 |239          |
# |0          |com.koindominoisland.app                                                                          |237          |
# |0          |com.newandromo.dev535927.app777705                                                                |235          |
# |0          |com.ozen.alisverislistesi                                                                         |235          |
# |0          |epasar.ecommerce.app                                                                              |230          |
# |0          |com.c51                                                                                           |227          |
# |0          |com.ibotta.android                                                                                |227          |
# |0          |com.univplay.appreward                                                                            |226          |
# |0          |id.com.android.nikimobile                                                                         |222          |
# |0          |com.yoshop                                                                                        |221          |
# |0          |com.radipgo.katalogdiskon                                                                         |218          |
# |0          |com.light.paidappssales                                                                           |218          |
# |0          |com.thekrazycouponlady.kcl                                                                        |216          |
# |0          |com.listonic.sales.deals.weekly.ads                                                               |215          |
# |0          |com.bikinaplikasi.bjcell                                                                          |214          |
# |0          |chinese.goods.online.cheap                                                                        |212          |
# |0          |com.toycar_rccars_shoppingonline.carstoys                                                         |212          |
# |0          |com.onlineshopping.malaysiashopping                                                               |212          |
# |0          |ua.slando                                                                                         |210          |
# |0          |com.tmon                                                                                          |210          |
# |0          |com.mobileshop.usa                                                                                |209          |
# |0          |com.douya.e_commercepart_time                                                                     |209          |
# |0          |ru.ozon.app.android                                                                               |207          |
# |0          |com.numatams.numatamsapps.discounttaxcalc                                                         |207          |
# |0          |br.com.lardev.android.rastreiocorreios                                                            |207          |
# |0          |com.b2w.americanas                                                                                |206          |
# |0          |com.vybesxapp                                                                                     |205          |
# |0          |com.biglotsshop.online                                                                            |205          |
# |0          |com.asyncbyte.wishlist                                                                            |205          |
# |0          |com.codylab.halfprice                                                                             |201          |
# |0          |com.anindyacode.cekharga                                                                          |199          |
# |0          |com.gazetki.gazetki                                                                               |199          |
# |0          |app1688.apyuw                                                                                     |197          |
# |0          |asia.bluepay.clientin                                                                             |197          |
# |0          |com.goodbarber.seniorfree                                                                         |195          |
# |0          |com.appscentral.free_squishy_shop                                                                 |194          |
# |0          |com.newandromo.dev535927.app836388                                                                |192          |
# |0          |com.oliks.G.til.freo                                                                              |192          |
# |0          |com.mycikalongwetanmarkets.cikalongwetanmarkets                                                   |191          |
# |0          |fashion.shop.clothes.dresses                                                                      |191          |
# |0          |com.bikinaplikasi.gadgetgrosir                                                                    |190          |
# |0          |com.roblox.robux.appreward.robux                                                                  |190          |
# |0          |prices.in.china                                                                                   |188          |
# |0          |com.brandicorp.brandi3                                                                            |187          |
# |0          |com.bunniimarketplace                                                                             |186          |
# |0          |com.tksolution.einkaufszettelmitspracheingabe                                                     |186          |
# |0          |com.roidtechnologies.appbrowzer                                                                   |186          |
# |0          |in.uknow.onlineshopping                                                                           |186          |
# |0          |de.roller.twa                                                                                     |185          |
# |0          |com.onlineshoppingphilippines.philippinesshopping                                                 |185          |
# |0          |com.icedblueberry.shoppinglisteasy                                                                |185          |
# |0          |com.teqnidev.paidappsfree.pro                                                                     |182          |
# |0          |com.dadidoo.catatanbelanja                                                                        |182          |
# |0          |com.shopping.online.id                                                                            |181          |
# |0          |ru.grocerylist.android                                                                            |181          |
# |0          |com.moramsoft.ppomppualarm                                                                        |180          |
# |0          |com.militarycompany.helicoptermod                                                                 |180          |
# |0          |com.costco.app.android                                                                            |178          |
# |0          |com.my.codaccounts                                                                                |177          |
# |0          |com.titansModforMcpe.AttackonTitanmod                                                             |171          |
# |0          |com.sinworldnlineshop.shop                                                                        |171          |
# |0          |com.shipt.groceries                                                                               |170          |
# |0          |com.biglotsapp.shop                                                                               |168          |
# |0          |com.anaonlineshopping.bra_womenunderwear                                                          |168          |
# |0          |com.bikinaplikasi.yandhika                                                                        |164          |
# |0          |com.glitterapps.popitcase                                                                         |164          |
# |0          |com.navigationproap.drivingvalleyrouteap.navigationrouteap                                        |164          |
# |0          |com.andromo.dev630323.app739765                                                                   |164          |
# |0          |com.froogloid.kring.google.zxing.client.android                                                   |163          |
# |0          |com.geekonweb.MalaysiaShoppingApp                                                                 |163          |
# |0          |com.wToyStoryToys_12242673                                                                        |162          |
# |0          |com.glitterapps.stress                                                                            |161          |
# |0          |barcode.qr.scanner                                                                                |161          |
# |0          |com.mymofi.mofi                                                                                   |159          |
# |0          |appinventor.ai_lightworld813.bodysize                                                             |157          |
# |0          |com.promocodes.couponsforlazada                                                                   |156          |
# |0          |com.amazon.aa                                                                                     |156          |
# |0          |com.rmystudio.budlist                                                                             |156          |
# |0          |com.newandromo.dev535927.app834897                                                                |156          |
# |0          |com.mattceonzo.ultimatewishlist                                                                   |155          |
# |0          |com.JOS.Japan.Online.Shopping                                                                     |152          |
# |0          |com.andromo.dev276588.app308783                                                                   |152          |
# |0          |club.fromfactory                                                                                  |152          |
# |0          |com.snapcart.android                                                                              |152          |
# |0          |com.glitterapps.popitfree                                                                         |151          |
# |0          |it.subito                                                                                         |150          |
# |0          |com.prolight.prolight                                                                             |149          |
# |0          |com.memiles.app                                                                                   |147          |
# |0          |com.agusharyanto.net.discountcalculator                                                           |147          |
# |0          |com.shopee.in                                                                                     |147          |
# |0          |com.bikroy                                                                                        |147          |
# |0          |com.inovasiteknologi.a101.onglaistore                                                             |146          |
# |0          |com.merchant.histore                                                                              |145          |
# |0          |com.kolaku.android                                                                                |145          |
# |0          |com.glitterapps.simple                                                                            |144          |
# |0          |com.myshopedia.shopediamarket                                                                     |143          |
# |0          |com.rusbuket                                                                                      |143          |
# |0          |com.supercute.rumahmurah                                                                          |142          |
# |0          |jp.co.unisys.android.yamadamobile                                                                 |137          |
# |0          |usbobble.ssgame.fruitMatchRealizeDream                                                            |137          |
# |0          |com.g2018.hfcoupons                                                                               |137          |
# |0          |com.shopclues                                                                                     |137          |
# |0          |com.infohargamobil                                                                                |135          |
# |0          |com.mhn.ponta                                                                                     |134          |
# |0          |com.visionsmarts.pic2shop                                                                         |134          |
# |0          |com.barcodescanner.qrreader                                                                       |133          |
# |0          |benefitmedia.android.sparpionier                                                                  |132          |
# |0          |com.sayuran.online                                                                                |132          |
# |0          |com.sonyericsson.pontofrio                                                                        |131          |
# |0          |com.ZAM.AgenKosmetikMurahIndonesia                                                                |131          |
# |0          |com.pricecheckscanner                                                                             |131          |
# |0          |kr.co.dreamshopping.mcapp                                                                         |131          |
# |0          |uk.vinted                                                                                         |130          |
# |0          |com.sugar_lipstick_makeup_set_makeup_brushes_sale.cosmetics_makeup_face_powder_compact_powder_sale|130          |
# |0          |com.zeptoconsumerapp                                                                              |129          |
# |0          |womens.cheap.clothes                                                                              |129          |
# |0          |com.taobao.htao.android                                                                           |129          |
# |0          |com.toko.online.net                                                                               |126          |
# |0          |com.buymeapie.bmap                                                                                |126          |
# |0          |com.fgf.shopping.game                                                                             |126          |
# |0          |com.wLapakLaris_13337038                                                                          |123          |
# |0          |com.glitterapps.fidget                                                                            |121          |
# |0          |com.rexsolution.golfreshmarket                                                                    |120          |
# |0          |one.track.app                                                                                     |119          |
# |0          |com.stylish.font.free                                                                             |118          |
# |0          |com.barcode.scanner.qr.scanner.app.qr.code.reader                                                 |118          |
# |0          |com.komorebi.shoppinglist                                                                         |118          |
# |0          |kids.onlineshopping                                                                               |116          |
# |0          |com.wkendarijualbeli_9915700                                                                      |116          |
# |0          |com.indonesiashopping.sonu                                                                        |115          |
# |0          |com.google.zxing.client.android                                                                   |115          |
# |0          |air.tMinis                                                                                        |114          |
# |0          |ba.pik.v2.android                                                                                 |113          |
# |0          |mobi.mobileforce.informa                                                                          |112          |
# |0          |com.gsretail.android.smapp                                                                        |112          |
# |0          |com.alimede.easyshopping                                                                          |111          |
# |0          |com.GomoGameDev.TopUpdanCaraBayarCodashop                                                         |111          |
# |0          |com.withsellit.app.sellit                                                                         |111          |
# |0          |com.onlineshoppingkorea.koreashopping                                                             |110          |
# |1          |com.app.tokobagus.betterb                                                                         |110          |
# |0          |jp.co.fablic.fril                                                                                 |110          |
# |0          |com.star5studio.HouseDecorationPlan.diamondHSLegend                                               |109          |
# |0          |com.handyapps.discountcalc                                                                        |108          |
# |0          |pl.tablica                                                                                        |108          |
# |0          |com.wKSMForMember_13916114                                                                        |106          |
# |0          |com.situs.kuliner                                                                                 |106          |
# |0          |com.wSIPLAHKamdikbud_10759627                                                                     |106          |
# |0          |com.ionicframework.kalkulatorbelanja439506                                                        |106          |
# |0          |shopping.mobile.com.products                                                                      |104          |
# |0          |com.mourjan.classifieds                                                                           |104          |
# |0          |com.selloship.thedopeshop                                                                         |103          |
# |0          |com.mybandungbarat.bandungbarat                                                                   |103          |
# |0          |com.banggood.client                                                                               |103          |
# |0          |com.wESGOKAJEN_12352637                                                                           |103          |
# |0          |com.myway.omr4                                                                                    |102          |
# |0          |com.myhighdominojackpotberuntung.highdominojackpotberuntung                                       |102          |
# |0          |com.chotot.vn                                                                                     |101          |
# |0          |com.headcode.ourgroceries                                                                         |100          |
# |0          |com.bf.app0def94                                                                                  |99           |
# |0          |com.andromo.dev707753.app828918                                                                   |99           |
# |0          |philippines.shopping.online.app                                                                   |99           |
# |0          |com.rodsapps.poptoshop.app                                                                        |98           |
# |0          |com.pinteng.lovelywholesale                                                                       |98           |
# |0          |com.airjordandeals.android                                                                        |98           |
# |0          |online.thailand.shopping                                                                          |98           |
# |0          |com.toreast.app                                                                                   |97           |
# |0          |com.manash.purplle                                                                                |96           |
# |0          |online.shoppingmarketplace                                                                        |95           |
# |0          |com.appnew.ubery2019orderinginstructions                                                          |94           |
# |0          |com.B2B.Marketplace.PasarDigital.UMKMBUMN                                                         |94           |
# |0          |com.swiftspeedappcreator.android5ec91dd9d0622                                                     |94           |
# |0          |com.wTopkoin_12421106                                                                             |93           |
# |0          |com.mmstore.apps                                                                                  |92           |
# |0          |com.indomaret.klikindomaret                                                                       |92           |
# |0          |com.summerseger.katalogZoyaHijabGamis                                                             |91           |
# |0          |shopping.list.free.lista.compra.gratis.liston                                                     |91           |
# |0          |com.tribunnews.tribunjualbeli                                                                     |90           |
# |0          |com.crispysoft.deliverycheck                                                                      |89           |
# |0          |com.trd.lapakmobkas                                                                               |89           |
# |0          |com.stickman.spider.rope.hero.gangstar.city                                                       |89           |
# |0          |com.lelong.buyer                                                                                  |89           |
# |0          |com.dolap.android                                                                                 |89           |
# |0          |mypoin.indomaret.android                                                                          |88           |
# |0          |com.ionicframework.avsi204446                                                                     |88           |
# |0          |com.tokohiggs.domino                                                                              |88           |
# |0          |com.ZAM.AgenKosmetikTermurahJakarta                                                               |88           |
# |0          |com.toys_kids_cheap.remotecontroltoys                                                             |87           |
# |0          |f2game.gemsRoad.pirateDiamondPuzzle                                                               |87           |
# |0          |com.jasonmg.simsale                                                                               |87           |
# |0          |com.ballbek.flying.shopping.mall.driver                                                           |87           |
# |0          |com.allinonestudio.chinedoapp                                                                     |86           |
# |0          |com.glitterapps.glow                                                                              |86           |
# |0          |com.zzkko                                                                                         |86           |
# |0          |com.summerseger.daftarhargaayam                                                                   |85           |
# |0          |com.mobil123.www                                                                                  |84           |
# |0          |test.aplivjl                                                                                      |84           |
# |0          |com.coupang.mobile                                                                                |84           |
# |0          |com.noel.shop                                                                                     |83           |
# |0          |com.luck_d                                                                                        |83           |
# |0          |ru.zenden.android                                                                                 |83           |
# |0          |com.umjjal.gif.maker                                                                              |82           |
# |0          |app.brickseek.com.brickseekapp                                                                    |82           |
# |0          |com.chestersw.foodlist                                                                            |81           |
# |0          |cardgame.pokerasia.fourofakind.capsa.susun                                                        |80           |
# |0          |com.asemediatech.resiongkir                                                                       |80           |
# |0          |web.id.isipulsa.data                                                                              |80           |
# |0          |com.intersport.app.de                                                                             |78           |
# |0          |com.hl.media.barcode.reader.free                                                                  |78           |
# |0          |com.moneydolly                                                                                    |78           |
# |0          |com.quikr                                                                                         |77           |
# |0          |com.real.wallpaper                                                                                |77           |
# |0          |com.ryananggowo.kumpulantokoonlineindonesiadalamsatuaplikasi                                      |77           |
# |0          |com.treasurelistings.gsalr                                                                        |77           |
# |0          |de.gavitec.android                                                                                |77           |
# |0          |com.ayopop                                                                                        |77           |
# |0          |kr.defind.shoepik.users                                                                           |76           |
# |0          |com.blanja.apps.android                                                                           |76           |
# |0          |com.mobincube.hk_coupons.sc_E5UB92                                                                |75           |
# |0          |com.onlineuae.shopping                                                                            |75           |
# |0          |com.holashp.purchasebate                                                                          |75           |
# |0          |com.clytie.app                                                                                    |75           |
# |0          |app.ndtv.matahari                                                                                 |75           |
# |0          |com.big.shopapp                                                                                   |74           |
# |0          |com.izzedam.weedvva                                                                               |74           |
# |0          |com.myfkpntutorial.fkpntutorial                                                                   |74           |
# |0          |com.gloriajeans.mobile                                                                            |73           |
# |0          |com.elevenst                                                                                      |73           |
# |0          |kr.co.camtalk                                                                                     |73           |
# |0          |id.jasamitra.app                                                                                  |73           |
# |0          |nl.onlineretailservice.reclamefolderandroid                                                       |73           |
# |0          |com.levistrauss.customer                                                                          |73           |
# |0          |com.myBaubauJB.BaubauJB                                                                           |72           |
# |0          |com.hudsonsbay.android                                                                            |72           |
# |0          |com.online.smart.bracelet.watch.shopping.oss                                                      |71           |
# |0          |com.zilingo.users                                                                                 |71           |
# |0          |com.newandromo.dev535927.app804717                                                                |71           |
# |0          |id.toco                                                                                           |71           |
# |0          |best.home.compare.coupons.discounts.tracker.assistant.alert                                       |71           |
# |0          |thecouponsapp.coupon                                                                              |70           |
# |0          |my.com.nineyi.shop.s200074                                                                        |70           |
# |0          |com.appybuilder.dhirajanand2009.OnlineToysShop                                                    |70           |
# |0          |cdiscount.mobile                                                                                  |70           |
# |0          |com.smokeeffect.nameart                                                                           |70           |
# |0          |com.fixeads.coisas                                                                                |70           |
# |0          |com.mygshp.gshp                                                                                   |70           |
# |0          |com.simple_memo_pad_notes_app.babylon                                                             |69           |
# |0          |com.FurnitureOnlineShopping                                                                       |69           |
# |0          |com.shopkick.app                                                                                  |69           |
# |0          |com.Konoha.NarutoShippundenMod                                                                    |68           |
# |0          |app.anakoslab.cekmurah                                                                            |68           |
# |0          |com.fivejack.itemku                                                                               |68           |
# |0          |cimasv.appsaptod.guideforappmarketaptoide.tiosd                                                   |68           |
# |0          |com.premiumappsfactory.drone                                                                      |67           |
# |0          |com.lyst.lystapp                                                                                  |67           |
# |0          |com.onlineshoppingvietnam.vietnamshopping                                                         |66           |
# |0          |best.apparel.compare.coupons.discounts.tracker.assistant.alert                                    |66           |
# |0          |com.indopub.codapayment                                                                           |66           |
# |0          |net.giosis.shopping.jp                                                                            |66           |
# |0          |com.bras.panty.nightwere.shop                                                                     |66           |
# |0          |com.uniqlo.id.catalogue                                                                           |66           |
# |0          |com.carsome.customer                                                                              |66           |
# |0          |com.topshop.shopping                                                                              |65           |
# |0          |com.promomobil.promomobil                                                                         |65           |
# |0          |com.ClothingShoppingOnline                                                                        |65           |
# |0          |com.septiancell.site                                                                              |64           |
# |0          |com.mfar.flyerify                                                                                 |64           |
# |0          |com.ranjith888999.onlinefurniturestore                                                            |64           |
# |0          |com.bikinaplikasi.cantikshop24                                                                    |64           |
# |0          |com.fineappstudio.android.petfriends                                                              |63           |
# |0          |com.wAntaraMotor_8841334                                                                          |63           |
# |0          |com.onlinecoupons.bathandbody                                                                     |63           |
# |0          |com.bestappsale                                                                                   |63           |
# |0          |com.socialapps.homeplus                                                                           |63           |
# |0          |com.mcraftmaps.parkour                                                                            |63           |
# |0          |com.priceminister.buyerapp                                                                        |63           |
# |0          |javasign.com.cekrekening                                                                          |63           |
# |0          |com.myayudidesign.ayudidesign                                                                     |62           |
# |0          |genius.trade2                                                                                     |62           |
# |0          |com.realmestore.app                                                                               |62           |
# |0          |com.indoarjuna.gudangtasgrosir                                                                    |61           |
# |0          |us.tokoonline.gratisongkir.bayarditempat                                                          |61           |
# |0          |at.willhaben                                                                                      |61           |
# |0          |ru.puma.android                                                                                   |61           |
# |0          |com.ria.auto                                                                                      |61           |
# |0          |web.id.isipulsa.app                                                                               |61           |
# |0          |com.thailand.onlineshopping                                                                       |61           |
# |0          |com.bikinaplikasi.brosayur                                                                        |60           |
# |0          |com.bikinaplikasi.rsfashiongrosir                                                                 |60           |
# |0          |com.mobappsbaker.kwaroffers                                                                       |59           |
# |0          |com.onlineksa.shopping                                                                            |59           |
# |0          |com.zoltopshopla.shop                                                                             |59           |
# |0          |com.rkskdldl.smartstore_forseller                                                                 |58           |
# |0          |right.apps.gleamaways                                                                             |58           |
# |0          |com.faunesia.apk                                                                                  |58           |
# |0          |com.airjordanoutlet.android                                                                       |58           |
# |0          |co.fusionweb.blackfriday                                                                          |58           |
# |0          |com.glitterapps.near                                                                              |58           |
# |0          |shopping.app.offer.deals.us.shopping                                                              |58           |
# |0          |com.mymamabelanjaindonesia.mamabelanjaindonesia                                                   |57           |
# |0          |com.summerseger.katalogerhadaftarharga                                                            |57           |
# |0          |com.ebay.kr.gmarket                                                                               |57           |
# |0          |com.indonesia_shopping.zakir                                                                      |57           |
# |0          |tukutuku.sn                                                                                       |56           |
# |0          |com.wemakeprice                                                                                   |56           |
# |0          |com.ingonan.store                                                                                 |56           |
# |0          |alejo.converse                                                                                    |56           |
# |0          |com.wSARAVANATRADERS_9366627                                                                      |55           |
# |0          |com.amazon_online_mobile_shopping.flipkart_mobile_sale                                            |55           |
# |0          |com.jumia.android                                                                                 |55           |
# |0          |web.id.isipulsa.agenkuota                                                                         |55           |
# |0          |be.tweedehands.m                                                                                  |55           |
# |0          |com.mymindoapplication.mindoapplication                                                           |55           |
# |0          |com.jockey_women_underwear_clovia_bra.online_jockey_sexy_nighties_hot_bra_lingeries               |55           |
# |0          |gr.playstore.studio.android5b8fc185820c2                                                          |54           |
# |0          |com.wCodashopIndonesia_10155135                                                                   |54           |
# |0          |com.taobao.taobao                                                                                 |53           |
# |0          |com.newandromo.dev535927.app1241888                                                               |53           |
# |0          |com.onlineshopping.thailandshopping                                                               |53           |
# |0          |com.souq.app                                                                                      |53           |
# |0          |com.droneflight_shoppingonline.drone                                                              |52           |
# |0          |id.serpul.sp3576                                                                                  |52           |
# |0          |com.yanedev.browserforlazadacoupons                                                               |51           |
# |0          |com.bengkeldroid.sophie                                                                           |51           |
# |0          |com.hartono.elektronika                                                                           |51           |
# |0          |com.olx.olx                                                                                       |51           |
# |0          |za.co.meeser.inglenook                                                                            |51           |
# |0          |com.Spark.Skillz                                                                                  |50           |
# |0          |pl.vinted                                                                                         |50           |
# |0          |com.anaonlineshopping.babytoys                                                                    |50           |
# |0          |com.iqouz.gameo                                                                                   |50           |
# |0          |kz.slando                                                                                         |50           |
# |0          |bra.shoppingapp                                                                                   |50           |
# |0          |com.oriflame.oriflame                                                                             |49           |
# |0          |com.insurance.guide2019                                                                           |49           |
# |0          |codashop.apubp                                                                                    |49           |
# |0          |com.japanshopping.sonu                                                                            |49           |
# |0          |com.mygarutfood.garutfood                                                                         |49           |
# |0          |com.wsayurQu_10842373                                                                             |48           |
# |0          |com.sayurbox                                                                                      |48           |
# |0          |indonesia.id.indonesiaonlineshops                                                                 |48           |
# |0          |com.chloesoft.pbapp                                                                               |48           |
# |0          |com.anaonlineshopping.makeupshopping                                                              |48           |
# |0          |com.wBluetoothSpeaker_7669770                                                                     |48           |
# |0          |com.appstrdesign.buysell                                                                          |48           |
# |0          |com.olshop.cekharga                                                                               |48           |
# |0          |app.goubba.com                                                                                    |48           |
# |0          |com.endless.myshoppinglist                                                                        |47           |
# |0          |se.scmv.belarus                                                                                   |47           |
# |0          |com.MCPEMOD.MilitarymodforMCPE                                                                    |47           |
# |0          |shopping.list.grocery.recipes.coupons                                                             |47           |
# |0          |bima101sok.java                                                                                   |46           |
# |0          |com.carstoys.onlineshopping_toycar                                                                |46           |
# |0          |sales.cashback.shopping.master                                                                    |46           |
# |0          |ch.anibis.anibis                                                                                  |46           |
# |0          |indopub.topup.upoint                                                                              |46           |
# |0          |com.dualgram.multiple.record.camera.guide                                                         |46           |
# |0          |com.pisgo.oneplusone                                                                              |46           |
# |0          |com.mm.views                                                                                      |46           |
# |0          |com.ONLINESHOPINDONESIA_8626456                                                                   |45           |
# |0          |com.glitterapps.menunderwearstore                                                                 |45           |
# |0          |com.itmobix.ksaendeals                                                                            |45           |
# |0          |com.coruscate.organichome                                                                         |45           |
# |0          |de.kleiderkreisel                                                                                 |45           |
# |0          |com.qxl.Client                                                                                    |45           |
# |0          |com.ujudebug.organicbazaar                                                                        |44           |
# |0          |asia.grosirpakaian.onlineshop                                                                     |44           |
# |0          |show.grip                                                                                         |44           |
# |0          |com.DramaProductions.Einkaufen5                                                                   |44           |
# |0          |com.club.dx.gb.luckygift                                                                          |44           |
# |0          |com.newandromo.dev969116.app1191840                                                               |44           |
# |0          |com.apps.globalmobileprices.mobile                                                                |44           |
# |0          |com.jafolders.allefolders                                                                         |43           |
# |0          |com.premiumappsfactory.lust                                                                       |43           |
# |0          |com.machtwatch_mobile                                                                             |43           |
# |0          |com.wBarangRongsok_12719277                                                                       |43           |
# |0          |com.wWAYGO_13944726                                                                               |43           |
# |0          |asia.acommerce.adidasid                                                                           |43           |
# |0          |io.oskm.hotdeals                                                                                  |43           |
# |0          |ch.tutti                                                                                          |43           |
# |0          |com.apprancher.slime                                                                              |43           |
# |0          |com.jaknot                                                                                        |42           |
# |0          |code.expert.lulumarketpromotions                                                                  |42           |
# |0          |com.lymin.codashopfree                                                                            |42           |
# |0          |net.furimawatch.fmw                                                                               |42           |
# |0          |com.wSHOP101APPONLINESHOPPINGINDIA_9585106                                                        |42           |
# |0          |com.ashby.everykart                                                                               |41           |
# |0          |com.muba.anuncios                                                                                 |41           |
# |0          |com.kakaku.kakakucomapp                                                                           |41           |
# |0          |com.jasonmg.market09                                                                              |40           |
# |0          |com.danimaster.reviews.hot.deals                                                                  |40           |
# |0          |com.ZAM.AGENKOSMETIKSURABAYA                                                                      |40           |
# |0          |com.zaful                                                                                         |40           |
# |0          |com.brashoppingapp                                                                                |40           |
# |0          |com.nttdocomo.android.dpoint                                                                      |40           |
# |0          |com.poolballcoins                                                                                 |40           |
# |0          |com.aptivdev.bra                                                                                  |39           |
# |0          |com.mygacek.gacek                                                                                 |39           |
# |0          |com.maxxum.server.distributoraccessories2                                                         |39           |
# |0          |id.agenkosmetik.store                                                                             |39           |
# |0          |com.mobappsbaker.uaeoffers                                                                        |39           |
# |0          |web.id.isipulsa.appku                                                                             |38           |
# |0          |com.tcm.supertreasure                                                                             |38           |
# |0          |com.anaonlineshopping.weddingdress                                                                |38           |
# |0          |com.app.dealfish.main                                                                             |38           |
# |0          |com.bikinaplikasi.wosupplier                                                                      |38           |
# |0          |io.makeroid.bsdeora55520.usa                                                                      |38           |
# |0          |com.shop.uzumarket                                                                                |38           |
# |0          |com.powergirdcar.egyptWitch.TreasureHunter                                                        |37           |
# |0          |com.lottemart.shopping                                                                            |37           |
# |0          |com.erafone.bct.lk6                                                                               |37           |
# |0          |com.nogamelabs.parcel.tracker                                                                     |37           |
# |0          |com.alfacart.apps                                                                                 |37           |
# |0          |company.wishupon                                                                                  |37           |
# |0          |com.androidapp.mblif                                                                              |37           |
# |0          |com.checkpoints.app                                                                               |37           |
# |0          |com.shooperapp                                                                                    |37           |
# |0          |com.razerzone.cortex.dealsv2                                                                      |37           |
# |0          |online.thailand.shopping.app                                                                      |37           |
# |0          |jewelry.bijouterie.cheap                                                                          |37           |
# |0          |com.tise.tise                                                                                     |37           |
# |0          |com.caracaracaramudah.caracarakredithponlinetanpakartukredit                                      |36           |
# |0          |com.manggaduajakarta.onlineshop                                                                   |36           |
# |0          |com.jasonmg.salepoison                                                                            |36           |
# |0          |com.niapp.cekresiallinone                                                                         |36           |
# |0          |com.shoesshoppingapp                                                                              |36           |
# |0          |com.kiwi.customer                                                                                 |35           |
# |0          |br.com.dafiti                                                                                     |35           |
# |0          |jp.id_credit_sp.android                                                                           |35           |
# |0          |com.mor.ntnu.appstore                                                                             |35           |
# |0          |com.studiodevs.womensintimates_bra                                                                |34           |
# |0          |com.wTrackingAliexpress_12686734                                                                  |34           |
# |0          |com.abmo.diamonshop                                                                               |34           |
# |0          |com.wUMKMOKE_13060134                                                                             |34           |
# |0          |com.watsons.id.android                                                                            |34           |
# |0          |com.galaxyfirsatlari                                                                              |34           |
# |0          |com.forsale.forsale                                                                               |34           |
# |0          |com.flipkart.shopsy                                                                               |34           |
# |0          |com.wCuelinksAffiliate_12013676                                                                   |34           |
# |0          |com.cupshe.cupshe                                                                                 |34           |
# |0          |ru.sunlight.sunlight                                                                              |34           |
# |0          |com.diamond.freefireindo                                                                          |34           |
# |0          |com.allshopping.aio                                                                               |34           |
# |0          |com.wBeiShopping_8806468                                                                          |34           |
# |0          |com.ZAM.AgenJilbabMurah                                                                           |34           |
# |0          |com.fly.chat.messenger                                                                            |33           |
# |0          |com.ff.diamond.zato                                                                               |33           |
# |0          |com.onlineshoppingksa.saudiarabiashopping                                                         |33           |
# |0          |com.woodoo                                                                                        |33           |
# |0          |kgkgkg.hong.hscanner                                                                              |33           |
# |0          |com.tippingcanoe.promodescuentos                                                                  |32           |
# |0          |com.smarttech.catalogs                                                                            |32           |
# |0          |com.devbigl.apkproj                                                                               |32           |
# |0          |com.glitterapps.toy                                                                               |32           |
# |0          |fr.snapp.fidme                                                                                    |31           |
# |0          |solusi.mediapanel                                                                                 |31           |
# |0          |com.anaonlineshopping.beautystore                                                                 |31           |
# |0          |com.onlineshopping.IndonesiaShopping                                                              |31           |
# |0          |com.thailandshopping.sonu                                                                         |31           |
# |0          |com.gameskharido.co.in                                                                            |31           |
# |0          |com.arata1972.yoshinoya                                                                           |31           |
# |0          |com.playstation.com                                                                               |31           |
# |0          |com.bradsdeals                                                                                    |30           |
# |0          |com.poppyplaytimetips.mommylong.legsberuntung                                                     |30           |
# |0          |singapore.shopping.app                                                                            |30           |
# |0          |jb.hifi2                                                                                          |29           |
# |0          |com.bikinaplikasi.kusone                                                                          |29           |
# |0          |us.tokoonline.alatpancing                                                                         |29           |
# |0          |com.wFAUZANSHOP_13513459                                                                          |29           |
# |0          |com.higgsdominostore.apps                                                                         |29           |
# |0          |com.wAppleStore_13962473                                                                          |29           |
# |0          |com.studioapps.chinashoping_onlineshopping                                                        |29           |
# |0          |com.bird.kaichon                                                                                  |29           |
# |0          |net.bbnapp.shopping.letgo                                                                         |28           |
# |0          |com.kt.accessory                                                                                  |28           |
# |0          |com.viphashtags.app                                                                               |28           |
# |0          |id.co.ikea.android                                                                                |28           |
# |0          |ru.cardsmobile.mw3                                                                                |28           |
# |0          |com.mobile.pomelo                                                                                 |28           |
# |0          |com.htechnology.heriyanto_sirait.kedeid                                                           |28           |
# |0          |com.gumtree.android.beta                                                                          |28           |
# |0          |com.twelvestoreez                                                                                 |27           |
# |0          |com.online.shoppingjapan                                                                          |27           |
# |0          |com.aio.allinone.onlineshopping                                                                   |27           |
# |0          |com.menshoesshoppingapps                                                                          |27           |
# |0          |com.xmaslist                                                                                      |27           |
# |0          |com.letyshops                                                                                     |27           |
# |0          |com.phone.shop.app                                                                                |27           |
# |0          |com.shoppinglist                                                                                  |27           |
# |0          |yqtrack.app                                                                                       |27           |
# |0          |com.kame3.apps.calcforshopping                                                                    |27           |
# |0          |com.wRKONLYBOZ_9243961                                                                            |27           |
# |0          |com.buyfansonline.fansandcoolersonline                                                            |27           |
# |0          |oskm.io.eventopia                                                                                 |26           |
# |0          |com.shoesdeals.app                                                                                |26           |
# |0          |com.nlsavenger.royalDiamond.homeRenovateHouseDecorate                                             |26           |
# |0          |com.glitterapps.phonecaseshop                                                                     |26           |
# |0          |com.on9store.shopping.tokoonlinepromo                                                             |26           |
# |0          |com.glitterapps.drones                                                                            |26           |
# |0          |com.linonlineshopping.remotecontrolcar                                                            |26           |
# |0          |sorisoft.co.kr.convgs                                                                             |26           |
# |0          |com.mylapaktrans.lapaktrans                                                                       |26           |
# |0          |com.appoler.nisnokeed                                                                             |26           |
# |0          |ssense.android.prod                                                                               |26           |
# |0          |cl.yapo                                                                                           |26           |
# |0          |com.coolapps.miband.hulyaapp                                                                      |26           |
# |0          |com.vietnam.onlineshopping                                                                        |26           |
# |0          |com.si.simembers                                                                                  |25           |
# |0          |com.ril.ajio                                                                                      |25           |
# |0          |id.compro.kartikaaccessories                                                                      |25           |
# |0          |br.com.encomendas                                                                                 |25           |
# |0          |com.sharpninja.ninja2                                                                             |25           |
# |0          |online.marketplace.ae                                                                             |25           |
# |0          |com.mobeasyapp.app414331889627                                                                    |25           |
# |0          |kr.co.lgfashion.lgfashionshop.v28                                                                 |25           |
# |0          |ro.mercador                                                                                       |25           |
# |0          |com.vatsap.whats.fm                                                                               |25           |
# |0          |com.mdkuriruser.pelanggan                                                                         |25           |
# |0          |com.whitelabel.putravoucher                                                                       |25           |
# |0          |the.semicolon.geantmarketpromotions                                                               |25           |
# |0          |pk.com.whatmobile.whatmobile                                                                      |25           |
# |0          |id.esoft.bajojo                                                                                   |25           |
# |0          |com.wKidsShop_12842603                                                                            |24           |
# |0          |shopping.app14                                                                                    |24           |
# |0          |com.elevenst.deals                                                                                |24           |
# |0          |com.styleshare.android                                                                            |24           |
# |0          |com.glitterapps.womenunderwearonlinestore                                                         |24           |
# |0          |com.msint.myshopping.list                                                                         |24           |
# |0          |com.fsn.nykaa                                                                                     |24           |
# |0          |com.wMultiKreditBandung_11656246                                                                  |24           |
# |0          |com.sdgcode.discountcalculator                                                                    |24           |
# |0          |com.empg.olxom                                                                                    |24           |
# |0          |com.noon.buyerapp                                                                                 |24           |
# |0          |com.rdev.rajapediadigital                                                                         |24           |
# |0          |com.luizalabs.mlapp                                                                               |23           |
# |0          |com.dai.giftcard.viewer                                                                           |23           |
# |0          |com.tul.tatacliq                                                                                  |23           |
# |0          |com.goodbarber.starbucksmenu                                                                      |23           |
# |0          |com.jakmall                                                                                       |23           |
# |0          |vietnam.shopping.online.app                                                                       |23           |
# |0          |com.optikmelawai.optikmelawai                                                                     |23           |
# |0          |de.quoka.kleinanzeigen                                                                            |23           |
# |0          |by.uniq.package_tracker                                                                           |23           |
# |0          |com.borneosell.kalimantan                                                                         |23           |
# |0          |com.toptoshirou.toplandarea                                                                       |23           |
# |0          |com.fivefly.android.shoppinglist                                                                  |23           |
# |0          |dk.dba.android                                                                                    |23           |
# |0          |com.eastpal.android.mnuri                                                                         |23           |
# |0          |com.gramedia.retail                                                                               |23           |
# |0          |com.empg.olxsa                                                                                    |23           |
# |0          |com.samsung.ecomm.global.in                                                                       |23           |
# |0          |com.kaskus.fjb                                                                                    |23           |
# |0          |com.bigsaving.shopeesale                                                                          |23           |
# |0          |com.samsungmall                                                                                   |23           |
# |0          |com.tradera                                                                                       |23           |
# |0          |com.winds.dcfeverreader                                                                           |23           |
# |0          |us.toko.bagus                                                                                     |22           |
# |0          |jewel.match.bmc                                                                                   |22           |
# |0          |tool.qrbarcodemaker.qrbarcodeScanner                                                              |22           |
# |0          |com.katalogpromoupdate.katalogpromoupdate                                                         |22           |
# |0          |com.kioser.app                                                                                    |22           |
# |0          |com.mandarin.android                                                                              |22           |
# |0          |com.anaonlineshopping.babyclothes_shoppingapps                                                    |22           |
# |0          |com.anaonlineshopping.womenjeans                                                                  |22           |
# |0          |com.grape.software.offcoupon                                                                      |22           |
# |0          |com.hongkong.onlineshopping                                                                       |22           |
# |0          |com.bigbasket.mobileapp                                                                           |22           |
# |0          |com.dapurmancing.android                                                                          |22           |
# |0          |fragrancebuy.android.app                                                                          |22           |
# |0          |com.ZAM.SupplierKosmetikKorea                                                                     |22           |
# |0          |com.anaonlineshopping.mensjeans                                                                   |22           |
# |0          |com.oblivion.tokoonline                                                                           |22           |
# |0          |com.xunmeng.pinduoduo                                                                             |22           |
# |0          |redeemapp.card                                                                                    |22           |
# |0          |org.tokoonlineindonesia.apps.tokoonlinebersama                                                    |22           |
# |0          |tv.jamlive                                                                                        |22           |
# |0          |com.landwirt                                                                                      |22           |
# |0          |com.mobileappruparupa                                                                             |21           |
# |0          |co.alfataqu.id                                                                                    |21           |
# |0          |offerup.letgousedstuff.wishshopping.guideforofferupbuysell.walmartcouponsdiscounts5miles          |21           |
# |0          |com.onlinecoupons.wish                                                                            |21           |
# |0          |kr.co.ssg                                                                                         |21           |
# |0          |com.wBaleStartupTukang_13653202                                                                   |21           |
# |0          |com.lenskart.app                                                                                  |21           |
# |0          |com.numatams.numatamsapps.doublediscountcalc                                                      |21           |
# |0          |com.gardrops                                                                                      |21           |
# |0          |shopfreeship.onlineshopping                                                                       |21           |
# |0          |com.ernestoyaquello.lista.de.la.compra                                                            |21           |
# |0          |com.newandromo.dev500606.app698315                                                                |21           |
# |0          |pk.com.vpro.mybirds                                                                               |21           |
# |0          |com.ePositiveApps.olxx                                                                            |21           |
# |0          |com.onlineshopping.kidstoys                                                                       |21           |
# |0          |com.my.ffaccounts                                                                                 |21           |
# |0          |com.wRUBYMINIGOLDSUBDEALER_14995213                                                               |20           |
# |0          |com.appybuilder.ajayhi0009.SIBrowser                                                              |20           |
# |0          |com.swaypay.swaypay                                                                               |20           |
# |0          |online.turkey.shopping                                                                            |20           |
# |0          |com.dev.demidust.promoapp                                                                         |20           |
# |0          |com.dealsali.alideals                                                                             |20           |
# |0          |com.anaonlineshopping.handbags_women                                                              |20           |
# |0          |com.topindo.android                                                                               |20           |
# |0          |com.ebay.gumtree.za                                                                               |20           |
# |0          |au.com.oo                                                                                         |20           |
# |0          |net.cumart.ccw                                                                                    |20           |
# |0          |com.devplank.rastreiocorreios                                                                     |20           |
# |0          |com.bikinaplikasi.grosirtaswanita                                                                 |20           |
# |0          |com.onlinecoupons.victoriassecret                                                                 |20           |
# |1          |com.adfone.indosat                                                                                |20           |
# |0          |ru.filit.mvideo.b2c                                                                               |19           |
# |0          |com.empg.olxkw                                                                                    |19           |
# |0          |com.appscentral.free_slime_shop                                                                   |19           |
# |0          |lazada.deals.vouchers                                                                             |19           |
# |0          |com.daraz.android                                                                                 |19           |
# |0          |com.ebay.global.gmarket                                                                           |19           |
# |0          |com.hnsmall                                                                                       |19           |
# |0          |com.moblyapp                                                                                      |19           |
# |0          |com.jabong.android                                                                                |19           |
# |0          |earn.moneyplayandwin                                                                              |19           |
# |0          |com.bunz                                                                                          |19           |
# |0          |com.pmi.limited.pmiappm05728                                                                      |19           |
# |0          |com.mygrownewsa.grownewsa                                                                         |19           |
# |0          |hawkvladz.com.rcmart                                                                              |19           |
# |0          |com.yaari                                                                                         |19           |
# |0          |com.imedicalapps.discountcalculator                                                               |19           |
# |0          |online.shopping.philippines                                                                       |19           |
# |0          |com.appyacenter.babyclothes                                                                       |19           |
# |0          |com.meesho.supply                                                                                 |18           |
# |0          |tj.somon.somontj                                                                                  |18           |
# |0          |com.wTopChineseOnlineShopping_12625474                                                            |18           |
# |0          |com.cyber_monday.cybermonday2015                                                                  |18           |
# |0          |com.olx.ssa.gh                                                                                    |18           |
# |0          |com.turboodev.vetements                                                                           |18           |
# |0          |com.summerseger.daftarhargacabe                                                                   |18           |
# |0          |com.tokyocoldstudio.WitchDiamondBlitz                                                             |18           |
# |0          |com.indonesia_shopping.dl                                                                         |18           |
# |0          |apps.gmart                                                                                        |18           |
# |0          |com.mgs.onlineshoppingjapan                                                                       |18           |
# |0          |com.rabbit.bakpaorabbit                                                                           |18           |
# |0          |com.listia.Listia                                                                                 |18           |
# |0          |il.co.PriceZ.android                                                                              |18           |
# |0          |com.reserves.shop                                                                                 |18           |
# |0          |com.airjordans.android                                                                            |18           |
# |0          |com.wSlimeShop_12288272                                                                           |18           |
# |0          |com.dubarter.androidApp                                                                           |18           |
# |0          |com.paybright.portal                                                                              |18           |
# |0          |com.trixiesoft.clapp                                                                              |18           |
# |0          |com.glitterapps.popit                                                                             |18           |
# |0          |furniture.online.shopping                                                                         |18           |
# |0          |com.allinone.mobile_recharge.mobilerechargeapp                                                    |18           |
# |0          |com.ccode.coupon                                                                                  |18           |
# |0          |app.creator203                                                                                    |18           |
# |0          |com.mocasa.ph                                                                                     |17           |
# |0          |com.oxigen.futurepay                                                                              |17           |
# |0          |com.tophatter                                                                                     |17           |
# |0          |com.online.kitchen.appliances.shops                                                               |17           |
# |0          |com.xiaomi.shop                                                                                   |17           |
# |0          |com.wMazayaReseller_8355796                                                                       |17           |
# |0          |com.apprancher.freeantistresstoys                                                                 |17           |
# |0          |com.langital.app                                                                                  |17           |
# |0          |com.Mini.Antialls                                                                                 |17           |
# |0          |com.pradiste.app                                                                                  |17           |
# |0          |id.histore                                                                                        |17           |
# |0          |com.inditex.pullandbear                                                                           |17           |
# |0          |com.wNikeExpressFactoryDirect_8704826                                                             |17           |
# |0          |com.wSHOP101CODSHOPPINGINDIA_12256769                                                             |17           |
# |0          |com.korchix.sales                                                                                 |17           |
# |0          |com.mykadaionline.kadaionline                                                                     |17           |
# +-----------+--------------------------------------------------------------------------------------------------+-------------+
# """
#
# """
# after_label	app	device_id_cnt
# 1	N                                                                                                 	10085
# 1	id.co.shopintar                                                                                   	327
# 1	com.app.tokobagus.betterb                                                                         	110
# 1	com.adfone.indosat                                                                                	20
# """
df=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/20220630_delay_1")
df_2=df.where("now_label=0").where("bundle_app_set is null")
df_2.createTempView("df_2_table")
spark.sql(" \
    select  \
        after_label    \
        ,lastdaygap_label    \
        ,count(device_id) as device_id_cnt  \
    from    \
        df_2_table  \
    group by    \
        after_label,lastdaygap_label    \
    order by    \
        after_label,device_id_cnt  desc \
").show(500,truncate=False)
"""
+-----------+----------------+-------------+                                    
|after_label|lastdaygap_label|device_id_cnt|
+-----------+----------------+-------------+
|0          |0               |61108066     |
|0          |100             |57607979     |
|0          |1               |14525431     |
|0          |2               |8290884      |
|0          |3               |5564099      |
|0          |4               |4265506      |
|0          |5               |3477294      |
|0          |6               |3127716      |
|0          |7               |2673333      |
|0          |8               |2668974      |
|0          |9               |2347500      |
|0          |10              |2140665      |
|0          |11              |1982320      |
|0          |12              |1906273      |
|0          |15              |1820488      |
|0          |13              |1774621      |
|0          |14              |1710452      |
|0          |16              |1617479      |
|0          |18              |1519327      |
|0          |17              |1471250      |
|0          |19              |1449419      |
|0          |20              |1369076      |
|0          |21              |1205753      |
|0          |23              |1084419      |
|0          |24              |1054740      |
|0          |25              |1052264      |
|0          |27              |1035720      |
|0          |22              |1035270      |
|0          |26              |1007113      |
|0          |28              |991155       |
|0          |29              |955286       |
|0          |31              |926051       |
|0          |51              |924258       |
|0          |32              |909921       |
|0          |30              |906409       |
|0          |33              |872799       |
|0          |35              |858878       |
|0          |36              |857486       |
|0          |37              |846876       |
|0          |43              |843382       |
|0          |50              |836025       |
|0          |34              |835304       |
|0          |41              |821895       |
|0          |42              |814844       |
|0          |49              |805922       |
|0          |45              |801669       |
|0          |91              |794397       |
|0          |40              |786718       |
|0          |39              |781040       |
|0          |44              |772030       |
|0          |53              |758156       |
|0          |38              |755831       |
|0          |52              |753652       |
|0          |48              |750788       |
|0          |47              |746793       |
|0          |46              |745604       |
|0          |92              |742335       |
|0          |55              |721371       |
|0          |63              |713944       |
|0          |67              |701837       |
|0          |57              |696790       |
|0          |88              |696014       |
|0          |56              |693572       |
|0          |62              |692497       |
|0          |65              |684513       |
|0          |54              |683463       |
|0          |81              |683162       |
|0          |80              |678084       |
|0          |93              |677179       |
|0          |76              |676060       |
|0          |87              |675124       |
|0          |75              |673984       |
|0          |71              |666966       |
|0          |86              |660699       |
|0          |68              |660330       |
|0          |64              |660054       |
|0          |74              |658700       |
|0          |82              |654990       |
|0          |77              |653751       |
|0          |70              |651092       |
|0          |79              |648093       |
|0          |69              |644223       |
|0          |66              |637057       |
|0          |83              |633507       |
|0          |58              |632674       |
|0          |89              |628963       |
|0          |73              |626963       |
|0          |60              |626265       |
|0          |78              |625379       |
|0          |84              |622384       |
|0          |90              |621640       |
|0          |72              |616994       |
|0          |85              |613543       |
|0          |94              |611726       |
|0          |59              |610136       |
|0          |98              |609831       |
|0          |96              |602914       |
|0          |97              |592152       |
|0          |99              |590839       |
|0          |95              |584048       |
|0          |61              |522745       |
|1          |0               |7422         |
|1          |1               |1073         |
|1          |2               |583          |
|1          |3               |327          |
|1          |4               |250          |
|1          |5               |203          |
|1          |6               |152          |
|1          |7               |104          |
|1          |8               |98           |
|1          |9               |64           |
|1          |10              |59           |
|1          |16              |11           |
|1          |14              |7            |
|1          |11              |5            |
|1          |12              |5            |
|1          |13              |3            |
|1          |17              |2            |
|1          |22              |2            |
|1          |26              |2            |
|1          |18              |2            |
|1          |27              |2            |
|1          |53              |2            |
|1          |100             |1            |
|1          |46              |1            |
|1          |19              |1            |
|1          |20              |1            |
|1          |90              |1            |
|1          |50              |1            |
|1          |57              |1            |
|1          |52              |1            |
|1          |34              |1            |
|1          |35              |1            |
|1          |78              |1            |
|1          |54              |1            |
|1          |44              |1            |
|1          |66              |1            |
|1          |24              |1            |
|1          |15              |1            |
+-----------+----------------+-------------+
"""


df_3=spark.read.format("orc").load("oss://sdkemr-yeahmobi/user/chensheng/pkg/lazada/IDN_android/sample_ana/20220701_delay_1")
df_4=df.where("now_label=0").where("bundle_app_set is null")
df_4.createTempView("df_4_table")
spark.sql(" \
    select  \
        after_label    \
        ,lastdaygap_label    \
        ,count(device_id) as device_id_cnt  \
    from    \
        df_4_table  \
    group by    \
        after_label,lastdaygap_label    \
    order by    \
        after_label,device_id_cnt  desc \
").show(500,truncate=False)

"""
+-----------+----------------+-------------+                                    
|after_label|lastdaygap_label|device_id_cnt|
+-----------+----------------+-------------+
|0          |0               |61108066     |
|0          |100             |57607979     |
|0          |1               |14525431     |
|0          |2               |8290884      |
|0          |3               |5564099      |
|0          |4               |4265506      |
|0          |5               |3477294      |
|0          |6               |3127716      |
|0          |7               |2673333      |
|0          |8               |2668974      |
|0          |9               |2347500      |
|0          |10              |2140665      |
|0          |11              |1982320      |
|0          |12              |1906273      |
|0          |15              |1820488      |
|0          |13              |1774621      |
|0          |14              |1710452      |
|0          |16              |1617479      |
|0          |18              |1519327      |
|0          |17              |1471250      |
|0          |19              |1449419      |
|0          |20              |1369076      |
|0          |21              |1205753      |
|0          |23              |1084419      |
|0          |24              |1054740      |
|0          |25              |1052264      |
|0          |27              |1035720      |
|0          |22              |1035270      |
|0          |26              |1007113      |
|0          |28              |991155       |
|0          |29              |955286       |
|0          |31              |926051       |
|0          |51              |924258       |
|0          |32              |909921       |
|0          |30              |906409       |
|0          |33              |872799       |
|0          |35              |858878       |
|0          |36              |857486       |
|0          |37              |846876       |
|0          |43              |843382       |
|0          |50              |836025       |
|0          |34              |835304       |
|0          |41              |821895       |
|0          |42              |814844       |
|0          |49              |805922       |
|0          |45              |801669       |
|0          |91              |794397       |
|0          |40              |786718       |
|0          |39              |781040       |
|0          |44              |772030       |
|0          |53              |758156       |
|0          |38              |755831       |
|0          |52              |753652       |
|0          |48              |750788       |
|0          |47              |746793       |
|0          |46              |745604       |
|0          |92              |742335       |
|0          |55              |721371       |
|0          |63              |713944       |
|0          |67              |701837       |
|0          |57              |696790       |
|0          |88              |696014       |
|0          |56              |693572       |
|0          |62              |692497       |
|0          |65              |684513       |
|0          |54              |683463       |
|0          |81              |683162       |
|0          |80              |678084       |
|0          |93              |677179       |
|0          |76              |676060       |
|0          |87              |675124       |
|0          |75              |673984       |
|0          |71              |666966       |
|0          |86              |660699       |
|0          |68              |660330       |
|0          |64              |660054       |
|0          |74              |658700       |
|0          |82              |654990       |
|0          |77              |653751       |
|0          |70              |651092       |
|0          |79              |648093       |
|0          |69              |644223       |
|0          |66              |637057       |
|0          |83              |633507       |
|0          |58              |632674       |
|0          |89              |628963       |
|0          |73              |626963       |
|0          |60              |626265       |
|0          |78              |625379       |
|0          |84              |622384       |
|0          |90              |621640       |
|0          |72              |616994       |
|0          |85              |613543       |
|0          |94              |611726       |
|0          |59              |610136       |
|0          |98              |609831       |
|0          |96              |602914       |
|0          |97              |592152       |
|0          |99              |590839       |
|0          |95              |584048       |
|0          |61              |522745       |
|1          |0               |7422         |
|1          |1               |1073         |
|1          |2               |583          |
|1          |3               |327          |
|1          |4               |250          |
|1          |5               |203          |
|1          |6               |152          |
|1          |7               |104          |
|1          |8               |98           |
|1          |9               |64           |
|1          |10              |59           |
|1          |16              |11           |
|1          |14              |7            |
|1          |11              |5            |
|1          |12              |5            |
|1          |13              |3            |
|1          |27              |2            |
|1          |17              |2            |
|1          |22              |2            |
|1          |18              |2            |
|1          |53              |2            |
|1          |26              |2            |
|1          |100             |1            |
|1          |20              |1            |
|1          |46              |1            |
|1          |52              |1            |
|1          |57              |1            |
|1          |44              |1            |
|1          |19              |1            |
|1          |24              |1            |
|1          |90              |1            |
|1          |35              |1            |
|1          |34              |1            |
|1          |50              |1            |
|1          |54              |1            |
|1          |15              |1            |
|1          |78              |1            |
|1          |66              |1            |
+-----------+----------------+-------------+
"""