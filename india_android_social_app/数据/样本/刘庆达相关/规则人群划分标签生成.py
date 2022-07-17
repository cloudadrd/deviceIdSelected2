package org.yeahmobi.dmp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.yeahmobi.utils.{RDDMultipleTextOutputFormat, write_mysql_util}
import org.yeahmobi.utils.write_to_s3.{Write_S3_multi_system, Write_S3_single}
import org.yeahmobi.service_pre.Deviceid_sdk_dmp.{getdayBefore, getday_Mysql}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.collection.mutable
import scala.collection.mutable.HashSet
object ali_device_new {
  def main(args: Array[String]): Unit = {

    var today_date: Date = new Date()
    val interval = args(0).toInt
    val yesterday = getdayBefore(today_date, interval)
    val three_day = getdayBefore(today_date, interval + 2 )
    val seven_day = getdayBefore(today_date, interval+ 6 )
    val two_week = getdayBefore(today_date, interval+14 )
    val two_month = getdayBefore(today_date, interval+ 90 )
    val day_string = getdayString(today_date,interval+7)

    val fuyu_path = "oss://sdkemr-yeahmobi/sdk_device/fuyu/" + yesterday

    val bundle_path = "oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_tag/day=" +yesterday

    val wm_path = "oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_source_aff"
    val adx_path = "oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_source_adx/"
    val direct_path = "oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_device_source_direct/"
    val ipua_path = "oss://dmp-yeahmobi-com/hive_dataware/dmp/t_dmp_ios_ipua/day=" + yesterday

    val spark = new sql.SparkSession.Builder()
      .appName("sdk_device")
      .enableHiveSupport()
      .getOrCreate()
    // -------- new us city
    val us_city_rdd = spark.sparkContext.textFile("oss://sdkemr-yeahmobi/sdk_device/us_region/").map(line => {
      val row = line.split("\t")
      (row(0),row(1),row(2),row(3),row(4),row(5),"","","","")
    })
    // -----------------------



    val kr_slot_rdd = spark.sparkContext.textFile("oss://sdkemr-yeahmobi/kr_slot_reflect").map(line => (line.split("\t")(2),(line.split("\t")(3),line.split("\t")(4)))).collectAsMap()
    val kr_slot_br = spark.sparkContext.broadcast(kr_slot_rdd)

    val filter_source_rdd = spark.sparkContext.textFile("oss://sdkemr-yeahmobi/filter_source.properties").collect()
    val filter_source_br = spark.sparkContext.broadcast(filter_source_rdd)

    val ipua_tag = spark.read.format("orc").load(ipua_path)
    ipua_tag.createTempView("ipua_source")
    val ipua_df = spark.sql(
      s"""
         |select ip,ua,osversion,lang,country,datediff(from_unixtime(unix_timestamp('${yesterday}','yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(lastactiveday,'yyyyMMdd'),'yyyy-MM-dd')) as dif_day from ipua_source where lastactiveday>= ${two_week}  and country not in ( 'GBR','FRA','TUR','IND') and ip like '%.%'
         |union
         |select ip,ua,osversion,lang,country,datediff(from_unixtime(unix_timestamp('${yesterday}','yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(lastactiveday,'yyyyMMdd'),'yyyy-MM-dd')) as dif_day from ipua_source where lastactiveday>= ${two_month}  and country in ( 'GBR','FRA','TUR','IND') and ip like '%.%'
         |""".stripMargin)
    val ipua_rdd = ipua_df.select(ipua_df.col("ip"),ipua_df.col("ua"),ipua_df.col("osversion"),ipua_df.col("lang"),ipua_df.col("country"),ipua_df.col("dif_day")).rdd.persist()
      val map_rdd = ipua_rdd.map(row => {
        val a = new Device_info
        a.ip = row(0).toString
        a.ua = row(1).toString
        a.osv = row(2).toString
        a.lang = row(3).toString
        if(row(4).toString == "ARE"){
          a.country = "AE"
        }else if(row(4).toString == "TUR"){
          a.country = "TR"
        } else if(row(4).toString == "KOR"){
          a.country = "KR"
        }else if(row(4).toString == "BGD"){
          a.country = "BD"
        }else if(row(4).toString == "PAK"){
          a.country = "PK"
        }else if(row(2).toString == "UKR"){
          a.country = "UA"
        } else if(row(2).toString == "IRL"){
          a.country = "IE"
        }else {
          a.country = row(4).toString.substring(0,2)
        }
        val dif_day = row(5).toString.toInt

          if (dif_day == 0) {
            a.label = "ew0"
            a.slot = "28206264"
          } else if (dif_day == 1) {
            a.label = "ew1"
            a.slot = "19194551"
          } else if (dif_day == 2) {
            a.label = "ew2"
            a.slot = "26241235"
          } else if (dif_day >= 3 && dif_day <= 6) {
            a.label = "ew3"
            a.slot = "83810538"
          } else if (dif_day >= 7) {
            a.label = "ew4"
            a.slot = "10981429"
          } else {
            a.label = "filter"
          }

        a.platform = "ios"
        a.source = "adx"


        a.device = "null"
        (a.country, a.platform , a.source,a.device, a.slot, a.label ,a.ip , a.ua,a.osv,a.lang)
    }).filter(a => a._6!="filter")

    val ew_rdd = ipua_rdd.map(row => {
      val a = new Device_info
      a.ip = row(0).toString
      a.ua = row(1).toString
      a.osv = row(2).toString
      a.lang = row(3).toString
      if(row(4).toString == "ARE"){
        a.country = "AE"
      }else if(row(4).toString == "TUR"){
        a.country = "TR"
      } else if(row(4).toString == "KOR"){
        a.country = "KR"
      }else if(row(4).toString == "BGD"){
        a.country = "BD"
      }else if(row(4).toString == "PAK"){
        a.country = "PK"
      }else if(row(4).toString == "UKR"){
        a.country = "UA"
      }else {
        a.country = row(4).toString.substring(0,2)
      }
      val dif_day = row(5).toString.toInt

      if(dif_day <=2){
        a.label = "ew"
        a.slot = "66422694"
      } else {
        a.label = "filter"
      }
      a.platform = "ios"
      a.source = "adx"
      a.device = "null"
      (a.country, a.platform , a.source,a.device, a.slot, a.label ,a.ip , a.ua,a.osv,a.lang)
    }).filter(a => a._6!="filter")

    val ew_out = map_rdd.union(ew_rdd)


    val df_tag = spark.read.format("orc").load(bundle_path)

    df_tag.createTempView("source_tag")


    val bundle_df = spark.sql(
      s"""
        select ifa,size(bundles) as bundle_size,ua,ip,platform,osversion,lang,country,geo from source_tag where lastactiveday >= ${seven_day}
        and size(bundles) > 0
        and platform in ('android','ios')
        and (geo in ('KOR','GBR','TWN','JPN','IND','DEU','BRA','MYS','VNM','PHL','SGP','THA','USA')or ( geo = 'OTHER' and country in ('ARE','TUR','BGD','PER','FRA','PAK','ESP','UKR','AZE','PER','MEX','MYS','VNM','PHL','SGP','THA','CIV','CMR','BFA','SEN','NGA','IRL')))
        union
        select ifa,size(bundles) as bundle_size,ua,ip,platform,osversion,lang,country,geo from source_tag where lastactiveday >= ${two_week}
        and size(bundles) > 0
        and platform in ('android','ios')
        and geo in ('RUS','IDN','MEX','BRA')

        """.stripMargin
    )
    //and (geo in ('KOR','GBR','TWN','JPN','IDN','IND','DEU','RUS')or country in ('ARE','TUR','BGD','PER'))
    val bundel_data = bundle_df.select(bundle_df.col("ifa"),bundle_df.col("bundle_size"),bundle_df.col("country"),bundle_df.col("platform"),bundle_df.col("ip"),bundle_df.col("ua"),bundle_df.col("osversion"),bundle_df.col("lang"))
    val bundle_rdd = bundel_data.rdd
      //.repartition(800)
      .map(row => {
      val struct = new Device_info
      struct.device = row(0).toString
      struct.bundles_size = row(1).toString.toInt
      try {
        struct.ip = row(4).toString
      }catch {
        case e: Exception => {
          struct.ip = ""
        }
      }
      try {
        struct.ua = row(5).toString
      }catch {
        case e: Exception => {
          struct.ua = ""
        }
      }
      try {
        struct.osv = row(6).toString
      }catch {
        case e: Exception => {
          struct.osv =  ""
        }
      }
      try {
        struct.lang = row(7).toString
      }catch {
        case e: Exception => {
          struct.lang =  ""
        }
      }

      if(row(2).toString == "ARE"){
        struct.country = "AE"
      }else if(row(2).toString == "TUR"){
        struct.country = "TR"
      } else if(row(2).toString == "KOR"){
        struct.country = "KR"
      }else if(row(2).toString == "BGD"){
        struct.country = "BD"
      }else if(row(2).toString == "PAK"){
        struct.country = "PK"
      }else if(row(2).toString == "UKR"){
        struct.country = "UA"
      }else if(row(2).toString == "IRL"){
        struct.country = "IE"
      }else if(row(2).toString == "SEN"){
        struct.country = "SN"
      }
      else {
        struct.country = row(2).toString.substring(0,2)
      }
      struct.platform = row(3).toString
      if ( struct.bundles_size == 1) struct.label = "a"
      else if ( struct.bundles_size >= 2 && struct.bundles_size <= 4 ) struct.label = "b"
      else if ( struct.bundles_size >= 5 && struct.bundles_size <= 10 ) struct.label = "c"
      else struct.label = "d"
      var osv_int = 0
      try{
        osv_int =struct.osv.substring(0,4).replaceAll("\\.","").toInt
      }catch {
        case e:  Exception => {
          osv_int = 0
        }
      }
      if ( struct.device == "" && struct.ua != "" && struct.ip != "" && osv_int >= 145){
        struct.label = "e"
      }
      struct
    })

    val gb_devcie = spark.sql(
      s"""
        |        select ifa,ua,ip,platform,osversion,lang,country,geo from source_tag where lastactiveday >= '$two_month' and lastactiveday < '${two_week}'
        |        and size(bundles) > 0
        |        and ((platform in ('android') and geo in ('GBR'))or (platform in ('android','ios') and geo = 'KOR'))
        |""".stripMargin).rdd.map(row => {
        if (row(7).toString == "GBR") ("GB", "android", "adx", row(0).toString, "57351627", "aw", "null", "null", "null", "null")
        else if (row(7).toString == "KOR" && row(3).toString == "android") ("KR", "android", "adx", row(0).toString, "57351627", "aw", "null", "null", "null", "null")
        else {
          try {
            ("KR", "ios", "adx", row(0).toString, "10981429", "ew4", row(2).toString, row(1).toString, row(4).toString, row(5).toString)
          } catch {
            case e: Exception => {
              ("KR", "ios", "adx","filter", "filter", "filter", "null", "null", "null", "null")
            }
          }
        }

    }).filter(row => row._5!="filter")

    val wm_df = get_Channel_Device(spark,wm_path,"aff_id",yesterday,three_day ,seven_day,two_week).repartition(1200)
    val adx_df = get_Channel_Device(spark,adx_path,"adx",yesterday,three_day,seven_day,two_week).repartition(1200)
    val direct_df = get_Channel_Device(spark,direct_path,"channel",yesterday,three_day,seven_day,two_week).repartition(100)
    val channel_df = wm_df.union(adx_df).union(direct_df)
      //.repartition(2000)


    val all_device_df = channel_df.select(channel_df.col("country_platform"), channel_df.col("uid"), channel_df.col("label") , channel_df.col("day")).rdd
      //.repartition(1000)
    val join_rdd = all_device_df.filter(row => {
        row(0).toString.contains("KR") ||
          row(0).toString.contains("GB")||
          row(0).toString.contains("TW")||
          row(0).toString.contains("JP") ||
          row(0).toString.contains("IN") ||
          row(0).toString.contains("ID") ||
          row(0).toString.contains("DE") ||
          row(0).toString.contains("AE") ||
          row(0).toString.contains("RU") ||
          row(0).toString.contains("FR") ||
          row(0).toString.contains("PK")
    })
      .map(row => {
        val struct = new Device_info
        struct.country = row(0).toString.split("_")(0)
        struct.platform =  row(0).toString.split("_")(1)
        struct.device = row(1).toString.split("SUB")(0).replaceAll(",","")
        struct.source_sec_set.add(row(2).toString)
        (struct.device,struct)
      }).reduceByKey((a,b) => {
      val s: mutable.HashSet[String] = a.source_sec_set.++(b.source_sec_set)
      a.source_sec_set = s
      a
    }).map( a => {
      val struct = a._2
      val size = struct.source_sec_set.size
      if( size == 0){
        struct.label = "w"
      } else if (size == 1) {
        struct.label = "x"
      } else if (size >= 2 && size <= 4) {
        struct.label = "y"
      } else {
        struct.label = "z"
      }
      struct
      // struct
    })

    val bundle_join_rdd = bundle_rdd.union(join_rdd)
      .map(a => (a.device,a))
      .reduceByKey((a,b) =>{
        if (a.label == "a" ||a.label == "b" || a.label == "c" || a.label == "d"){
          a.label = a.label +  b.label
        }else {
          a.label = b.label + a.label
        }
        if (a.ua == "" || a.ua == "null") {
          a.ua = b.ua
          a.ip = b.ip
          a.osv = b.osv
          a.lang = b.lang
        }

        a
      }).map(_._2)

      // .filter(a => a.label.contains("a") || a.label.contains("b") || a.label.contains("c") || a.label.contains("d"))
      .map( a => {
        if(a.label.toString.length == 1 ) {
          a.label = a.label + "w"
        }
        if(a.platform == "ios"){

            a.slot = kr_slot_br.value.getOrElse(a.label, ("00000", "00000"))._1


        }else {
          a.slot = kr_slot_br.value.getOrElse(a.label,("00000","00000"))._2
        }
        val adx = List[String]("aw","ew")
        val direct = List[String](  "ax","ay","az","bw","cw")
        val wm = List[String]("cx","cy","cz","bx","by","bz","dw","dx","dy","dz")
        if (adx.contains(a.label)) {
          a.source = "adx"
        } else if (direct.contains(a.label)) {
          a.source = "direct"
        } else if (wm.contains(a.label)) {
          a.source = "wm"
        }else {
          a.source = "filter"
        }
        (a.country, a.platform , a.source,a.device, a.slot, a.label ,a.ip , a.ua,a.osv,a.lang)
      }).filter(a => a._3 != "filter")
      //.map(a => a._1 + "\t" + a._2 + a._3 + "\t" +a._4 + "\t" +a._5 + "\t" +a._6 + "\t" +a._7  ) .repartition(20).saveAsTextFile("oss://sdkemr-yeahmobi/sdk_device/bd")


    val all_device_rdd = all_device_df.filter( row => {
      !row(0).toString.contains("KR")&&
        !row(0).toString.contains("GB")&&
        !row(0).toString.contains("JP") &&
        !row(0).toString.contains("TW") &&
        !row(0).toString.contains("IN")&&
        !row(0).toString.contains("ID")&&
        !row(0).toString.contains("DE") &&
        !row(0).toString.contains("AE") &&
        !row(0).toString.contains("RU") &&
        !row(0).toString.contains("FR") &&
        !row(0).toString.contains("PK")
    }  )
     // .repartition(2000)
      .map(row => {
        var day_count : Int = 0
        try {
           day_count = row(3).toString.toInt
        }catch {
          case e: Exception => {
            day_count  = 0
          }
        }
        val label_set = HashSet(row(2).toString)
        (row(0) + "\t" + row(1),(day_count,label_set))
      })
      .reduceByKey((a,b)=>{
      val label_set : HashSet[String] = a._2.++(b._2)
        (a._1 + b._1,label_set)
    })
      .map(line => {
      val row = line._1.split("\t")
      val country = row(0).split("_")(0)
      val platform = row(0).split("_")(1)
      var device = ""
      if (row.length > 1) {
        device = row(1).toString.split("SUB")(0).replaceAll(",", "")
      }
      val day_count = line._2._1
      val label_count = line._2._2.size
      //device + "\t" + country  + "\t" + day_count.toString + "\t" + label_count.toString
      var day = ""
      var label = ""
      if (country == "KR" || country == "CN") {
        if (day_count == 1) {
          day = "a"
        } else if (day_count == 2) {
          day = "b"
        } else {
          day = "c"
        }
      } else {
        if (day_count == 1) {
          day = "a"
        } else if (day_count <= 3) {
          day = "b"
        } else if (day_count <= 6) {
          day = "c"
        } else {
          day = "d"
        }
      }

      if (label_count == 1) {
        label = "w"
      } else if (label_count == 2) {
        label = "x"
      } else if (label_count <= 4) {
        label = "y"
      } else {
        label = "z"
      }
      var slot = ""
      var label_reflect = day + label
      if (platform == "ios") {
        slot = kr_slot_br.value.getOrElse(label_reflect, ("00000", "00000"))._1
      } else {
        slot = kr_slot_br.value.getOrElse(label_reflect, ("00000", "00000"))._2
      }
      var source = ""
      if (country == "KR" || country == "CN") {
        val adx = List[String]("aw")
        val direct = List[String]("ax", "ay", "az", "bw", "cw")
        val wm = List[String]("cx", "cy", "cz", "bx", "by", "bz")
        if (adx.contains(label_reflect)) {
          source = "adx"
        } else if (direct.contains(label_reflect)) {
          source = "direct"
        } else if (wm.contains(label_reflect)) {
          source = "wm"
        } else {
          source = "filter"
        }
      } else {
        val adx = List[String]("aw")
        val direct = List[String]("ax", "ay", "az", "bw", "cw")
        val wm = List[String]("cx", "cy", "cz", "bx", "by", "bz", "dw", "dx", "dy", "dz")
        if (adx.contains(label_reflect)) {
          source = "adx"
        } else if (direct.contains(label_reflect)) {
          source = "direct"
        } else if (wm.contains(label_reflect)) {
          source = "wm"
        } else {
          source = "filter"
        }
      }
      if (country == "IN" && source == "direct") {
        source = "wm"
        val IN_filter = Array[String]("aw", "bw", "cw", "dw", "dy")
        if (platform == "android" && IN_filter.contains(label_reflect)) label_reflect = "filter"
      } else if (country == "CN" && platform == "ios" && source == "direct") {
        source = "wm"
        if (label_reflect == "cw") label_reflect = "filter"
      }

      (country ,platform ,source, device,slot, label_reflect,"null","null","null","null")
    })
    .filter(a => a._3 != "filter" && a._4.toString != "")
      //.filter(a => a._1.toString != "US_ios_adx" && a._1.toString != "US_ios_direct" && a._1.toString != "IN_android_adx")
      .filter(a => !filter_source_br.value.contains(a._1 + "_" + a._2 + "_" + a._3))
      .union(bundle_join_rdd).union(ew_out).union(us_city_rdd).union(gb_devcie)

    val rowRdd = all_device_rdd.map(x => Row(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10))

    val schema = StructType(List(
          StructField("country",StringType,true),
          StructField("platform",StringType,true),
          StructField("source",StringType,true),
          StructField("device",StringType,true),
          StructField("slot",StringType,true),
          StructField("label",StringType,true),
          StructField("ip",StringType,true),
          StructField("ua",StringType,true),
          StructField("osv",StringType,true),
          StructField("lang",StringType,true)
         ))
    val df_out =  spark.createDataFrame(rowRdd,schema)
    df_out.createOrReplaceTempView("sdk_use_device")
    val result = spark.sql("select * from sdk_use_device")
    result.repartition(500).write.orc(fuyu_path)

  }
  def getdayBefore(dt:Date,interval:Int):String={
    val dateFormat :SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(dt)
    cal.add(Calendar.DATE,-interval)
    val date=dateFormat.format(cal.getTime)
    date
  }
  def getdayString(dt:Date,interval:Int):String = {
    var today_date: Date = new Date()
    val dateFormat :SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(today_date)
    v



def get_Channel_Device(spark: SparkSession, path: String, source: String, yesterday: String, three_day: String,
                       seven_day: String, two_week: String): DataFrame = {
  val


df_channel = spark.read.format("orc").load(path)
val
table_name = "source_" + source
df_channel.createTempView(table_name)
val
channel_df = spark.sql(
  s
"""
         | select  country_platform,uid,label,sum(day) day from
         |(select  concat(geo, '_', platform) as country_platform,ifa as uid ,${source} as label , count(day) as day
         |       from ${table_name}
         |       where day <= '${yesterday}' and day >= '${three_day}' and geo in ('KR','HK','JP','TW','CN','US','IN','BR','GB','TH','PH','DE','VN','ID','CA','MX','EC','SA','AE','CO','MY','SG','AU','MO','FR','ES','RU','ZA','PT','IT','PK','FR','AZ','PE','NG','IE') and platform in ('ios','android')
         | group by geo,platform,ifa,${source}
         | union
         | select concat(geo, '_', platform) as country_platform,ifa as uid ,${source} as label , count(day) as day
         |       from ${table_name}
         |       where day <= '${three_day}' and day >= '${seven_day}' and geo in ('HK','JP','TW','BR','GB','TH','PH','DE','VN','ID','CA','MX','EC','SA','AE','CO','MY','SG','AU','MO','FR','ES','IN','US','RU','ZA','PT','IT','PK','FR','NG','IE') and platform in ('ios','android')
         |
         |group by geo,platform,ifa,${source}
         |union
         | select concat(geo, '_', platform) as country_platform,ifa as uid ,${source} as label , count(day) as day
         |       from ${table_name}
         |       where day <= '${seven_day}' and day >= '${two_week}' and geo in ('RU','ID','MX','BR')
         |group by geo,platform,ifa,${source}
         |)
         |group by country_platform,uid,label
         """.stripMargin)
channel_df
}








