package org.kwangsun.spark.kotlin.example

import org.apache.spark.sql.SparkSession

object AdjustmentHourlyExample {
    @JvmStatic fun main(args: Array<String>) {

        val spark = SparkSession.builder()
                .appName("SampleAdjustmentHourly")
                .getOrCreate()
        SparkSession.builder()


        spark.read().
                format("jdbc").
                option("url", "jdbc:postgresql://flamelrdspg.cpb35ue8880n.ap-northeast-2.rds.amazonaws.com:15487/flameldb").
                option("query", "select * from flamelscm.tbl_req_records_period").
                option("user","gagamel").
                option("password","tmajvm2019!").
                load().
                write().
                //mode("append").
                save("tbl_req_records_period.parquet")

        val reqRecordsPeriodDf = spark.read().parquet("tbl_req_records_period.parquet")
        reqRecordsPeriodDf.createOrReplaceTempView("tbl_req_records_period")

        val sensorDataHourDf = spark.read().parquet("tbl_sensor_data_hourly.parquet")
        sensorDataHourDf.repartition(10)
        sensorDataHourDf.cache()
        sensorDataHourDf.createOrReplaceTempView("tbl_sensor_data_hourly")

        // Load tbl_req_records
        val prodInfoDf = spark.read().parquet("tbl_product_info.parquet")
        prodInfoDf.createOrReplaceTempView("tbl_product_info")
        //prodInfoDf.show

        val sensInfoDf = spark.read().parquet("tbl_sensor_info.parquet")
        sensInfoDf.createOrReplaceTempView("tbl_sensor_info")
        //sensInfoDf.show

        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", -1)


        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", -1)
        val joinDf2 = spark.sql("""

          select pi.prod_id, s.sensor_id, s.base_time, tb.cnt, s.quality*tb.cnt as quality, pi.location_id
            from tbl_sensor_data_hourly s
               , tbl_product_info pi
               , tbl_sensor_info si
               , ( select base_time, sub_location_id, count(*) as cnt
                    from tbl_req_records_period r
                        , tbl_location_full_info fl
                    where r.location_id = fl.location_id
                    Group by base_time, sub_location_id
               ) tb
           where s.base_time = tb.base_time
           And pi.prod_id = si.prod_id
           And si.sensor_id = s.sensor_id
           And pi.location_id = tb.sub_location_id

        """)

        joinDf2.createOrReplaceTempView("joinDf2")

        val joinDf3 = spark.sql("""
        Select prod_id, sum(cnt) as tCnt, sum(quality) as tQuality
        From joinDf2
        Group by prod_id
        """)


        val cur = java.text.SimpleDateFormat("yyyyMMddHHmmss").format(java.util.Date())
        joinDf3.write().
                mode("overwrite").
                save("joinDf3_${cur}.parquet")


    }
}

