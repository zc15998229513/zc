package com.wocloud.config

import org.apache.spark.SparkConf

object SparkConfig {
    val sparkConf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("StreamingAlarmApp")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        .set("spark.streaming.kafka.maxRatePerPartition", "80")
        .set("spark.dynamicAllocation.enabled","false")
    //    .set("spark.locality.wait","0")
    //    .set("spark.speculation", "true")
    //    .set("spark.speculation.interval", "300s")
    //    .set("spark.speculation.quantile","0.9")

}
