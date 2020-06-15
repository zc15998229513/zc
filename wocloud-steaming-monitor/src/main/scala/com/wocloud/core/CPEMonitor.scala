package com.wocloud.core

import com.wocloud.config.PropertiesConfig.{cpeID, kafkaServers, offsetReset}
import com.wocloud.config.SparkConfig.sparkConf
import com.wocloud.handler.CpeMessageHandler._
import com.wocloud.config.PropertiesConfig._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}



object CPEMonitor {
    def main(args: Array[String]): Unit = {

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> kafkaServers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> cpeID,
            "max.poll.interval.ms" -> "600000",
            "auto.offset.reset" -> offsetReset,
            "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )

        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.sparkContext.setLogLevel("ERROR")

        val cpeMonitor = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](List(yunchang_cloudNetwork_cpe), kafkaParams))
        val cpe2VpeMonitor = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](List(yunchang_cloudNetwork_vpe), kafkaParams))


        //时间窗口长度
        val window = 300

        val cpeMonitorData = cpeMonitor.map(_.value()).window(Seconds(window), Seconds(window))
        val cpe2VpeMonitorData = cpe2VpeMonitor.map(_.value()).window(Seconds(window), Seconds(window))

        handleCpeMonitor(cpeMonitorData)
        handleCpe2VpeMonitor(cpe2VpeMonitorData)

        ssc.start()
        ssc.awaitTermination()

    }
}
