package com.wocloud.core

import com.wocloud.config.SparkConfig.sparkConf
import com.wocloud.handler.VpeMessageHandler._
import com.wocloud.config.PropertiesConfig._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.wocloud.config.PropertiesConfig.{vpeID, kafkaServers, offsetReset}

object VPEMonitor {
    def main(args: Array[String]): Unit = {

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> kafkaServers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> vpeID,
            "max.poll.interval.ms" -> "600000",
            "auto.offset.reset" -> offsetReset,
            "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )

        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.sparkContext.setLogLevel("ERROR")

        val vpeMonitor = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](List(yunchang_cloudNetwork_vpe), kafkaParams))
        val vpeDelayMonitor = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](List(yunchang_cloudNetwork_sdwan), kafkaParams))

        //时间窗口长度
        val window = 300

        val vpeMonitorData = vpeMonitor.map(_.value()).window(Seconds(window), Seconds(window))
        val vpeDelayMonitorData = vpeDelayMonitor.map(_.value()).window(Seconds(window), Seconds(window))

        handleVpeMonitor(vpeMonitorData)
        handleVpeDelayMonitor(vpeDelayMonitorData)

        ssc.start()
        ssc.awaitTermination()

    }
}
