package com.wocloud.core

import com.wocloud.config.PropertiesConfig._
import com.wocloud.config.SparkConfig.sparkConf
import com.wocloud.handler.CeMessageHandler._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
 * @Author : ZC
 * @Description :
 * @Date : 2020/6/11
 * @Version : 
 */
object CEMonitor {
    def main(args: Array[String]): Unit = {

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> kafkaServers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> ceID,
            "max.poll.interval.ms" -> "600000",
            "auto.offset.reset" -> offsetReset,
            "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
            "enable.auto.commit" -> (true: java.lang.Boolean)
        )

        val ssc = new StreamingContext(sparkConf, Seconds(2))
        ssc.sparkContext.setLogLevel("ERROR")

        val ceMonitor = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](List(yunchang_cloudNetwork_ce),kafkaParams))

        //时间窗口长度
        val window = 300

        val ceMonitorData = ceMonitor.map(_.value()).window(Seconds(window), Seconds(window))

        handleCEMonitor(ceMonitorData)

        ssc.start()
        ssc.awaitTermination()

    }
}
