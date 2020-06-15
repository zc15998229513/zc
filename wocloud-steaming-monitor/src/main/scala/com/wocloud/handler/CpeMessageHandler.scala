package com.wocloud.handler

import com.wocloud.config.MongodbConfig.saveToMongodb
import com.wocloud.consts.FieldFilterInfo._
import com.wocloud.consts.ResourceLogoInfo._
import com.wocloud.consts.TableNameInfo._
import com.wocloud.utils.TimeUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.DStream

object CpeMessageHandler {

    val spark = SparkSession.builder().getOrCreate()

    val fiveMiniuteUDF=udf((col:Double)=>getCurrentFiveMin(col.toString))

    val concatCpeUDF=udf((uuid:String,dateTime:Long)=>uuid+"_"+dateTime)
    val concatCpePortUDF=udf((uuid:String,ifName:String,dateTime:Long)=>uuid+"_"+ifName+"_"+dateTime)

    def handleCpeMonitor(cpeMonitorData:DStream[String]): Unit = {
        cpeMonitorData.foreachRDD(rdd=>{
            import spark.implicits._

            val cpeData = rdd.filter(_.contains(resourceLogo_cpe))
            val cpePopDelayData = rdd.filter(_.contains(resourceLogo_pop_delay))
            val cepTunStatsData = rdd.filter(_.contains(resourceLogo_tun_stats))
            val cpePortData = rdd.filter(_.contains(resourceLogo_port))

            val dfCpe=spark.read.json(spark.createDataset(cpeData))
            val dfcpePopDelay = spark.read.json(spark.createDataset(cpePopDelayData))
            val dfcpeTunStats = spark.read.json(spark.createDataset(cepTunStatsData))
            val dfcpePort = spark.read.json(spark.createDataset(cpePortData))

            if(!dfCpe.isEmpty) {
                val selectedCpe = dfCpe.select("resourceUuid","resourceLogo","collectdate","fabric","host","cpu","memUsage","diskFree","diskTotal","connections")
                                        .withColumn("diskInUse", col("diskTotal") - col("diskFree"))
                                        .withColumn("diskUsage", col("diskInUse") / col("diskTotal"))

                val aggregatedCpe = selectedCpe
                                    .groupBy("resourceUuid","resourceLogo","fabric","host")
                                    .agg(
                                        round(avg("cpu").*(100),2).as("cpuUsedRate"),
                                        round(avg("memUsage"),2).as("memUsedRate"),
                                        round(avg("diskUsage")*100,2).as("diskUsedRate"),
                                        round(avg("diskFree")/1024/1024,2).as("diskAvalible"),
                                        round(avg("connections"),2).as("cpeConnections"),
                                        avg("collectdate").as("collectdate")
                                    )

                //历史数据
                val cpe = aggregatedCpe
                            .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                            .withColumn("dealTime", lit(getCurrentFiveMin()))
                            .withColumn("_id", concatCpeUDF(col("resourceUuid"), col("dealTime")))
                            .withColumn("timeType", lit("5"))
                            .drop("collectdate")

                cpe.show(false)
                saveToMongodb(cpe, cpe_basic)

            }

            if(!dfcpePopDelay.isEmpty) {
                val selectedCpePopDelay = dfcpePopDelay.select("resourceUuid","resourceLogo","collectdate","cpe","fabric","host","jitter","loss","rtt","jitterIpsec","lossIpsec","rttIpsec")

                val aggregatedCpePopDelay = selectedCpePopDelay
                                            .groupBy("resourceUuid","resourceLogo","cpe","fabric","host")
                                            .agg(
                                                round(avg("rtt"),2).as("rtt"),
                                                round(avg("rttIpsec"),2).as("rttIpsec"),
                                                round(avg("loss")*100,2).as("loss"),
                                                round(avg("lossIpsec")*100,2).as("lossIpsec"),
                                                round(avg("jitter"),2).as("jitter"),
                                                round(avg("jitterIpsec"),2).as("jitterIpsec"),
                                                avg("collectdate").as("collectdate")
                                            )

                //历史数据
                val CpePopDelay = aggregatedCpePopDelay
                                    .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                    .withColumn("dealTime", lit(getCurrentFiveMin()))
                                    .withColumn("_id", concatCpeUDF(col("resourceUuid"),col("dealTime")))
                                    .withColumn("timeType", lit("5"))
                                    .drop("collectdate")

                CpePopDelay.show(false)
                saveToMongodb(CpePopDelay, cpe_chain)

            }

            if(!dfcpeTunStats.isEmpty) {
                val selectedCpeTunStats = dfcpeTunStats.select("resourceUuid","resourceLogo","collectdate","cpe","fabric","host","jitter","loss","rtt","rxByte","txByte")

                val aggregatedCpeTunStats = selectedCpeTunStats
                                            .groupBy("resourceUuid","resourceLogo","cpe","fabric","host")
                                            .agg(
                                                round(avg("jitter"),2).as("jitter"),
                                                round(avg("rtt"),2).as("rtt"),
                                                round(avg("loss")*100,2).as("loss"),
                                                (max("rxByte")-min("rxByte")).as("rxByteDuration"),
                                                (max("txByte")-min("txByte")).as("txByteDuration"),
                                                (max("collectdate")-min("collectdate")).as("duration"),
                                                avg("collectdate").as("collectdate")
                                            )

                //历史数据
                val CpeTunStats = aggregatedCpeTunStats
                                    .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                    .withColumn("dealTime", lit(getCurrentFiveMin()))
                                    .withColumn("_id", concatCpeUDF(col("resourceUuid"),col("dealTime")))
                                    .withColumn("timeType", lit("5"))
                                    .withColumn("rxBytes",round(col("rxByteDuration")/col("duration")*8,2))
                                    .withColumn("txBytes",round(col("txByteDuration")/col("duration")*8,2))
                                    .drop("duration")
                                    .drop("rxByteDuration")
                                    .drop("txByteDuration")
                                    .drop("collectdate")

                CpeTunStats.show(false)
                saveToMongodb(CpeTunStats, cpe_tunnel)

            }

            if(!dfcpePort.isEmpty) {
                val selectedCpePort = dfcpePort.select("resourceUuid","resourceLogo","collectdate","fabric","host","ifName","monIfKey","rxBytes","rxDrops","rxErrs","rxPkts","txBytes","txDrops","txErrs","txPkts")

                val aggregatedCpePort = selectedCpePort
                                        .groupBy("resourceUuid","resourceLogo","fabric","host","ifName","monIfKey")
                                        .agg(
                                            (max("rxBytes")-min("rxBytes")).as("rxByteDuration"),
                                            (max("txBytes")-min("txBytes")).as("txByteDuration"),
                                            (max("rxDrops")-min("rxDrops")).as("rxDrops"),
                                            (max("txDrops")-min("txDrops")).as("txDrops"),
                                            (max("rxErrs")-min("rxErrs")).as("rxErrs"),
                                            (max("txErrs")-min("txErrs")).as("txErrs"),
                                            (max("rxPkts")-min("rxPkts")).as("rxPkts"),
                                            (max("txPkts")-min("txPkts")).as("txPkts"),
                                            (max("collectdate")-min("collectdate")).as("duration"),
                                            avg("collectdate").as("collectdate")
                                        )

                //历史数据
                val CpePort = aggregatedCpePort
                                .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                .withColumn("dealTime", lit(getCurrentFiveMin()))
                                .withColumn("_id", concatCpePortUDF(col("resourceUuid"),col("ifName"),col("dealTime")))
                                .withColumn("timeType", lit("5"))
                                .withColumn("rxBytes",round(col("rxByteDuration")/col("duration")*8,2))
                                .withColumn("txBytes",round(col("txByteDuration")/col("duration")*8,2))
                                .drop("duration")
                                .drop("rxByteDuration")
                                .drop("txByteDuration")
                                .drop("collectdate")
                CpePort.show(false)
                saveToMongodb(CpePort, cpe_interface)

            }

        })
    }

    def handleCpe2VpeMonitor(cpe2VpeMonitorData:DStream[String]): Unit = {

        cpe2VpeMonitorData.foreachRDD(rdd=> {
            import spark.implicits._
            val cpe2VpeData = rdd.filter(_.contains(resourceLogo_vpePort)).filter(_.contains(is_cpe))

            val dfCpe2Vpe = spark.read.json(spark.createDataset(cpe2VpeData))

            if (!dfCpe2Vpe.isEmpty) {
                val selectedCpe2Vpe = dfCpe2Vpe.select("peer","resourceLogo","fabric","collectdate","rxBytes","txBytes")
                val aggregatedCpe2Vpe = selectedCpe2Vpe
                                        .groupBy("peer","resourceLogo","fabric")
                                        .agg(
                                            (max("rxBytes")-min("rxBytes")).as("rxTotal"),
                                            (max("txBytes")-min("txBytes")).as("txTotal"),
                                            (max("collectdate")-min("collectdate")).as("duration"),
                                            avg("collectdate").as("collectdate")
                                        )
                //历史数据
                val Cpe2Vpe = aggregatedCpe2Vpe
                                .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                .withColumn("dealTime", lit(getCurrentFiveMin()))
                                .withColumn("_id", concatCpeUDF(col("peer"),col("dealTime")))
                                .withColumn("timeType", lit("5"))
                                .withColumn("rxBytes",round(col("rxTotal")/col("duration")*8,2))
                                .withColumn("txBytes",round(col("txTotal")/col("duration")*8,2))
                                .drop("duration")
                                .drop("collectdate")


                Cpe2Vpe.show(false)
                saveToMongodb(Cpe2Vpe, cpe_pop)

            }
        })
    }

}
