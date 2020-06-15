package com.wocloud.handler

import org.apache.spark.streaming.dstream.DStream
import com.wocloud.consts.TableNameInfo._
import org.apache.spark.sql.SparkSession
import com.wocloud.consts.ResourceLogoInfo._
import com.wocloud.utils.TimeUtils.getCurrentFiveMin
import org.apache.spark.sql.functions._
import com.wocloud.config.MongodbConfig._

object VpeMessageHandler {

    val spark = SparkSession.builder().getOrCreate()

    val fiveMiniuteUDF=udf((col:Double)=>getCurrentFiveMin(col.toString))

    val concatVpeUDF=udf((uuid:String,dateTime:Long)=>uuid+"_"+dateTime)
    val concatVpePortFlowUDF=udf((uuid:String,ifName:String,dataTime:String)=>uuid+"_"+ifName+"_"+dataTime)
    val concatVpeFlowUDF=udf((uuid:String,peer:String,dataTime:String)=>uuid+"_"+peer+"_"+dataTime)
    val concatVpeDelayUDF=udf((uuid:String,category:String,dataTime:String)=>uuid+"_"+category+"_"+dataTime)

    def handleVpeMonitor(vpeMonitorData:DStream[String]): Unit = {
        vpeMonitorData.foreachRDD(rdd=> {
            import spark.implicits._

            val vpeData = rdd.filter(_.contains(resourceLogo_vpe))
            val vpeDiskData = rdd.filter(_.contains(resourceLogo_vpe_disk))
            val vpeConnectionData = rdd.filter(_.contains(resourceLogo_vpe_connection))
            val vpeFlowData = rdd.filter(_.contains(resourceLogo_vpePort))

            val dfVpe = spark.read.json(spark.createDataset(vpeData))
            val dfVpeDisk = spark.read.json(spark.createDataset(vpeDiskData))
            val dfVpeConnection = spark.read.json(spark.createDataset(vpeConnectionData))
            val dfVpeFlow = spark.read.json(spark.createDataset(vpeFlowData))

            if (!dfVpe.isEmpty) {
                val selectedVpe = dfVpe.select("resourceUuid","resourceLogo","collectdate","fabric","host","cpu","memUsage")
                val aggregatedVpe = selectedVpe
                                    .groupBy("resourceUuid","resourceLogo","fabric","host")
                                    .agg(
                                        round(avg("cpu")*100, 2).as("cpu"),
                                        round(avg("memUsage"), 2).as("memUsedRate"),
                                        avg("collectdate").as("collectdate")
                                    )

                //历史数据
                val vpe = aggregatedVpe
                            .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                            .withColumn("dealTime", lit(getCurrentFiveMin()))
                            .withColumn("_id", concatVpeUDF(col("resourceUuid"), col("dealTime")))
                            .withColumn("timeType", lit("5"))
                            .drop("collectdate")

                vpe.show(false)
                saveToMongodb(vpe, vpe_basic)

            }

            if (!dfVpeDisk.isEmpty) {
                val selectedVpeDisk = dfVpeDisk.select("resourceUuid","resourceLogo","collectdate","fabric","host","inodesFree","inodesTotal","free","total","used","usedPercent")
                val aggregatedVpeDisk = selectedVpeDisk
                                        .groupBy("resourceUuid","resourceLogo","fabric","host")
                                        .agg(
                                            round(avg("usedPercent"),2).as("usedPercent"),
                                            round(avg("free")/1024/1024,2).as("diskAvalible"),
                                            round(avg("total")/1024/1024,2).as("diskTotal"),
                                            floor(avg("inodesTotal")).as("inodesTotal"),
                                            floor(avg("inodesFree")).as("inodesFree"),
                                            avg("collectdate").as("collectdate")
                                        )

                //历史数据
                val vpeDisk = aggregatedVpeDisk
                                .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                .withColumn("dealTime", lit(getCurrentFiveMin()))
                                .withColumn("_id", concatVpeUDF(col("resourceUuid"),col("dealTime")))
                                .withColumn("timeType", lit("5"))
                                .drop("collectdate")

                vpeDisk.show(false)
                saveToMongodb(vpeDisk,vpe_disk)

            }

            if (!dfVpeConnection.isEmpty) {
                val selectedVpeConnection = dfVpeConnection.select("resourceUuid","resourceLogo","collectdate","fabric","host","connections")
                val aggregatedVpeConnection = selectedVpeConnection
                                                .groupBy("resourceUuid","resourceLogo","fabric","host")
                                                .agg(
                                                    floor(avg("connections")).as("connections"),
                                                    avg("collectdate").as("collectdate")
                                                )

                //历史数据
                val vpeConnection = aggregatedVpeConnection
                                    .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                    .withColumn("dealTime", lit(getCurrentFiveMin()))
                                    .withColumn("_id", concatVpeUDF(col("resourceUuid"),col("dealTime")))
                                    .withColumn("timeType", lit("5"))
                                    .drop("collectdate")

                vpeConnection.show(false)
                saveToMongodb(vpeConnection,vpe_connection)

            }

            if (!dfVpeFlow.isEmpty) {
                val selectedVpeFlow = dfVpeFlow.select("resourceUuid","resourceLogo","collectdate","fabric","host","ifName","rxBytes","rxDrops","rxErrs","rxPkts","txBytes","txDrops","txErrs","txPkts")
                val aggregatedVpeFlow = selectedVpeFlow
                                        .groupBy("resourceUuid","resourceLogo","fabric","host","ifName")
                                        .agg(
                                            (max("rxBytes")-min("rxBytes")).as("rxTotal"),
                                            (max("txBytes")-min("txBytes")).as("txTotal"),
                                            (max("rxPkts")-min("rxPkts")).as("rxPackage"),
                                            (max("txPkts")-min("txPkts")).as("txPackage"),
                                            (max("collectdate")-min("collectdate")).as("duration"),
                                            avg("collectdate").as("collectdate")
                                        )

                //历史数据
                val vpeFlow = aggregatedVpeFlow
                                .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                .withColumn("dealTime", lit(getCurrentFiveMin()))
                                .withColumn("_id", concatVpePortFlowUDF(col("resourceUuid"),col("ifName"),col("dealTime")))
                                .withColumn("timeType", lit("5"))
                                .withColumn("rxBytes",round(col("rxTotal")/col("duration")*8,2))
                                .withColumn("txBytes",round(col("txTotal")/col("duration")*8,2))
                                .drop("duration")
                                .drop("collectdate")


                vpeFlow.show(false)
                saveToMongodb(vpeFlow,vpe_interface)
            }

            if (!dfVpeFlow.isEmpty) {
                val selectedVpeFlow = dfVpeFlow.select("resourceUuid","resourceLogo","collectdate","fabric","host","peer","rxBytes","rxDrops","rxErrs","rxPkts","txBytes","txDrops","txErrs","txPkts")
                val aggregatedVpeFlow = selectedVpeFlow
                                            .groupBy("resourceUuid","resourceLogo","fabric","host","peer")
                                            .agg(
                                                (max("rxBytes")-min("rxBytes")).as("rxTotal"),
                                                (max("txBytes")-min("txBytes")).as("txTotal"),
                                                (max("rxPkts")-min("rxPkts")).as("rxPackage"),
                                                (max("txPkts")-min("txPkts")).as("txPackage"),
                                                (max("collectdate")-min("collectdate")).as("duration"),
                                                avg("collectdate").as("collectdate")
                                            )

                //历史数据
                val vpeFlow = aggregatedVpeFlow
                                    .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                    .withColumn("dealTime", lit(getCurrentFiveMin()))
                                    .withColumn("_id", concatVpeFlowUDF(col("resourceUuid"),col("peer"),col("dealTime")))
                                    .withColumn("timeType", lit("5"))
                                    .withColumn("rxBytes",round(col("rxTotal")/col("duration")*8,2))
                                    .withColumn("txBytes",round(col("txTotal")/col("duration")*8,2))
                                    .drop("duration")
                                    .drop("collectdate")


                vpeFlow.show(false)
                saveToMongodb(vpeFlow,vpe_peer_flow)
            }

        })
    }

    def handleVpeDelayMonitor(vpeDelayMonitorData:DStream[String]): Unit = {
        vpeDelayMonitorData.foreachRDD(rdd=> {
            import spark.implicits._
            val dfVpeDelay = spark.read.json(spark.createDataset(rdd))

            if (!dfVpeDelay.isEmpty) {
                val selectedVpeDelay = dfVpeDelay.select("resourceUuid","resourceLogo","collectdate","fabric","category","src","srcName","dst","dstName","delay","jitter","loss")
                val aggregatedVpeDelay = selectedVpeDelay
                                                .groupBy("resourceUuid","resourceLogo","fabric","category","src","srcName","dst","dstName")
                                                .agg(
                                                    round(avg("loss")*100, 2).as("loss"),
                                                    round(avg("delay"), 2).as("delay"),
                                                    round(avg("jitter"), 2).as("jitter"),
                                                    avg("collectdate").as("collectdate")
                                                )

                //历史数据
                val vpeDelay = aggregatedVpeDelay
                                .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                                .withColumn("dealTime", lit(getCurrentFiveMin()))
                                .withColumn("_id", concatVpeDelayUDF(col("resourceUuid"),col("category"),col("dealTime")))
                                .withColumn("timeType", lit("5"))
                                .drop("collectdate")

                vpeDelay.show(false)
                saveToMongodb(vpeDelay,vpe_delay)

            }
        })
    }

}
