package com.wocloud.handler

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import com.wocloud.consts.ResourceLogoInfo._
import org.apache.spark.sql.functions._
import com.wocloud.utils.TimeUtils._
import com.wocloud.config.MongodbConfig.saveToMongodb
import com.wocloud.consts.TableNameInfo._

/**
 * @Author : ZC
 * @Description :
 * @Date : 2020/6/11
 * @Version :
 */
object CeMessageHandler {

    val spark = SparkSession.builder().getOrCreate()
    val fiveMiniuteUDF=udf((col:Double)=>getCurrentFiveMin(col.toString))
    val concatCeUDF=udf((uuid:String,dateTime:Long)=>uuid+"_"+dateTime)
    val concatCePortUDF=udf((uuid:String,portUuid:String,dateTime:Long)=>uuid+"_"+portUuid+"_"+dateTime)

    def handleCEMonitor(ceMonitorData: DStream[String]): Unit = {
        ceMonitorData.foreachRDD(rdd => {
            import spark.implicits._
            val ceData = rdd.filter(_.contains(resourceLogo_switch))
            val cePortData = rdd.filter(_.contains(resourceLogo_switch_port))

            val dfCe = spark.read.json(spark.createDataset(ceData))
            val dfCePort = spark.read.json(spark.createDataset(cePortData))
            dfCe.selectExpr()
            if (!dfCe.isEmpty) {
                val selectedCe = dfCe.select("switchUuid","resourceLogo","collectdate","fabric","cpuUsage","memUsage","memUse")
                val aggregatedCe = selectedCe
                                    .groupBy("switchUuid","resourceLogo","fabric")
                                    .agg(
                                        round(avg("cpuUsage"), 2).as("cpu"),
                                        round(avg("memUsage")*100, 2).as("memUsedRate"),
                                        round(avg("memUse"),2).as("memUse"),
                                        avg("collectdate").as("collectdate")
                                    )

                //历史数据
                val ce = aggregatedCe
                        .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                        .withColumn("dealTime", lit(getCurrentFiveMin()))
                        .withColumn("_id", concatCeUDF(col("switchUuid"),col("dealTime")))
                        .withColumn("timeType", lit("5"))
                        .drop("collectdate")

                ce.show(false)
                saveToMongodb(ce,ce_basic)
            }

            if (!dfCePort.isEmpty) {
                val selectedCePort = dfCePort.select("switchUuid","resourceLogo","collectdate","fabric","host","deviceType","portUuid","inBroadcastPkg","inDiscardPkg","inErrorPkg","inPkg","inTraffic","inUcastPkg","inUtilization","outBroadcastPkg","outDiscardPkg","outErrorPkg","outPkg","outTraffic","outUcastPkg","outUtilization")
                val aggregatedCePort = selectedCePort
                                                .groupBy("switchUuid","resourceLogo","fabric","host","deviceType","portUuid")
                                                .agg(
                                                    (max("inTraffic")-min("inTraffic")).as("inTrafficTotal"),
                                                    (max("outTraffic")-min("outTraffic")).as("outTrafficTotal"),
                                                    (max("collectdate")-min("collectdate")).as("duration"),
                                                    round(avg("inUtilization"),2).as("inUtilization"),
                                                    round(avg("outUtilization"),2).as("outUtilization"),
                                                    (max("inBroadcastPkg")-min("inBroadcastPkg")).as("inBroadcastPkg"),
                                                    (max("inDiscardPkg")-min("inDiscardPkg")).as("inDiscardPkg"),
                                                    (max("inErrorPkg")-min("inErrorPkg")).as("inErrorPkg"),
                                                    (max("inPkg")-min("inPkg")).as("inPkg"),
                                                    (max("inUcastPkg")-min("inUcastPkg")).as("inUcastPkg"),
                                                    (max("outBroadcastPkg")-min("outBroadcastPkg")).as("outBroadcastPkg"),
                                                    (max("outDiscardPkg")-min("outDiscardPkg")).as("outDiscardPkg"),
                                                    (max("outErrorPkg")-min("outErrorPkg")).as("outErrorPkg"),
                                                    (max("outPkg")-min("outPkg")).as("outPkg"),
                                                    (max("outUcastPkg")-min("outUcastPkg")).as("outUcastPkg"),
                                                    avg("collectdate").as("collectdate")
                                                )

                //历史数据
                val cePort = aggregatedCePort
                            .withColumn("timestamp", fiveMiniuteUDF(col("collectdate")))
                            .withColumn("dealTime", lit(getCurrentFiveMin()))
                            .withColumn("inTraffic",round(col("inTrafficTotal")/col("duration")*8,2))
                            .withColumn("outTraffic",round(col("outTrafficTotal")/col("duration")*8,2))
                            .withColumn("_id", concatCePortUDF(col("switchUuid"),col("portUuid"),col("dealTime")))
                            .withColumn("timeType", lit("5"))
                            .drop("inTrafficTotal")
                            .drop("outTrafficTotal")
                            .drop("duration")
                            .drop("collectdate")

                cePort.show(false)
                saveToMongodb(cePort,ce_port)

            }

        })
    }
}