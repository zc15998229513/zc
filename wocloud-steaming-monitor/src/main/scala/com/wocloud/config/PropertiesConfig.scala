package com.wocloud.config

import com.wocloud.utils.PropsUtils

object PropertiesConfig {
    //kafka参数
    val kafkaServers = PropsUtils.loadLocalProps("bootstrap.servers")
    val offsetReset = PropsUtils.loadLocalProps("auto.offset.reset")

    val cpeID=PropsUtils.loadLocalProps("group.cpe")
    val vpeID=PropsUtils.loadLocalProps("group.vpe")
    val ceID=PropsUtils.loadLocalProps("group.ce")

    val yunchang_cloudNetwork_cpe = PropsUtils.loadLocalProps("cpe")
    val yunchang_cloudNetwork_vpe = PropsUtils.loadLocalProps("vpe")
    val yunchang_cloudNetwork_sdwan = PropsUtils.loadLocalProps("sdwan")
    val yunchang_cloudNetwork_ce = PropsUtils.loadLocalProps("switch")

    //mongodb参数
    val mongodb_output=PropsUtils.loadLocalProps("mongodb_output_uri")
}
