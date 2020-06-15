package com.wocloud.utils

import java.io.File
import com.typesafe.config.{ConfigFactory}


object PropsUtils {

    /**
     * 线上环境使用
     * @param name
     * @return
     */
    def loadProps(name:String):String={
        val config = ConfigFactory.parseFile(new File("application.properties"))
        config.getString(name)
    }

    /**
     * 本地测试使用
     * @param name
     * @return
     */
    def loadLocalProps(name:String):String={
        val config = ConfigFactory.load
        config.getString(name)
    }
}
