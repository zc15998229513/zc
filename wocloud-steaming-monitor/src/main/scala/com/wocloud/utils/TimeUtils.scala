package com.wocloud.utils

import java.lang.Float
import java.util.{Calendar, Date}


object TimeUtils {

    def getCurrentFiveMin(timestamp: String): Long = {
        val ts = Float.parseFloat(timestamp).toLong * 1000L
        val time = new Date(ts)
        val calendar = Calendar.getInstance
        calendar.setTime(time)
        val mins = calendar.get(Calendar.MINUTE)
        calendar.set(Calendar.MINUTE, mins / 5 * 5)
        calendar.set(Calendar.SECOND, 0)
        //    calendar.add(Calendar.MINUTE, 5)
        val end = calendar.getTime.getTime
        end
    }

    def getCurrentFiveMin(): Long = {
        val timestamp=new Date().getTime/1000
        val ts = timestamp * 1000L
        val time = new Date(ts)
        val calendar = Calendar.getInstance
        calendar.setTime(time)
        val mins = calendar.get(Calendar.MINUTE)
        calendar.set(Calendar.MINUTE, mins / 5 * 5)
        calendar.set(Calendar.SECOND, 0)
        //    calendar.add(Calendar.MINUTE, 5)
        val start = calendar.getTime.getTime
        start
    }

    def getCurrentMin(timestamp: String): Long = {
        val ts = Float.parseFloat(timestamp).toLong * 1000L
        val time = new Date(ts)
        val calendar = Calendar.getInstance
        calendar.setTime(time)
        val mins = calendar.get(Calendar.MINUTE)
        calendar.set(Calendar.MINUTE, mins)
        calendar.set(Calendar.SECOND, 0)
        val end = calendar.getTime.getTime
        end
    }
}