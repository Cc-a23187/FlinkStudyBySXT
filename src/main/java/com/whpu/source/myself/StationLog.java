package com.whpu.source.myself;

import java.util.Objects;

/**
 * @author cc
 * @create 2020-07-29-16:23
 * 主要的方法，启动一个source，并且从source中返回数据
 * 如果run方法停止，则数据流终止
 */

public class StationLog{
    public String sid;
    public String callOut;
    public String callIn;
    public String callType;
    public Long callTime;
    public Long duration;

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public  String getCallOut() {
        return callOut;
    }

    public void setCallOut(String callOut) {
        this.callOut = callOut;
    }

    public String getCallIn() {
        return callIn;
    }

    public void setCallIn(String callIn) {
        this.callIn = callIn;
    }

    public String getCallType() {
        return callType;
    }

    public void setCallType(String callType) {
        this.callType = callType;
    }

    public Long getCallTime() {
        return callTime;
    }

    public void setCallTime(Long callTime) {
        this.callTime = callTime;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public StationLog() {
    }

    public StationLog(String sid, Long duration) {
        this.sid = sid;
        this.duration = duration;
    }

    public StationLog(String sid, String callOut, String callIn, String callType, Long callTime, Long duration) {
        this.sid = sid;
        this.callOut = callOut;
        this.callIn = callIn;
        this.callType = callType;
        this.callTime = callTime;
        this.duration = duration;
    }

    @Override
    public String toString() {
        return "StationLog{" +
                 sid + ',' +
                 callOut + ',' +
                callIn + ',' +
                callType + ',' +
                callTime +','+
                 duration +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StationLog that = (StationLog) o;
        return Objects.equals(sid, that.sid) &&
                Objects.equals(callOut, that.callOut) &&
                Objects.equals(callIn, that.callIn) &&
                Objects.equals(callType, that.callType) &&
                Objects.equals(callTime, that.callTime) &&
                Objects.equals(duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sid, callOut, callIn, callType, callTime, duration);
    }
}
