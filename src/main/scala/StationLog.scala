import java.util.Objects

/**
  * @author cc
  * @create 2020-09-01-16:22
  */
class StationLog {
  var sid: String = null
  var callOut: String = null
  var callIn: String = null
  var callType: String = null
  var callTime = 0L
  var duration = 0L

  def getSid: String = sid

  def setSid(sid: String): Unit = {
    this.sid = sid
  }

  def getCallOut: String = callOut

  def setCallOut(callOut: String): Unit = {
    this.callOut = callOut
  }

  def getCallIn: String = callIn

  def setCallIn(callIn: String): Unit = {
    this.callIn = callIn
  }

  def getCallType: String = callType

  def setCallType(callType: String): Unit = {
    this.callType = callType
  }

  def getCallTime: Long = callTime

  def setCallTime(callTime: Long): Unit = {
    this.callTime = callTime
  }

  def getDuration: Long = duration

  def setDuration(duration: Long): Unit = {
    this.duration = duration
  }


  def this(sid: String, duration: Long) {
    this()
    this.sid = sid
    this.duration = duration
  }

  def this(sid: String, callOut: String, callIn: String, callType: String, callTime: Long, duration: Long) {
    this()
    this.sid = sid
    this.callOut = callOut
    this.callIn = callIn
    this.callType = callType
    this.callTime = callTime
    this.duration = duration
  }

  override def toString: String = "StationLog{" + sid + ',' + callOut + ',' + callIn + ',' + callType + ',' + callTime + ',' + duration + '}'

  override def equals(o: Any): Boolean = {
    if (this eq o) return true
    if (o == null || (getClass ne o.getClass)) return false
    val that = o.asInstanceOf[StationLog]
    Objects.equals(sid, that.sid) && Objects.equals(callOut, that.callOut) && Objects.equals(callIn, that.callIn) && Objects.equals(callType, that.callType) && Objects.equals(callTime, that.callTime) && Objects.equals(duration, that.duration)
  }

  override def hashCode: Int = Objects.hash(sid, callOut, callIn, callType, callTime, duration)


}
