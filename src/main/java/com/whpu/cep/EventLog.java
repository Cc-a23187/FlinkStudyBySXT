package com.whpu.cep;

/**
 * @author cc
 * @create 2020-08-26-17:21
 */
public class EventLog {

    private Long id;
    private String userName;
    private String eventType;
    private Long eventTime;

    public EventLog(Long id, String userName, String eventType, Long eventTime) {
        this.id = id;
        this.userName = userName;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "EventLog{" +
                "id=" + id +
                ", userName='" + userName + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
