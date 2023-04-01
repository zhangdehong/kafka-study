package com.hong.kafka.entity;

/**
 * @Author: ZhangDeHong
 * @Describe: TODO
 * @Date Create in  9:07 下午 2020/7/20
 */
public class Channel {

    private String sChannelId;
    private String sHostId;
    private String sLibraryId;

    public String getsChannelId () {
        return sChannelId;
    }

    public void setsChannelId (String sChannelId) {
        this.sChannelId = sChannelId;
    }

    public String getsHostId () {
        return sHostId;
    }

    public void setsHostId (String sHostId) {
        this.sHostId = sHostId;
    }

    public String getsLibraryId () {
        return sLibraryId;
    }

    public void setsLibraryId (String sLibraryId) {
        this.sLibraryId = sLibraryId;
    }
}
