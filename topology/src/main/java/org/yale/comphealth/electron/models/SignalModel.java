package org.yale.comphealth.electron.models;

import com.google.gson.annotations.SerializedName;

import java.util.Date;

/**
 * Created by Wade Schulz on 12/23/2015.
 */
public class SignalModel {
    // MSH_DATE_TIME
    @SerializedName("msh_ts")
    public Date MshTimestamp;

    // OBR_HL7_START_DATE
    @SerializedName("start_ts")
    public Date StartTimestamp;

    // OBR_UNIT
    @SerializedName("unit")
    public String Unit;

    // OBR_BED
    @SerializedName("bed")
    public String Bed;

    // OBR_SOURCE
    @SerializedName("src")
    public String Source;

    @SerializedName("obx_objects")
    public ObxModel[] ObxObjects;

    @SerializedName("hl7")
    public String Hl7;

    @SerializedName("msg_id")
    public String MessageId;

    @SerializedName("helix_rcv_ts")
    public Date HelixReceivedTimestamp;

    @SerializedName("tz_offset")
    public int TimezoneOffset;
}
