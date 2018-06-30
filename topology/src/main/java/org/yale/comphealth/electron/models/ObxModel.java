package org.yale.comphealth.electron.models;

import com.google.gson.annotations.SerializedName;

/**
 * Created by Wade Schulz on 12/23/2015.
 */
public class ObxModel {
    // OBX_LINE_COUNTER
    @SerializedName("line")
    public String LineCounter;

    // OBX_CHANNEL
    @SerializedName("channel")
    public String Channel;

    // OBX_TEXT
    @SerializedName("text")
    public String Text;
}
