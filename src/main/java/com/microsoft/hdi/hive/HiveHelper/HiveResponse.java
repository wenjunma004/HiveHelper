package com.microsoft.hdi.hive.HiveHelper;

public class HiveResponse {
    int code;
    String msg;

    public HiveResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "HiveResponse{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                '}';
    }
}
