package com.gfyulx.DI.schedule.service;

public class SLAException extends Exception{
    public SLAException(String msg)
    {
        super(msg);
    }

    public SLAException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
