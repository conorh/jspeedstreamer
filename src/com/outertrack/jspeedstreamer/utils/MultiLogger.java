package com.outertrack.jspeedstreamer.utils;

public class MultiLogger
{
    private static MultiLogger logger = new MultiLogger();

    private MultiLogger()
    {}

    public static MultiLogger getLogger(Class objClass)
    {
        return logger;
    }

    public void debug(String message)
    {
        System.out.println(Thread.currentThread().getName() + " - " + message);
    }

    public void info(String message)
    {
        System.out.println(Thread.currentThread().getName() + " - " + message);
    }
}