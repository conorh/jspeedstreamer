package com.outertrack.jspeedstreamer;

import com.outertrack.jspeedstreamer.http.HttpRequest;
import com.outertrack.jspeedstreamer.http.HttpResponse;
import com.outertrack.jspeedstreamer.utils.CircularDownloadBuffer;
import com.outertrack.jspeedstreamer.utils.MultiLogger;

import java.io.*;
import java.net.SocketTimeoutException;

/**
 * This is a single thread that downloads blocks of data from an HTTP stream.
 * 
 * @author conorhunt
 * 
 */
public class DownloadThread extends Thread
{
    private static MultiLogger log = MultiLogger.getLogger(DownloadThread.class);    
    
    HttpRequest request = null;
    CircularDownloadBuffer buffer = null;
    ProxyThread manager = null;
    
    long currentPosition = 0;
    long startPosition = 0;
    long endPosition = 0;

    /***
     * 
     * @param request HTTP Request to execute
     * @param buffer Buffer to write results of HTTP request to
     * @param manager ProxyThread manager to be notified of status changes
     */
    public DownloadThread(HttpRequest request, CircularDownloadBuffer buffer, ProxyThread manager)
    {
        this.request = request;
        this.buffer = buffer;
        this.manager = manager;
    }

    /**
     * Set the block of data in the stream that this download thread should download
     * 
     * @param startPosition position in the stream of this block
     * @param blockSize size of the block to download
     */
    public void setBlock(long startPosition, long endPosition)
    {
        this.startPosition = startPosition;
        this.currentPosition = startPosition;
        this.endPosition = endPosition;
    }

    public void run()
    {
        try
        {
            byte[] byteBuf = new byte[20000];
            boolean tryAgain = false;
            // Keep going while the manager thread says so
            while (manager.notifyThreadReady(this, (int) (currentPosition - startPosition)))
            {
                do
                {
                    try
                    {
                        tryAgain = false;
                        lastMeasuredPosition = currentPosition;

                        // log.debug("Dl thread start: " + currentPosition + " -> " + (endPosition));
                        // Execute the Http request to get the block of data that the manager thread told this thread to download.
                        HttpResponse response = request.execute(currentPosition, endPosition - 1, 1000);

                        InputStream in = response.getInputStream();
                        int bytesRead = -2;
                        while (currentPosition < endPosition && !manager.isFinished() && (bytesRead = in.read(byteBuf)) >= 0)
                        {
                            buffer.write(byteBuf, bytesRead, currentPosition);
                            currentPosition += bytesRead;
                        }
                        response.close();
                    }
                    catch(SocketTimeoutException e)
                    {
                        log.debug("Dl thread trying again after timeout startPosition: " + startPosition + " currentPosition: " + currentPosition);                        
                        tryAgain = true;
                    }                
                } while(tryAgain);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    private long lastMeasuredPosition = startPosition;
    private long lastMeasuredTime = System.currentTimeMillis();
    /**
     * Calculates the speed of the download of this thread in bytes per second.
     * @return
     */
    public int getSpeed() 
    {
        long currentData = currentPosition;
        long currentTime = System.currentTimeMillis();
        long dataTransferred = currentData - lastMeasuredPosition;
        long timeTaken = currentTime - lastMeasuredTime;
        lastMeasuredTime = currentTime;
        lastMeasuredPosition = currentData;
        
        if(timeTaken > 0)
          return (int) (dataTransferred / (timeTaken / 1000));
        else
          return 0;
    }
    
    public long getCurrentPosition()
    {
        return currentPosition;
    }
    
    public long getEndPosition()
    {
        return endPosition;
    }
    
    public long getStartPosition()
    {
        return startPosition;
    }
}
