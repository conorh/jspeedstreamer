package com.outertrack.jspeedstreamer;

import com.outertrack.jspeedstreamer.http.*;
import com.outertrack.jspeedstreamer.utils.CircularDownloadBuffer;
import com.outertrack.jspeedstreamer.utils.MultiLogger;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;

/**
 * This is the 'manager' thread that manages multiple download threads downloading multiple chunks
 * of data at different positions from an HTTP stream and then writing that data to the proxy client
 * 
 * @author conorhunt
 * 
 */
public class ProxyThread extends Thread
{
    private static MultiLogger log = MultiLogger.getLogger(ProxyThread.class);
    
    // Socket of the requesting proxy client
    private Socket socket = null;
    
    // This is a notification flag that is set when the d/l is finished or something goes wrong
    private boolean downloadFinished = false;
       
    // Size of the content we are downloading
    private long contentLength = -1;
    private long bytesSent = 0;
    
    // Block size that each thread downloads
    private int maxBlockSize = 1000000;
    private int blockSize = 200000;
 
    // Threads are allocated blocks of data to download, this variable keeps track of the highest
    // byte position in the stream that has been allocated for download
    private long serverReadPosition = 0;    
    
    // Number of parallel threads to use
    private int downloadThreadCount = 4;
    DownloadThread downloadThreads[] = null;
 
    // The buffer gets allocated only when needed
    CircularDownloadBuffer buffer = null;
    private int bufferSize = 6000000;
    
    // Directory to write streamed output to
    private String outputDir = null;
       
    public ProxyThread(Socket s, HashMap options)
    {
        this.socket = s;
        
        Integer value = (Integer) options.get("MAXSEG");
        if(value != null)
          maxBlockSize = value.intValue();
        
        value = (Integer) options.get("MINSEG");
        if(value != null)
          blockSize = value.intValue();
        
        value = (Integer) options.get("THREADS");
        if(value != null)
          downloadThreadCount = value.intValue();
        
        value = (Integer) options.get("BUFFER");
        if(value != null)
          bufferSize = value.intValue();
        
        String stringOpt = (String) options.get("OUTDIR");
        if(stringOpt != null)
          outputDir = stringOpt;
    }

    public void run()
    {
        log.debug("Starting request");
        try
        {
            socket.setSoTimeout(5000);
            
            BufferedInputStream clientIn = new BufferedInputStream(socket.getInputStream());
            OutputStream clientOut = socket.getOutputStream();
            
            // Parse the request from the proxy client
            HttpRequest request = new HttpRequest(clientIn);
            
            // Send the request on to the server and get the response
            HttpResponse response = request.execute();
            
            // Write out the response headers to the proxy client
            clientOut.write(response.getResponseBytes());
            clientOut.flush();
            
            int responseCode = response.getResponseCode();
            contentLength = response.getContentLength();
            log.debug("response code = " + responseCode + " contentLength = " + contentLength);
            
            // Only do the multi-threaded download if it is a GET request, there is > 5megs of data and a 200 response (or 206 - partial content)
            if (request.getRequestType().equalsIgnoreCase("GET") && (responseCode == 200 || responseCode == 206) && contentLength > 5000000)
            {
                response.close();
                doDownload(request, contentLength, clientOut);
            }
            else if (!(responseCode >= 300 && responseCode < 400) && !request.getRequestType().equalsIgnoreCase("HEAD")) {
                // For all other requests just stream the rest of the data to the proxy client
                InputStream stream = response.getInputStream();
                BufferedOutputStream bufOut = new BufferedOutputStream(clientOut);
                for (int counter = 0, b=-1; (counter < contentLength || contentLength < 0) && (b = stream.read()) >= 0; counter++)
                {
                    bufOut.write(b);
                }
                bufOut.flush();
            }
            response.close();            
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                downloadFinished = true;
                socket.close();
            }
            catch (Exception e)
            {}
        }
        log.debug("Finished with request");
    }

    /**
     * This method does the multi threaded downloading
     * 
     * @param request
     * @param contentLength
     * @param clientOut
     */    
    OutputStream cout = null;
    public void doDownload(HttpRequest request, long contentLength, OutputStream clientOut)
    {
        cout = clientOut;
        SpeedThread speedThread = new SpeedThread(this);
        buffer = new CircularDownloadBuffer(bufferSize);

        // Also write the request out to a file
        BufferedOutputStream fileOut = null; 
        
        try
        {
            String outFile = request.getFileName();
            if(outputDir != null) {
                outFile = outputDir + File.separator + request.getFileName();
                fileOut = new BufferedOutputStream(new FileOutputStream(outFile));
            }
            
            downloadThreads = new DownloadThread[downloadThreadCount];
            for (int i = 0; i < downloadThreadCount; i++)
            {
                downloadThreads[i] = new DownloadThread(request, buffer, this);
                downloadThreads[i].start();
            }
            
            speedThread.start();
            
            // Whoa there boyo, give the server time to give us some data before we start sending it to the client
            Thread.sleep(2000);
            
            byte readBuffer[] = new byte[4096];
            int readLength = 4096;
            while (bytesSent < contentLength - 1 && !downloadFinished)
            {
                // For the last set of bytes that we download we need to make sure we don't try and read too much
                // from the circular buffer, otherwise it will block waiting for more data
                if(readLength + bytesSent >= contentLength)
                    readLength = (int) (contentLength - bytesSent - 1);
                
                int newBytes = buffer.read(readBuffer, readLength);
                clientOut.write(readBuffer, 0, newBytes);
                if(fileOut != null) {
                   fileOut.write(readBuffer, 0, newBytes);
                }
                bytesSent += newBytes;
            }
            clientOut.flush();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            downloadFinished = true;
            buffer.quit();
            if(fileOut != null)
              try { fileOut.close(); } catch(IOException e) {}
        }
        log.debug("Manager - finished sent: " + bytesSent);
    }
    
    private long lastTime = System.currentTimeMillis();
    private long lastBytesSent = 0;
    /**
     * This method outputs the current speed of the downloading client
     * and the speeds of each of the downloading threads.
     */
    private int readZeroBytes = 0;
    public void outputCurrentSpeeds()
      throws IOException
    {
        // Measure the current progress and output it to the console
        long currentTime = System.currentTimeMillis();        
        int clientReadSpeed = (int) (((bytesSent - lastBytesSent) / 1000) / ((currentTime - lastTime) / 1000));
        if(lastBytesSent == bytesSent) { 
          if(readZeroBytes++ == 12) {
            downloadFinished = true;
            buffer.quit();            
            log.debug("Killing download, client appears to have disconnected: " + bytesSent);            
            cout.close();
          }
        } else {
            readZeroBytes = 0;
        }
        
            
        System.out.println("Client read speed - " + clientReadSpeed + "k/s @ " + bytesSent);
        lastTime = currentTime;           
        lastBytesSent = bytesSent;     
        
       for(int i = 0; i < downloadThreads.length; i++)
       {
         int speed = downloadThreads[i].getSpeed();
         long start = downloadThreads[i].getStartPosition();
         long end = downloadThreads[i].getEndPosition();
         long current = downloadThreads[i].getCurrentPosition();
         System.out.println("Dl thread - " + i + " " + (speed / 1000) + "k/s " + start + " -> " + end + " @ " + current);
       }
    }
    
    // Threads call this when they are done with their block. If it returns false then they should quit
    public synchronized boolean notifyThreadReady(DownloadThread thread, int bytesDownloaded)
    {
        if (downloadFinished) return false;
        
        int threadBlockSize = blockSize;
        
        // Ramp up the block size. We start with a small block size and  then work our way up. 
        // The theory being that the first few blocks should be small so that we buffer
        // data quickly ahead of the client.
        if(blockSize * 2 <= maxBlockSize)
          blockSize *= 2;
        else
          blockSize = maxBlockSize;
        
        // If we are nearing the end of the stream the block size might go over it, so we shorten it
        if (serverReadPosition + threadBlockSize > contentLength)
        {
            threadBlockSize = (int) (contentLength - serverReadPosition);
        }
        
        // Tell the thread to download the next block
        thread.setBlock(serverReadPosition, serverReadPosition + threadBlockSize);
        serverReadPosition += threadBlockSize;
        return true;
    }

    public boolean isFinished()
    {
        return downloadFinished;
    }    
    
    /**
     * Download threads call this method to notify the manager that something went wrong
     * and that they have to quit.
     *
     */
    public void notifyThreadQuit()
    {
        downloadFinished = true;
    }

    /**
     * Simple thread that sleeps 1 second and then outputs the current speeds of the download
     * 
     * @author conorhunt
     *
     */
    private class SpeedThread extends Thread
    {
        ProxyThread parent = null;
        
        public SpeedThread(ProxyThread parent)
        {
            this.parent = parent;
        }
        
        public void run()
        {
            try {
            while(!parent.isFinished())
            {
              try
              {
                Thread.sleep(1000);
              }
              catch(InterruptedException e) { }
              
              parent.outputCurrentSpeeds();  
            }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }        
}
