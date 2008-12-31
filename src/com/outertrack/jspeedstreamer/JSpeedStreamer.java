package com.outertrack.jspeedstreamer;

import java.io.*;
import java.net.*;
import java.util.HashMap;

import com.outertrack.jspeedstreamer.utils.MultiLogger;

/**
 * Main class. Accepts connections and spins off ProxyThreads
 * 
 * @author conorhunt
 *
 */
public class JSpeedStreamer
{
    private static MultiLogger log = MultiLogger.getLogger(JSpeedStreamer.class);    
    
    public static final int DEFAULT_PORT = 9050;
    public static int port = DEFAULT_PORT;
    
    public static void main(String args[])
    {
        ServerSocket server = null;
        HashMap options = parseCommandLine(args);
        
        if(options.containsKey("PORT"))
            port = ((Integer)options.get("PORT")).intValue();
        
        log.info("Starting JStreamSpeeder on part " + port + ". Press CTRL-C to end");
        log.info("JStreamSpeeder waiting for connections");
        try
        {
            server = new ServerSocket(port);
            while (true)
            {
                Socket client = server.accept();
                log.info("Received connection");
                ProxyThread t = new ProxyThread(client, options);
                t.start();
            }
        }
        catch (Exception e)
        {}
        finally
        {
            if (server != null)
            {
                try
                {
                    server.close();
                }
                catch (IOException e)
                {}
            }
        }
    }
    
    private static HashMap parseCommandLine(String args[])
    {
        HashMap hash = new HashMap();
        for (int i=0; i < args.length; i++) {
          String arg = args[i];
            
          if(args[i].startsWith("-"))
          {
            if(arg.equals("-port"))
              hash.put("PORT", Integer.parseInt(args[++i]));
            else if(arg.equals("-buf"))
              hash.put("BUFFER", Integer.parseInt(args[++i]));
            else if(arg.equals("-max_seg"))
              hash.put("MAXSEG", Integer.parseInt(args[++i]));
            else if(arg.equals("-min_seg"))
              hash.put("MINSEG", Integer.parseInt(args[++i]));
            else if(arg.equals("-threads"))
              hash.put("THREADS", Integer.parseInt(args[++i]));
            else if(arg.equals("-outdir"))
              hash.put("OUTDIR", args[++i]);
            else {
              System.err.print("Unknown argument: " + arg);
            }
          }
        }
        return hash;
    }
}
