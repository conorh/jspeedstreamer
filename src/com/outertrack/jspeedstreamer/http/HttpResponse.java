package com.outertrack.jspeedstreamer.http;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;

import com.outertrack.jspeedstreamer.utils.MultiLogger;

/**
 * This class parses the response to a HTTP request.
 * 
 * @author conorhunt
 *
 */
public class HttpResponse
{
    private static MultiLogger log = MultiLogger.getLogger(HttpResponse.class);
    
    private BufferedInputStream input = null;
    private Socket socket = null;
    
    private int responseCode = -1;
    private long contentLength = -1;
    
    private HashMap<String, String> headers = new HashMap<String, String>();
    
    // For ease of writing the headers to the proxy client we just store them in a byte array
    // this way we can just dump the server response to the client by writing this array
    private byte[] responseByteHeaders = null;

    public HttpResponse(Socket s) throws IOException
    {
        this.socket = s;
        this.input = new BufferedInputStream(s.getInputStream());
        parseResponse(input);
    }

    public byte[] getResponseBytes()
    {
        return responseByteHeaders;
    }

    public void close() throws IOException
    {
        if (socket != null) socket.close();
    }

    /**
     * Reads the response to a http request. Parses out the response code and headers
     * 
     * @param in
     * @throws IOException
     */
    private void parseResponse(InputStream in) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String line = readLine(in, out);
        
        int firstSpace = line.indexOf(' ');
        int secondSpace = line.indexOf(' ', firstSpace + 1);
        this.responseCode = Integer.parseInt(line.substring(firstSpace + 1, secondSpace));
        
        while ((line = readLine(in, out)).length() > 0)
        {
            // Parse each header
            int colonIndex = line.indexOf(':');
            if (colonIndex >= 0)
            {
                headers.put(line.substring(0, colonIndex).toLowerCase().trim(), line.substring(colonIndex + 1).trim());
            }
        }
        
        String contentLength = headers.get("content-length");
        if (contentLength != null)
            this.contentLength = Long.parseLong(contentLength);
        
       // if(this.content)
        //    Connection header including the connection-token close
            
        //    Keep-Alive
            
        // Save the original headers into a byte array
        this.responseByteHeaders = out.toByteArray();
    }

    /**
     * Read a single line from an input stream and write out the data including line endings to an
     * output stream.
     * 
     * @param in
     * @param out
     * @return
     * @throws IOException
     */
    private String readLine(InputStream in, OutputStream out) throws IOException
    {
        StringBuffer buf = new StringBuffer(128);
        int b = 0;
        while ((b = in.read()) >= 0)
        {
            out.write(b);
            if (b == '\r')
            {
                b = in.read();
                out.write(b);
                break;
            }
            buf.append((char) b);
        }
        String bufString = buf.toString();
      //  log.debug(bufString);
        return bufString;
    }
    
    public InputStream getInputStream() throws IOException
    {
        return input;
    }

    public OutputStream getOutputStream() throws IOException
    {
        return socket.getOutputStream();
    }

    public int getResponseCode()
    {
        return responseCode;
    }

    public long getContentLength()
    {
        return contentLength;
    }
}
