package com.outertrack.jspeedstreamer.utils;

/**
 * This is a circular download buffer that understands that it is a 'window' into a larger stream of
 * data.
 * 
 * NOTE: It assumes there are many writers and only one reader. It might work otherwise, but I doubt
 * it :)
 * 
 * There is a fixed size byte buffer allocated internally and all data is read and written to this
 * buffer.
 * 
 * @author conorhunt
 * 
 */
public class CircularDownloadBuffer
{
    private static MultiLogger log = MultiLogger.getLogger(CircularDownloadBuffer.class);    
    
    // buffer to hold data
    private int bufferSize = 6000000;
    private byte[] buffer = null;
    
    // Array that keeps track of whether bytes in the buffer are ready to read or not
    private boolean[] writeRecord = null;
    
    // Current count of the total bytes read out
    private long streamReadPosition = 0;
    
    // Current read position in the circular buffer
    private int bufferReadPosition = 0;

    boolean finished = false;

    public CircularDownloadBuffer()
    {
        buffer = new byte[bufferSize];
        writeRecord = new boolean[bufferSize];
    }
    
    public CircularDownloadBuffer(int size)
    {
        buffer = new byte[size];
        writeRecord = new boolean[size];
        bufferSize = size;
    }
    
    public void quit()
    {
        finished = true;
    }

    public int read(byte[] output, int maxReadLength)
    {
        // Make sure there are enough bytes to read, if not then pause until there are.
        // Note: we don't have to worry about bytes in the middle of the range being false and reading those by accident
        // The loop that reads bytes (see the synchronized section below) will only read until the next 'true'
        while (finished != true && writeRecord[(bufferReadPosition + maxReadLength) % bufferSize] != true)
        {
            log.debug("Buffer - read waiting: " + streamReadPosition);
            try
            {
                Thread.sleep(200);
            }
            catch (InterruptedException e)
            {}
        }
 
        int counter = 0;
        synchronized (this)
        {
            // For all of the bytes that we are about to read, mark them as now free for writing in the buffer
            int startPos = bufferReadPosition;
            for(counter = 0; counter < maxReadLength && writeRecord[bufferReadPosition] == true; counter++)
            {                
                writeRecord[bufferReadPosition] = false;
                bufferReadPosition += 1;
                if (bufferReadPosition >= bufferSize)
                    bufferReadPosition = 0;                
            }
            
            // Get as many bytes as are ready and read them into the output buffer
            if (startPos + counter >= bufferSize)
            {
                // If the bytes we read goes over the end of the buffer then we need to split the
                // array copy into two to handle the wrap around
                int bytesToEnd = bufferSize - startPos;
                System.arraycopy(buffer, startPos, output, 0, bytesToEnd);
                System.arraycopy(buffer, 0, output, bytesToEnd, counter - bytesToEnd);
            }
            else
            {
                System.arraycopy(buffer, startPos, output, 0, counter);
            }
            
            streamReadPosition += counter;
        }
        return counter;
    }

    public void write(byte newBytes[], int byteCount, long streamWritePosition)
    {
        // Check that there are enough free bytes in the buffer to write the data
        while (finished != true && streamWritePosition + byteCount >= streamReadPosition + bufferSize)
        {
            // If there are not enough free bytes then we pause until the buffer
            // window has moved along the stream and there is space at the end of the buffer
            try
            {
                Thread.sleep(200);
            }
            catch (InterruptedException e)
            {
                break;
            }
        }
        // Write the bytes into the circular buffer. This is synchronized
        // because the reading thread
        // will modify the bufferReadPosition and the streamReadPosition when it
        // reads data
        int writePosition = -1;
        synchronized (this)
        {
            // Calculate where we should be writing into the buffer
            // Explanation:
            // Think of the buffer as a sliding window that keeps moving along the stream
            // 1. As we read bytes from the stream the streamReadPosition increases (the start of the window)
            // 2. The position in the stream that we want to write to is streamWritePosition. So we know that
            // the index into our window (the buffer) is streamWritePosition - streamReadPosition.
            // 3. This is a circular buffer. The start of the buffer is at the position that we are 
            // reading bytes from, so in order to get the position into the buffer that we want to
            // write the bytes to we add the bufferReadPosition to the position calculated in step 2.
            // 4. Since this is a circular buffer this resulting position can be greater than the size of 
            // the buffer and so we wrap it by doing % BUFFER_SIZE
            writePosition = (int) ((bufferReadPosition + (streamWritePosition - streamReadPosition)) % bufferSize);
            if (writePosition + byteCount >= bufferSize)
            {
                // If the bytes we are writing goes over the end of the buffer then
                // we need to split the array copy into two to handle the wrap around.
                int bytesToEnd = bufferSize - writePosition;
                System.arraycopy(newBytes, 0, buffer, writePosition, bytesToEnd);
                System.arraycopy(newBytes, bytesToEnd, buffer, 0, byteCount - bytesToEnd);
            }
            else
            {
                System.arraycopy(newBytes, 0, buffer, writePosition, byteCount);
            }

            // Mark the bytes that we have written to note that they are ready for reading.
            for(int counter = 0; counter < byteCount; counter++, writePosition++) 
            { 
                if (writePosition >= bufferSize) writePosition = 0;             
                writeRecord[writePosition] = true;
            }  
        }
    }
}