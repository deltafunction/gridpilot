package gridpilot.ftplugins.https;

import java.io.IOException;

import org.globus.common.ChainedIOException;
import org.globus.io.gass.client.GassException;
import org.globus.io.gass.client.internal.GASSProtocol;
import org.globus.io.streams.HTTPOutputStream;
import org.globus.util.http.HttpResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MyHTTPOutputStream extends HTTPOutputStream {

    private static Log logger =
        LogFactory.getLog(MyHTTPOutputStream.class.getName());

    private static final byte[] CRLF = "\r\n".getBytes();
    private static final int DEFAULT_TIME = 3000;
    
    public MyHTTPOutputStream(String host, 
        int port, 
        String file, 
        long length, 
        boolean append)
       throws GassException, IOException {
    super(host, port, file, length, append);
  }
    
    protected MyHTTPOutputStream() {
    }



    private void sleep(int time) {
	try {
	    Thread.sleep(time);
	} catch(Exception e) {}
    }

    protected void put(String host, String file, long length, int waittime) 
	throws IOException {
	
	output = socket.getOutputStream();
	in  = socket.getInputStream();
	
	String msg =  GASSProtocol.PUT(file,
				       host,
				       length,
				       append);
	
	if (logger.isTraceEnabled()) {
	    logger.trace("SENT: " + msg);
	}
	
	output.write( msg.getBytes() );
	output.flush();
	
	if (waittime < 0) {
	    int maxsleep = DEFAULT_TIME;
	    while(maxsleep != 0) {
		sleep(1000);
		maxsleep -= 1000;
		checkForReply();
	    }
	} else {
	    sleep(waittime);
	}

	checkForReply();
    }
    
    private void checkForReply() 
	throws IOException {

	if (in.available() <= 0) {
	    return;
	}

	HttpResponse reply = new HttpResponse(in);
	
	if (logger.isTraceEnabled()) {
	    logger.trace("REPLY: " + reply);
	}
	
	if (reply.httpCode != 100 && reply.httpCode != 200 && reply.httpCode != 201) {
	    abort();
	    throw new IOException("Gass PUT failed: " + reply.httpMsg + ":" + reply.httpCode);
	} else {
	    logger.debug("Received continuation reply");
	}
    }

    public void close() 
    throws IOException {
    
    // is there a way to get rid of that wait for final reply?
    
    finish();
    
    HttpResponse hd = new HttpResponse(in);

    closeSocket();
    
    if (logger.isTraceEnabled()) {
        logger.trace("REPLY: " + hd);
    }
    
    if (hd.httpCode != 100 && hd.httpCode != 200 && hd.httpCode != 201) {
        throw new ChainedIOException("Gass close failed.",
             new GassException("Gass PUT failed: " + hd.httpMsg + ":" + hd.httpCode));
    }
      }

    private void finish() throws IOException {
      if (size == -1) {
          String lHex = Integer.toHexString(0);
          output.write(lHex.getBytes());
          output.write(CRLF);
          output.write(CRLF);
      }
      output.flush();
        }

    private void closeSocket() {
      try {
          if (socket != null) socket.close();
          if (in != null) in.close();
          if (output != null) output.close();
      } catch(Exception e) {}
        }

}
