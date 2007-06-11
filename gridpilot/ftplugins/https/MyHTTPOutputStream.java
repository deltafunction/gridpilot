package gridpilot.ftplugins.https;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;

import org.globus.io.gass.client.internal.GASSProtocol;
import org.globus.io.gass.client.GassException;
import org.globus.io.streams.GlobusOutputStream;
import org.globus.net.SocketFactory;
import org.globus.util.http.HttpResponse;
import org.globus.common.ChainedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MyHTTPOutputStream extends GlobusOutputStream {

    private static Log logger =
        LogFactory.getLog(MyHTTPOutputStream.class.getName());

    private static final byte[] CRLF = "\r\n".getBytes();
    private static final int DEFAULT_TIME = 3000;

    protected OutputStream output;
    protected InputStream in;
    protected Socket socket;
    protected long size = -1;
    protected boolean append = false;


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
	
	if (reply.httpCode != 100 && reply.httpCode != 201 && reply.httpCode != 200) {
	    abort();
	    throw new IOException("Gass PUT failed: " + reply.httpMsg);
	} else {
	    logger.debug("Received continuation reply");
	}
    }

}
