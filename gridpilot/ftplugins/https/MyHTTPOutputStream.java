package gridpilot.ftplugins.https;

import java.io.IOException;

import org.globus.io.gass.client.internal.GASSProtocol;
import org.globus.io.streams.HTTPOutputStream;
import org.globus.util.http.HttpResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MyHTTPOutputStream extends HTTPOutputStream {

    private static Log logger =
        LogFactory.getLog(MyHTTPOutputStream.class.getName());

    private static final int DEFAULT_TIME = 3000;

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
