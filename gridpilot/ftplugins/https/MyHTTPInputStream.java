package gridpilot.ftplugins.https;

import gridpilot.Debug;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStream;

import org.globus.io.streams.HTTPInputStream;
import org.globus.util.http.HttpResponse;
import org.globus.util.http.HTTPChunkedInputStream;
import org.globus.util.GlobusURL;


public class MyHTTPInputStream extends HTTPInputStream {

  protected void execute(String host, int port, String file, String cmd) throws IOException {
	
    HttpResponse hd = null;

    while(true){
      this.socket = openSocket(host, port);
      this.input = this.socket.getInputStream();
      OutputStream out = socket.getOutputStream();
  
      String msg = MyGASSProtocol.EXECUTE(file, host + ":" + port, cmd);

      try{
        out.write( msg.getBytes() );
        out.flush();
          
        Debug.debug("SENT: " + msg, 2);
          
        hd = new HttpResponse(input);
      }
      catch(IOException e){
        abort();
        throw e;
      }
  
      if (hd.httpCode == 200 || hd.httpCode == 201 || hd.httpCode == 204 || hd.httpCode == 207) {
        break;
      }
      else{
        abort();
        switch(hd.httpCode){
        case 404:
            throw new FileNotFoundException(
                                "File " + file + " not found on the server."
            );
        case 301:
        case 302:
            Debug.debug("Received redirection to: " + hd.location, 2);
            GlobusURL newLocation = new GlobusURL(hd.location);
            host = newLocation.getHost();
            port = newLocation.getPort();
            file = newLocation.getPath();
            break;
        default:
             throw new IOException(
                                "Failed to retrieve file from server. " + 
              " Server returned error: " + hd.httpMsg +
              " (" + hd.httpCode + ")"
                        );
        }
      }
    }

    if (hd.chunked) {
      input = new HTTPChunkedInputStream(input);
    }
    else if (hd.contentLength > 0) {
      size = hd.contentLength;    
    }      
  }
    
}

