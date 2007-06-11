package gridpilot.ftplugins.https;

import gridpilot.Debug;
import gridpilot.GridPilot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;

import org.globus.io.streams.FTPInputStream;
import org.globus.io.streams.FTPOutputStream;
import org.globus.io.streams.GassInputStream;
import org.globus.io.streams.GlobusFileInputStream;
import org.globus.io.streams.GlobusFileOutputStream;
import org.globus.io.streams.GlobusInputStream;
import org.globus.io.streams.GlobusOutputStream;
import org.globus.io.streams.GridFTPInputStream;
import org.globus.io.streams.GridFTPOutputStream;
import org.globus.io.streams.HTTPInputStream;
import org.globus.io.streams.HTTPOutputStream;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.io.urlcopy.UrlCopyException;

import org.globus.gsi.gssapi.auth.Authorization;

public class MyUrlCopy extends UrlCopy implements Runnable {
  
  private String result;

    
  /**
   * Executes the command cmd.
   * Currently, https is supported.
   * 
   * @throws UrlCopyException in case of an error.
   */
  public void execute(String cmd) throws UrlCopyException {
    if (srcUrl == null) {
      throw new UrlCopyException("URL is not specified");
    }
    String toP    = srcUrl.getProtocol();
    if (!toP.equals("https")) {
      throw new UrlCopyException("Protocol not supported: "+toP);    
    }
    GlobusInputStream in   = null;
    boolean rs             = false;
    try {
      in = getInputStream(cmd);
      long size = in.getSize();
      if(size==-1){
        Debug.debug("Source size: unknown", 2);
      }
      else{
        Debug.debug("Source size: " + size, 3);
      }
      rs = execute(in);
      in.close();
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("Could not execute command "+cmd, e);
      if(in!=null){
        in.abort();
      }
      throw new UrlCopyException("execute failed", e);
    }
    if(!rs && isCanceled()){
      throw new UrlCopyException("delete Aborted");
    }
  }
    
  /**
   * This function performs the actual action.
   */
  private boolean execute(GlobusInputStream in) throws IOException {
    
    result = null;
      
    byte [] buffer       = new byte[bufferSize];
    int bytes            = 0;
    long transferedBytes = 0;
    
    // byt array to catch the output
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    while((bytes = in.read(buffer))!= -1){
      out.write(buffer, 0, bytes);
      out.flush();
      if (listeners!=null){
        transferedBytes += bytes;
      }
      if(isCanceled()){
        return false;
      }
    }
    
    result = out.toString();
    
    Debug.debug("The server returned "+result, 3);
    
    return true;
  }
  
  /**
   * Get the response from the server after execute();
   * @return the response string, null if there was no response.
   */
  public String getResult(){
    return result;
  }

  protected GlobusInputStream getInputStream(String cmd) 
     throws Exception {
    
    GlobusInputStream in = null;
    String fromP         = srcUrl.getProtocol();
    String fromFile      = srcUrl.getPath();

    if(fromP.equalsIgnoreCase("https")){
      Authorization auth = getSourceAuthorization();
      if (auth == null){
        
        // NOTICE: host authorization disabled!
        // This is because I cannot get a normal Apache+mod_gridsite
        // to work. jglobus tries to verify the host (target) subject
        // with my (expected target) subject name (or vice versa - don't remember).
        // Perhaps related to globus allowing gass servers to be started with
        // user certificates...
        
        //auth = SelfAuthorization.getInstance();
      }
      in = new MyGassInputStream(getSourceCredentials(),
                               auth,
                               srcUrl.getHost(),
                               srcUrl.getPort(),
                               fromFile,
                               cmd);
    }
    else{
      throw new Exception("Protocol: " + fromP + " not supported!");
    }
    
    return in;
  }


  protected GlobusInputStream getInputStream() 
  throws Exception {
  
    GlobusInputStream in = null;
    String fromP         = srcUrl.getProtocol();
    String fromFile      = srcUrl.getPath();
    
    if (fromP.equalsIgnoreCase("file")) {
        fromFile = URLDecoder.decode(fromFile);
        in = new GlobusFileInputStream(fromFile);
    } else if (fromP.equalsIgnoreCase("ftp")) {
        fromFile = URLDecoder.decode(fromFile);
        in = new FTPInputStream(srcUrl.getHost(),
                                srcUrl.getPort(),
                                srcUrl.getUser(),
                                srcUrl.getPwd(),
                                fromFile);
    } else if (fromP.equalsIgnoreCase("gsiftp") ||
               fromP.equalsIgnoreCase("gridftp")) {
        Authorization auth = getSourceAuthorization();
        if (auth == null) {
            //auth = HostAuthorization.getInstance();
        }
        fromFile = URLDecoder.decode(fromFile);
        in = new GridFTPInputStream(getSourceCredentials(),
                                    auth,
                                    srcUrl.getHost(),
                                    srcUrl.getPort(),
                                    fromFile,
                                    getDCAU());
        
    } else if (fromP.equalsIgnoreCase("https")) {
        Authorization auth = getSourceAuthorization();
        if (auth == null) {
            //auth = SelfAuthorization.getInstance();
        }
        in = new GassInputStream(getSourceCredentials(), 
                                 auth,
                                 srcUrl.getHost(),
                                 srcUrl.getPort(),
                                 fromFile);
    } else if (fromP.equalsIgnoreCase("http")) {
        in = new HTTPInputStream(srcUrl.getHost(),
                                 srcUrl.getPort(),
                                 fromFile);
    } else {
        throw new Exception("Source protocol: " + fromP + 
                            " not supported!");
    }
    
    return in;
  }

  protected GlobusOutputStream getOutputStream(long size) 
  throws Exception {

    GlobusOutputStream out = null;
    String toP             = dstUrl.getProtocol();
    String toFile          = dstUrl.getPath();
    
    if (toP.equalsIgnoreCase("file")) {
        toFile = URLDecoder.decode(toFile);
        out = new GlobusFileOutputStream(toFile, appendMode);
    } else if (toP.equalsIgnoreCase("ftp")) {
        toFile = URLDecoder.decode(toFile);
        out = new FTPOutputStream(dstUrl.getHost(),
                                  dstUrl.getPort(),
                                  dstUrl.getUser(),
                                  dstUrl.getPwd(),
                                  toFile,
                                  appendMode);
    } else if (toP.equalsIgnoreCase("gsiftp") ||
               toP.equalsIgnoreCase("gridftp")) {
        Authorization auth = getDestinationAuthorization();
        if (auth == null) {
            //auth = HostAuthorization.getInstance();
        }
        toFile = URLDecoder.decode(toFile);
        out = new GridFTPOutputStream(getDestinationCredentials(),
                                      auth,
                                      dstUrl.getHost(),
                                      dstUrl.getPort(),
                                      toFile,
                                      appendMode,
                                      getDCAU());
    } else if (toP.equalsIgnoreCase("https")) {
        Authorization auth = getDestinationAuthorization();
        if (auth == null) {
            //auth = SelfAuthorization.getInstance();
        }
        out = new MyGassOutputStream(getDestinationCredentials(),
                                   auth,
                                   dstUrl.getHost(),
                                   dstUrl.getPort(),
                                   toFile,
                                   size,
                                   appendMode);
    } else if (toP.equalsIgnoreCase("http")) {
        out = new HTTPOutputStream(dstUrl.getHost(),
                                   dstUrl.getPort(),
                                   toFile,
                                   size,
                                   appendMode);
    } else {
        throw new Exception("Destination protocol: " + toP + 
                            " not supported!");
    }
    
    return out;
  }
  
}
