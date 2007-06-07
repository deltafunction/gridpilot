package gridpilot.ftplugins.https;

import gridpilot.Debug;
import gridpilot.GridPilot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.globus.io.streams.GlobusInputStream;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.io.urlcopy.UrlCopyException;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.SelfAuthorization;

public class MyUrlCopy extends UrlCopy implements Runnable {
  
  private String result;

    
  /**
   * Deletes the source URL.
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
   * This function performs the actual delete.
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
    
    Debug.debug("The server returned "+result, 1);
    
    return true;
  }
  
  /**
   * Get the response from the server after execute();
   * @return the response string, null if there was no response.
   */
  public String getResult(){
    return result;
  }

  /**
   * Returns input stream based on the source url
   */
  protected GlobusInputStream getInputStream(String cmd) 
     throws Exception {
    
    GlobusInputStream in = null;
    String fromP         = srcUrl.getProtocol();
    String fromFile      = srcUrl.getPath();

    if(fromP.equalsIgnoreCase("https")){
      Authorization auth = getSourceAuthorization();
      if (auth == null){
        auth = SelfAuthorization.getInstance();
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

}
