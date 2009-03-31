package gridpilot.ftplugins.https;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.globus.util.GlobusURL;

import gridfactory.common.LogFile;
import gridfactory.common.SSL;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

public class HTTPSFileTransfer extends
    gridfactory.common.https.HTTPSFileTransfer {

  public HTTPSFileTransfer(String _user, SSL _ssl, LogFile _logFile) {
    super(_user, _ssl, _logFile);
    fileCacheMgr = GridPilot.getClassMgr().getFileCacheMgr();
  }
  
  public HTTPSFileTransfer() throws IOException, GeneralSecurityException{
    super(GridPilot.IS_FIRST_RUN?null:GridPilot.getClassMgr().getSSL().getGridSubject(),
        GridPilot.getClassMgr().getSSL(), GridPilot.getClassMgr().getLogFile());
    fileCacheMgr = GridPilot.getClassMgr().getFileCacheMgr();
  }

  /**
   * Creates an empty file or directory in the directory on the
   * host specified by url.
   * Asks for the name. Returns the name of the new file or
   * directory.
   * If a name ending with a / is typed in, a directory is created.
   * (this path must match the URL url).
   * @throws Exception 
   */
  public String create(GlobusURL globusUrlDir)
     throws Exception{
    String fileName = MyUtil.getName("File name (end with a / to create a directory)", "");   
    if(fileName==null){
      return null;
    }
    String urlDir = globusUrlDir.getURL();
    if(!urlDir.endsWith("/") && !fileName.startsWith("/")){
      urlDir = urlDir+"/";
    }
    write(new GlobusURL(urlDir+fileName), "");
    return fileName;
  }

}
