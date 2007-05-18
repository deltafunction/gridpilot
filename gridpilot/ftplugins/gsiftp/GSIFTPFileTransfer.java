package gridpilot.ftplugins.gsiftp;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Vector;

import javax.swing.JOptionPane;
import javax.swing.JProgressBar;

import org.globus.ftp.Buffer;
import org.globus.ftp.DataChannelAuthentication;
import org.globus.ftp.DataSink;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.ServerException;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.io.urlcopy.UrlCopyException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;
import org.apache.log4j.*;

import gridpilot.ConfirmBox;
import gridpilot.Debug;
import gridpilot.FileTransfer;
import gridpilot.LocalStaticShellMgr;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.TransferControl;
import gridpilot.Util;

public class GSIFTPFileTransfer implements FileTransfer {
  
  private String user = null;
  private HashMap jobs = null;
  private HashMap urlCopyTransferListeners = null;
  private HashMap fileTransfers = null;
  
  private static String pluginName;
  
  public GSIFTPFileTransfer(){
    pluginName = "gsiftp";
    GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
    GlobusCredential globusCred = null;
    if(credential instanceof GlobusGSSCredentialImpl){
      globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
    }
    Debug.debug("getting identity", 3);
    user = globusCred.getIdentity();
    /* remove leading whitespace */
    user = user.replaceAll("^\\s+", "");
    /* remove trailing whitespace */
    user = user.replaceAll("\\s+$", "");
    
    jobs = new HashMap();
    urlCopyTransferListeners = new HashMap();
    fileTransfers = new HashMap();
    
    // The log4j root logger. All class loggers used
    // by cog and jarclib inherit from this and none
    // set their own appenders, so they should log
    // only to the console??
    Logger rootLogger = Logger.getRootLogger();
    // remove all logging
    rootLogger.removeAllAppenders();
    // log only info messages and above (DEBUG, INFO, WARN, ERROR, FATAL)
    rootLogger.setLevel(Level.WARN);
    // add logging to console
    SimpleLayout myLayout = new SimpleLayout();
    ConsoleAppender myAppender = new ConsoleAppender(myLayout);
    rootLogger.addAppender(myAppender);
    // This should log to gridpilot.log instead of the console.
    // log4j does not seem to support logging to different destinations
    // depending on the level.
    // TODO: test and consider
    /*FileAppender ap = new FileAppender();
    ap.setFile(GridPilot.logFileName);  
    ap.setName("GridPilot Log");
    ap.activateOptions();
    rootLogger.addAppender(ap);*/
  }

  public String getUserInfo(){
    return user;
  }
  
  public boolean checkURLs(GlobusURL [] srcUrls, GlobusURL [] destUrls){
    Debug.debug("srcUrls.length: "+srcUrls.length, 3);
    Debug.debug("destUrls.length: "+destUrls.length, 3);
    Debug.debug("srcUrls[0].getProtocol(): "+srcUrls[0].getProtocol(), 3);
    Debug.debug("destUrls[0].getProtocol(): "+destUrls[0].getProtocol(), 3);
    // We only allow copying one file at a time - hmm, why...?
    return (srcUrls.length==destUrls.length && /*srcUrls.length==1 &&*/ (
        srcUrls[0].getProtocol().equalsIgnoreCase("gsiftp") &&
           destUrls[0].getProtocol().equalsIgnoreCase("file") ||
        srcUrls[0].getProtocol().equalsIgnoreCase("file") &&
           destUrls[0].getProtocol().equalsIgnoreCase("gsiftp") ||
        srcUrls[0].getProtocol().equalsIgnoreCase("gsiftp") &&
           destUrls[0].getProtocol().equalsIgnoreCase("gsiftp")
          ));
  }

  /**
   * Connect to gridftp server and set environment.
   * This method must be synchronized: before there were problems with
   * simultaneous GPSS submissions, i.e. connecting in parallel to the same
   * host.
   */
  public synchronized GridFTPClient connect(String host, int port) throws IOException, FTPException{
    
    GridFTPClient gridFtpClient = null;
    
    try{
      GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      gridFtpClient = new GridFTPClient(host, port);
      gridFtpClient.authenticate(credential);
    }
    catch(Exception e){
      Debug.debug("Could not connect "+e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    
    // NorduGrid variant
    /*try{
      //gridFtpClient.setOptions(new RetrieveOptions(1));
      //gridFtpClient.setMode(GridFTPSession.MODE_EBLOCK);
      //gridFtpClient.setType(GridFTPSession.TYPE_IMAGE);
      gridFtpClient.setProtectionBufferSize(16384);
      gridFtpClient.setDataChannelAuthentication(DataChannelAuthentication.NONE);
      gridFtpClient.quote("PROT: C");
      gridFtpClient.quote("PASV/SPAS");
      //gridFtpClient.setDataChannelProtection(GridFTPSession.PROTECTION_SAFE);
      // if no exception is thrown data channel authentication will be enabled
      Debug.debug("Path: "+gridFtpClient.getCurrentDir(), 3);
    }
    catch(FTPException ee){
      ee.printStackTrace();*/
      // LCG variant
      try{              
        //Debug.debug(gridFtpClient.quote("MODE E").getMessage(), 3);// S
        //gridFtpClient.setMode(GridFTPSession.MODE_EBLOCK);
        //Debug.debug(gridFtpClient.quote("Type I").getMessage(), 3);
        try{
          gridFtpClient.setType(GridFTPSession.TYPE_IMAGE);
        }
        catch(Exception ee){
        }
        //Debug.debug(gridFtpClient.quote("PBSZ 16384").getMessage(), 3);
        gridFtpClient.setProtectionBufferSize(16384);
        //Debug.debug(gridFtpClient.quote("DCAU N").getMessage(), 3);// N, A
        try{
          gridFtpClient.setDataChannelAuthentication(DataChannelAuthentication.NONE);
        }
        catch(Exception ee){
        }
        //Debug.debug(gridFtpClient.quote("PROT C").getMessage(), 3);
        //gridFtpClient.setDataChannelProtection(GridFTPSession.PROTECTION_SAFE);
        //gridFtpClient.quote("PORT 129,194,55,235,35,46");
        
        //gridFtpClient.setLocalPassive();
        //gridFtpClient.setActive();
        //Debug.debug(gridFtpClient.quote("PASV").getMessage(), 3);
        gridFtpClient.setPassiveMode(true);

        Debug.debug("Current dir: "+gridFtpClient.getCurrentDir(), 3);
      }
      catch(FTPException e){
        e.printStackTrace();
        throw e;
      }
    //}
    return gridFtpClient;
  }

  public void getFile(GlobusURL globusUrl, File downloadDirOrFile,
      StatusBar statusBar, JProgressBar pb)
     throws ClientException, ServerException, FTPException, IOException {
    
    // TODO: implement wildcard *
    
    if(!globusUrl.getProtocol().equalsIgnoreCase("gsiftp")){
      Debug.debug("ERROR: protocol not supported: "+globusUrl.getURL(), 1);
      return;
    }
    String localPath = globusUrl.getPath();
    if(!localPath.startsWith("/")){
      localPath = "/" + localPath;
    }
    String host = globusUrl.getHost();
    String fileName = "";
    int port = globusUrl.getPort();
    if(port<0){
      port = 2811;
    }
    localPath = localPath.replaceAll("/[^\\/]*/\\.\\.", "");
    String localDir = null;
    int lastSlash = localPath.lastIndexOf("/");
    if(lastSlash>0 && lastSlash<localPath.length() - 1){
      localDir = localPath.substring(0, lastSlash);
      fileName = localPath.substring(lastSlash + 1);
    }
    else if(lastSlash==localPath.length()-1){
      throw new IOException("ERROR: Cannot get directories. "+localPath);
    }
    else{
      localDir = "/";
      fileName = localPath;
    }
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    Debug.debug("Directory: "+localDir, 3);
    
    final String id = globusUrl.getURL()+"::"+downloadDirOrFile.getCanonicalPath();

    Debug.debug("Getting "+fileName, 3);
    if(statusBar!=null){
      statusBar.setLabel("Getting "+fileName);
    }
    if(pb!=null){
      pb.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent e){
          try{
            abortTransfer(id);
          }
          catch(Exception ee){
          }
        }
      });
      pb.setToolTipText("Click here to cancel download");
    }

    GridFTPClient gridFtpClient = null;
    
    try{
      gridFtpClient = connect(host, port);
      fileTransfers.put(id, gridFtpClient);
    }
    catch(FTPException e){
      Debug.debug("Could not connect to "+globusUrl.getURL()+". "+
         e.getMessage(), 1);
      //e.printStackTrace();
      throw e;
    }

    try{
      /*
      client.setType(GridFTPSession.TYPE_IMAGE);
      //extended mode is required by parallel transfers
      client.setMode(GridFTPSession.MODE_EBLOCK);
      //required to transfer multiple files
      //client.setLocalPassive();
      client.setActive();
      //set parallelism
      client.setOptions(new RetrieveOptions(2));
      DataSink sink = new FileRandomIO(new
      RandomAccessFile(localFile,"rw"));
      long size = client.getSize(remoteFile);
      client.extendedGet(remoteFile, size, sink, null);
      */
      
      File downloadFile = null;
      if(downloadDirOrFile.isDirectory()){
        downloadFile = new File(downloadDirOrFile.getAbsolutePath(), fileName);
      }
      else{
        downloadFile = downloadDirOrFile;
      }
      Debug.debug("Downloading "+localPath+"->"+downloadFile.getAbsolutePath(), 3);
      gridFtpClient.get(localPath, downloadFile);
      gridFtpClient.close();
     
      // if we don't get an exception, the file got downloaded
      if(statusBar!=null){
        statusBar.setLabel("Download of "+fileName+" done");
      }
      Debug.debug(fileName+" downloaded.", 2);
    }
    catch(FTPException e){
      //e.printStackTrace();
      Debug.debug("Could not read "+localPath, 1);
      throw e;
    }
    finally{
      try{
        gridFtpClient.close();
      }
      catch(Exception e){
      }
    }
  }
  
  /**
   * Upload file to gridftp server.
   */
  public void putFile(File file, GlobusURL globusFileUrl,
      StatusBar statusBar, JProgressBar pb)
    throws IOException, FTPException{
    
    Debug.debug("put "+globusFileUrl.getURL(), 3);
    
    if(!globusFileUrl.getProtocol().equalsIgnoreCase("gsiftp")){
      Debug.debug("ERROR: protocol not supported: "+globusFileUrl.getURL(), 1);
      return;
    }
    
    String localPath = globusFileUrl.getPath();
    if(!localPath.startsWith("/")){
      localPath = "/"+localPath;
    }
    String host = globusFileUrl.getHost();
    int port = globusFileUrl.getPort();
    if(port<0){
      port = 2811;
    }
    localPath = localPath.replaceAll("/[^\\/]*/\\.\\.", "");
    String localDir = null;
    int lastSlash = localPath.lastIndexOf("/");
    
    if(lastSlash>0 && lastSlash<localPath.length()-1){
      localDir = localPath.substring(0, lastSlash);
    }
    else if(lastSlash>0 && lastSlash==localPath.length()-1){
      throw new IOException("ERROR: Upload destination cannot be a directory. "+localPath);
    }
    else{
      localDir = "/";
    }
    
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    Debug.debug("Directory: "+localDir, 3);
    
    final String id = file.getCanonicalPath() +"::"+ globusFileUrl.getURL();
    
    pb.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        try{
          abortTransfer(id);
        }
        catch(Exception ee){
        }
      }
    });
    pb.setToolTipText("Click here to cancel upload");
    
    GridFTPClient gridFtpClient = null;
    
    try{
      gridFtpClient = connect(host, port);
      fileTransfers.put(id, gridFtpClient);
    }
    catch(FTPException e){
      Debug.debug("Could not connect to "+globusFileUrl.getURL()+". "+
         e.getMessage(), 1);
      //e.printStackTrace();
      throw e;
    }
    try{
      Debug.debug("Current dir: "+gridFtpClient.getCurrentDir(), 3);
      gridFtpClient.changeDir(localDir);
      gridFtpClient.put(file, localPath, false);     
      // if we don't get an exception, the file got written...
      Debug.debug("File or directory "+globusFileUrl.getURL()+" written.", 2);
    }
    catch(FTPException e){
      //e.printStackTrace();
      Debug.debug("Could not read "+localPath, 1);
      throw e;
    }
    finally{
      try{
        gridFtpClient.close();
      }
      catch(Exception e){
      }
    }
  }
  
  /**
   * Cancels a running transfer from fileTransfers.
   * These are transfers initiated by getFile or putFile.
   * @param id the ID of the transfer.
   * @throws IOException 
   * @throws ServerException 
   */
  private void abortTransfer(String id) throws ServerException, IOException{
    GridFTPClient gridftpClient = ((GridFTPClient) fileTransfers.get(id));
    gridftpClient.abort();
    gridftpClient.close(true);
    fileTransfers.remove(id);
  }
  
  /**
   * Delete files on gridtp server. They MUST all be on the same server.
   */
  public void deleteFiles(GlobusURL [] globusUrls) throws
     IOException, FTPException{
    
    if(globusUrls==null || globusUrls.length==0){
      Debug.debug("No files to delete. "+globusUrls, 2);
      return;
    }
    
    for(int i=0; i<globusUrls.length; ++i){
      if(!globusUrls[i].getHost().equals(globusUrls[0].getHost())){
        throw new IOException("ERROR: all files to be deleted must be on the same server. "+
            globusUrls[i]+" <-> "+globusUrls[0]);
      }
    }
    
    Debug.debug("delete "+Util.arrayToString(globusUrls), 3);
    
    for(int i=0; i<globusUrls.length; ++i){
      if(!globusUrls[i].getProtocol().equalsIgnoreCase("gsiftp")){
        throw new IOException(
            "ERROR: protocol not supported: "+globusUrls[i].getURL());
      }
      if(!globusUrls[i].getHost().equals(globusUrls[0].getHost())){
        throw new IOException(
            "ERROR: non-uniform set of URLs: "+Util.arrayToString(globusUrls));
      }
    }
    
    String host = globusUrls[0].getHost();
    int port = globusUrls[0].getPort();
    if(port<0){
      port = 2811;
    }
    
    GridFTPClient gridFtpClient = null;
    try{
      gridFtpClient = connect(host, port);
    }
    catch(FTPException e){
      Debug.debug("Could not connect to "+host+":"+port+". "+
         e.getMessage(), 1);
      throw e;
    }

    try{
      GlobusURL globusUrl = null;
      for(int i=0; i<globusUrls.length; ++i){
        globusUrl = globusUrls[i];
        String localPath = globusUrl.getPath();
        if(!localPath.startsWith("/")){
          // globusUrl.getPath() returns the path relative to /
          localPath = "/" + localPath;
        }
        localPath = localPath.replaceAll("/[^\\/]*/\\.\\.", "");
        String localDir = null;
        int lastSlash = localPath.lastIndexOf("/");

        if(lastSlash>0 && lastSlash<localPath.length() - 1){
          localDir = localPath.substring(0, lastSlash);
        }
        else if(lastSlash>0 && lastSlash==localPath.length() - 1){
          //throw new IOException("ERROR: directory delete not supported. "+localPath);
        }
        else{
          localDir = "/";
        }

        Debug.debug("Host: "+host, 3);
        Debug.debug("Port: "+port, 3);
        Debug.debug("Path: "+localPath, 3);
        Debug.debug("Directory: "+localDir, 3);

        try{
          Debug.debug("Current dir: "+gridFtpClient.getCurrentDir(), 3);
          gridFtpClient.changeDir(localDir);
          if(lastSlash>0 && lastSlash==localPath.length() - 1){
            gridFtpClient.deleteDir(localPath);
          }
          else{
            gridFtpClient.deleteFile(localPath);       
          }
          // if we don't get an exception, the file got deleted
          Debug.debug("File or directory "+globusUrl.getURL()+" deleted.", 2);
        }
        catch(FTPException e){
          //e.printStackTrace();
          Debug.debug("WARNING: Could not delete "+localPath, 1);
          throw e;
        }
      }
    }
    catch(FTPException e){
      throw e;
    }
    finally{
      try{
        gridFtpClient.close();
      }
      catch(Exception e){
      }
    }
  }

  /**
   * Creates an empty file or directory in the directory on the
   * host specified by url.
   * Asks for the name. Returns the name of the new file or
   * directory.
   * If a name ending with a / is typed in, a directory is created.
   * (this path must match the URL url).
   */
  public String create(GlobusURL globusUrlDir)
     throws FTPException, IOException{
    String fileName = Util.getName("File name (end with a / to create a directory)", "");   
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

  /**
   * Write file containing text <text> on server,
   * or, if <text> is null or empty and <url> ends with /,
   * create directory
   */
  public void write(GlobusURL globusUrl, String text)
    throws FTPException, IOException{
    
    Debug.debug("writeGsiftpFile "+globusUrl.getURL(), 3);
    
    if(!globusUrl.getProtocol().equalsIgnoreCase("gsiftp")){
      Debug.debug("ERROR: protocol not supported: "+globusUrl.getURL(), 1);
      return;
    }
    String localPath = globusUrl.getPath();
    if(!localPath.startsWith("/")){
      localPath = "/" + localPath;
    }
    String host = globusUrl.getHost();
    int port = globusUrl.getPort();
    if(port<0){
      port = 2811;
    }
    localPath = localPath.replaceAll("/[^\\/]*/\\.\\.", "");
    String localDir = null;
    int lastSlash = localPath.lastIndexOf("/");
    if(lastSlash>0 && lastSlash<localPath.length() - 1){
      localDir = localPath.substring(0, lastSlash);
    }
    else if(lastSlash>0 && lastSlash==localPath.length() - 1){
      // we're writing a directory
      int lastSlash1 = localPath.substring(0, lastSlash).lastIndexOf("/");
      if(lastSlash1>0){
        localDir = localPath.substring(0, lastSlash1);
      }
      else{
        localDir = "/";
      }
    }
    else{
      localDir = "/";
    }
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    Debug.debug("Directory: "+localDir, 3);
    File tmpFile = null;
    GridFTPClient gridFtpClient = null;
    try{
      gridFtpClient = connect(host, port);
    }
    catch(IOException e){
      Debug.debug("Could not connect to"+globusUrl.getURL()+". "+
         e.getMessage(), 1);
      throw e;
    }
    try{
      gridFtpClient.changeDir(localDir);
      Debug.debug("Current dir: "+gridFtpClient.getCurrentDir(), 3);
      if(localPath.endsWith("/") && (text==null || text.equals(""))){
        gridFtpClient.makeDir(localPath);
      }
      else if(!localPath.endsWith("/")){
        tmpFile = File.createTempFile("gridpilot.", ".txt");
        LocalStaticShellMgr.writeFile(tmpFile.getAbsolutePath(), text, false);
        Debug.debug("Created temp file "+tmpFile, 3);
        String fileName = (new File(localPath)).getName();
        Debug.debug("Uploading "+tmpFile.getAbsolutePath()+" --> "+fileName, 3);
        // just in case the file is already there
        try{
          gridFtpClient.deleteFile(fileName);
        }
        catch(Exception e){
          Debug.debug("Checked, but "+fileName+" not there", 3);
          //e.printStackTrace();
        }
        // it would be better to first rename the remote file and then delete it,
        // but rename doesn't work...
        //gridFtpClient.rename(fileName, fileName+".tmp");
        gridFtpClient.put(tmpFile, fileName, false);
        //gridFtpClient.deleteFile(fileName+".tmp");
        tmpFile.delete();
      }
      else{
        throw new IOException("ERROR: Cannot write text to a directory.");
      }
      // if we don't get an exception, the file got written...
      Debug.debug("File or directory "+globusUrl.getURL()+" written.", 2);
      return;
    }
    catch(FTPException e){
      Debug.debug("Could not write "+localPath, 1);
      throw e;
    }
    finally{
      try{
        tmpFile.delete();
        gridFtpClient.close();
      }
      catch(Exception e){
      }
    }
  }

  /**
   * Argument: a space separated list of URL strings.
   * If each URL given is a directory, return list of files in the
   * directory as a set of GlobusURLs.
   * If each URL contains a *, return the matching files as a set of
   * GlobusURLs.
   */
  public TreeSet resolveWildCardUrls(String remoteUrl) throws IOException{
    
    String [] urls = Util.split(remoteUrl);
    String url = null;
    String localPath = null;
    String host = null;
    String hostAndPath = null;
    String port = "2811";
    String hostAndPort = null;
    String localDir = null;
    String localFile = null;
    Object [] textArr = null;
    TreeSet urlSet = new TreeSet();
    Vector failedUrls = new Vector();
    GlobusURL globusUrl = null;

    for(int i=0; i<urls.length; ++i){
      // Find host, port, dir name and file name.
      url =urls[i];
      Debug.debug("gsiftpGetFile "+url, 3);
      
      if(url.startsWith("gsiftp://")){
        hostAndPath = url.substring(9);
      }
      else if(url.startsWith("gsiftp:/")){
        hostAndPath = url.substring(8);
      }
      else{
        Debug.debug("ERROR: protocol not supported: "+url, 1);
        return null;
      }
      Debug.debug("Host+path: "+hostAndPath, 3);
      hostAndPort = hostAndPath.substring(0, hostAndPath.indexOf("/"));
      int colonIndex=hostAndPort.indexOf(":");
      if(colonIndex>0){
        host = hostAndPort.substring(0, hostAndPort.indexOf(":"));
        port = hostAndPort.substring(hostAndPort.indexOf(":")+1);
      }
      else{
        host = hostAndPort;
        port = "2811";
      }
      localPath = hostAndPath.substring(hostAndPort.length(), hostAndPath.length());
      localPath = localPath.replaceAll("/[^\\/]*/\\.\\.", "");
      int lastSlash = localPath.lastIndexOf("/");
      if(lastSlash>0){
        localDir = localPath.substring(0, lastSlash);
        localFile = localPath.substring(lastSlash+1);
      }
      else{
        localDir = "/";
        localFile = localPath;
      }
      Debug.debug("Host: "+host, 3);
      Debug.debug("Port: "+port, 3);
      Debug.debug("Path: "+localPath, 3);
      Debug.debug("Directory: "+localDir, 3);
      Debug.debug("File: "+localFile, 3);

      // Populate urlSet
      GridFTPClient gridFtpClient = null;
      try{
        gridFtpClient = connect(host, Integer.parseInt(port));
      }
      catch(Exception e){
        Debug.debug("Could not donnect "+localPath+". "+
           e.getMessage(), 1);
        e.printStackTrace();
        failedUrls.add(url);
        continue;
        //throw new IOException(e.getMessage());
      }
      try{
        gridFtpClient.changeDir(localDir);
                
        Debug.debug("Current dir: "+gridFtpClient.getCurrentDir(), 3);
                
        // List all files in directory localDir on server.
        // NorduGrid variant
        try{
          textArr = gridFtpClient.mlsd().toArray();
        }
        catch(FTPException ee){
          ee.printStackTrace();
          // LCG variant
          try{
            textArr = gridFtpClient.list().toArray();
          }
          catch(FTPException e){
            e.printStackTrace();
          }
        }
        
        final ByteArrayOutputStream received2 = new ByteArrayOutputStream(100000);
        DataSink dataSink = new DataSink(){
          public void write(Buffer buffer) 
          throws IOException{
            Debug.debug("received " + buffer.getLength() +
               " bytes of directory listing", 1);
               received2.write(buffer.getBuffer(),
               0,
               buffer.getLength());
            return;
          }
          public void close() throws IOException{
            Debug.debug("closing", 1);
          };                
        };
        gridFtpClient.list("", "", dataSink);
        textArr = Util.split(received2.toString(), "\n");

        // Find the files matching localFile
        if(localFile.indexOf("*")>-1){
          // Parse wildcard
          Debug.debug("Parsing wildcard", 3);
          String [] text = new String[textArr.length];
          String [] entryArr = null;
          for(int j=0; j<textArr.length; ++j){
            text[j] = textArr[j].toString();
            //Debug.debug(text[j], 3);
            entryArr = Util.split(text[j]);
            text[j] = entryArr[entryArr.length-1];
          }
          localFile = localFile.replaceAll("\\.", "\\\\.");
          localFile = localFile.replaceAll("\\*", ".*");
          Debug.debug("Matching "+localFile, 3);
          for(int j=0; j<text.length; ++j){
            if(text[j].matches(localFile)){
              try{
                globusUrl = new GlobusURL(host+":"+port+localDir+"/"+text[j]);
                urlSet.add(globusUrl);
              }
              catch(Exception e){
                e.printStackTrace();
                failedUrls.add(url);
              }
            }
          }
        }
        else{
          globusUrl = new GlobusURL(host+":"+port+localDir+"/"+localFile);
          urlSet.add(globusUrl);
        }
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Error while adding "+localPath, 1);
        failedUrls.add(url);
        continue;
      }
      finally{
        try{
          gridFtpClient.close();
        }
        catch(ServerException e){
        }
      }
    }
    if(!urlSet.isEmpty() && !failedUrls.isEmpty()){
      int choice = 1;
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame()/*,"",""*/); 
      try{
        String confirmString = "WARNING: " + failedUrls.size() + " out of " +
        (urlSet.size() + failedUrls.size()) + " could not be resolved.\n\n" +
        "The following URLs could not be " +
            "resolved: "+Util.arrayToString(failedUrls.toArray(), ", ") +
            ".\n\nDo you want to continue?";
        choice = confirmBox.getConfirm("Confirm continue",
            confirmString, new Object[] {"OK",  "Cancel"});        
      }
      catch(java.lang.Exception e){
        Debug.debug("Could not get confirmation, "+e.getMessage(), 1);
      }
      if(choice==0){
        return urlSet;
      }
      else{
        return null;
      }
    }
    else if(urlSet.isEmpty() && !failedUrls.isEmpty()){
      return null;
    }
    else{
      return urlSet;
    }
  }  

  /**
   * List files and/or directories in a *directory* on gridftp server.
   */
  public Vector list(GlobusURL globusUrl, String filter,
      StatusBar statusBar) throws IOException, FTPException{
    globusUrl = new GlobusURL(globusUrl.getURL().replaceFirst("/[^\\/]*/\\.\\.", ""));
    String localPath = "/";
    if(globusUrl.getPath()!=null){
      localPath = globusUrl.getPath().replaceFirst("/[^\\/]*/\\.\\.", "");
      if(!localPath.startsWith("/")){
        localPath = "/" + localPath;
      }
    } 
    String host = globusUrl.getHost();
    int port = globusUrl.getPort();
    if(port<0){
      port = 2811;
    }
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    Object [] textArr = null;
    GridFTPClient gridFtpClient = null;
    try{
      gridFtpClient = connect(host, port);
      
      if(localPath.length()>1){
        gridFtpClient.changeDir(localPath.substring(0, localPath.length()-1));
      }
      Debug.debug("Current dir: "+gridFtpClient.getCurrentDir(), 3);
      final ByteArrayOutputStream received2 = new ByteArrayOutputStream(100000);
      DataSink dataSink = new DataSink(){
        public void write(Buffer buffer) 
        throws IOException{
          Debug.debug("received " + buffer.getLength() +
             " bytes of directory listing", 1);
             received2.write(buffer.getBuffer(),
             0,
             buffer.getLength());
          return;
        }
        public void close() throws IOException{
          Debug.debug("closing", 1);
        };                
      };
      gridFtpClient.list("", "", dataSink);
      textArr = Util.split(received2.toString(), "\n");
      Vector textVector = new Vector();
      String [] entryArr = null;
      String line = null;
      String fileName = null;
      String bytes = null;
      int directories = 0;
      int files = 0;
      boolean onlyDirs = false;
      if(filter==null || filter.equals("")){
        filter = "*";
      }
      else{
        onlyDirs = filter.endsWith("/");
        if(onlyDirs){
          filter = filter.substring(0, filter.length()-1);
        }
      }
      if(statusBar!=null){
        statusBar.setLabel("Filtering...");
      }
      Debug.debug("Filter is "+filter, 3);
      filter = filter.replaceAll("\\.", "\\\\.");
      filter = filter.replaceAll("\\*", ".*");
      Debug.debug("Filtering with "+filter, 3);
      for(int i=0; i<textArr.length; ++i){
        line = textArr[i].toString();
        Debug.debug(line, 3);
        // Here we are assuming that there are no file names with spaces
        // on the gridftp server...
        // TODO: improve
        if(line.length()==0){
          continue;
        }
        else{
          entryArr = Util.split(line);
          fileName = entryArr[entryArr.length-1];
          bytes = entryArr[entryArr.length-2];
        }
        // If server is nice enough to provide file information, use it
        if(fileName.matches(filter)){
          if(line.matches("d[rwxsS-]* .*")){
            textVector.add(fileName+"/");
            ++directories;
            continue;
          }
          else if(!onlyDirs && line.matches("-[rwxsS-]* .*")){
            textVector.add(fileName+" "+bytes);
            ++files;
            continue;
          }
          try{
            // File information not provided, we have to do it the slow way...
            // Check if fileName it is a directory by trying to cd into it
            Debug.debug("Checking file/directory information the slow way...", 2);
            gridFtpClient.changeDir(fileName);
            gridFtpClient.goUpDir();
            textVector.add(fileName+"/");
            ++directories;
          }
          catch(Exception e){
            if(onlyDirs){
              continue;
            }
            textVector.add(fileName+" "+bytes);
            ++files;
          }
        }
      }
      if(statusBar!=null){
        statusBar.setLabel(directories+" directories, "+files+" files");
      }
      return textVector;
    }
    catch(FTPException e){
      Debug.debug("oops, failed listing!", 2);
      throw e;
    }
    finally{
      try{
        gridFtpClient.close();
      }
      catch(Exception e){
      }
    }
  }
  
  public long getFileBytes(GlobusURL url) throws Exception {
    String file = url.getPath().replaceFirst(".*/([^/]+)", "$1");
    String dir = url.getURL().replaceFirst("(.*/)[^/]+", "$1");
    Vector listVector = list(new GlobusURL(dir), file, null);
    String line = (String) listVector.get(0);
    String [] entries = Util.split(line);
    return Long.parseLong(entries[1]);
  }
  
  // This should work, but NG servers do not support getSize...
  public long getFileBytesNoGood(GlobusURL url) throws Exception {
    GridFTPClient gridFtpClient = null;
    try{
      gridFtpClient = connect(url.getHost(), url.getPort());
    }
    catch(FTPException e){
      Debug.debug("Could not connect to "+url.getURL()+". "+
         e.getMessage(), 1);
      throw e;
    }
    return gridFtpClient.getSize(url.getPath());
  }
  
  private Date makeDate(String dateInput){
    Date date = null;
    try{
      SimpleDateFormat df = new SimpleDateFormat(GridPilot.dateFormatString);
      date = df.parse(dateInput);
    }
    catch(Throwable e){
      Debug.debug("Could not set date. "+e.getMessage(), 1);
      e.printStackTrace();
    }
    return date;
  }

  private String makeDateString(Date date){
    String dateString =  null;
    try{
      SimpleDateFormat df = new SimpleDateFormat(GridPilot.dateFormatString);
      dateString = df.format(date);
    }
    catch(Throwable e){
      Debug.debug("Could not parse date. "+e.getMessage(), 1);
      e.printStackTrace();
    }
    return dateString;
  }

  /**
   * Checks if a given source has already been downloaded and is still there
   * and if so, if the source has changed since the download (date and size).
   */
  private boolean checkCache(GridFTPClient client, UrlCopy urlCopy){
    File destinationFile = new File(urlCopy.getDestinationUrl().getPath());
    File cacheInfoDir = new File(destinationFile.getParentFile().getAbsolutePath(), ".gridpilot_info");
    File cacheInfoFile = new File(cacheInfoDir, destinationFile.getName());
    long cachedSize = -1;
    Date cachedDate = null;
    boolean cacheOk = false;
    try{
      if(!cacheInfoDir.exists()){
        cacheInfoDir.mkdir();
      }
      long fileSize = client.getSize(urlCopy.getDestinationUrl().getPath());
      Date modificationDate = client.getLastModified(urlCopy.getDestinationUrl().getPath());
      if(cacheInfoFile.exists()){
        // Parse the file. It has the format:
        // date: <date>
        // size: <size>
        RandomAccessFile cacheRAF = new RandomAccessFile(cacheInfoFile.getAbsolutePath(), "rw");
        String line = "";
        while(line!=null){
           line = cacheRAF.readLine();
           if(line.startsWith("date: ")){
             cachedDate = makeDate(line.replaceFirst("date: (.*)", "$1"));
           }
           if(line.startsWith("size: ")){
             cachedSize = Long.parseLong(line.replaceFirst("size: (.*)", "$1"));
           }
        }
        cacheRAF.close();
      }
      if(destinationFile.exists() && cachedDate!=null && cachedSize>-1){
        if(modificationDate!=null && fileSize>-1){
          if(cachedDate.equals(modificationDate) && cachedSize==fileSize){
            cacheOk = true;
          }
          else{
            // if the file is there, but not up to date, move it out of the way
            try{
              destinationFile.renameTo(new File(destinationFile.getAbsolutePath()+".bk"));
            }
            catch(Exception e){
            }
          }
        }
      }
      if(!cacheOk && modificationDate!=null && fileSize>-1){
        // write the file size and modification date to .gridpilot_cache/.<file name>
        LocalStaticShellMgr.writeFile(cacheInfoFile.getAbsolutePath(),
            "date: "+makeDateString(modificationDate), false);
        LocalStaticShellMgr.writeFile(cacheInfoFile.getAbsolutePath(),
            "size: "+Long.toString(fileSize), true);
      }
    }
    catch(Exception e){
      cacheOk = false;
      e.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: problem checking cache. File will be downloaded.", e);
    }
    return cacheOk;
  }

  public String[] startCopyFiles(GlobusURL[] srcUrls, GlobusURL[] destUrls)
     throws UrlCopyException {
    Debug.debug("", 2);
    UrlCopyTransferListener urlCopyTransferListener = null;
    String [] ret = new String[srcUrls.length];
    GridFTPClient srcClient = null;
    Debug.debug("Copying "+srcUrls.length+" files", 2);
    for(int i=0; i<srcUrls.length; ++i){
      try{
        urlCopyTransferListener = new UrlCopyTransferListener();
        final UrlCopy urlCopy = new UrlCopy();
        urlCopy.setSourceUrl(srcUrls[i]);
        urlCopy.setDestinationUrl(destUrls[i]);
        if(srcUrls[i].getProtocol().equalsIgnoreCase("gsiftp") &&
            destUrls[i].getProtocol().equalsIgnoreCase("gsiftp")){
          urlCopy.setUseThirdPartyCopy(true);
          srcClient = connect(srcUrls[i].getHost(), srcUrls[i].getPort());
          GridFTPClient dstClient = connect(destUrls[i].getHost(), destUrls[i].getPort());
          Authorization srcAuth = srcClient.getAuthorization();
          Authorization dstAuth = dstClient.getAuthorization();
          //urlCopy.setDCAU(true);
          urlCopy.setSourceAuthorization(srcAuth);
          urlCopy.setDestinationAuthorization(dstAuth);
          srcClient.close();
          dstClient.close();
        }
        urlCopy.addUrlCopyListener(urlCopyTransferListener);
        // The transfer id is chosen to be "gsiftp-{get|put|copy}::'srcUrl' 'destUrl'"
        final String id = pluginName + "-copy::'" + srcUrls[i].getURL()+"' '"+destUrls[i].getURL()+"'";
        Debug.debug("ID: "+id, 3);
        jobs.put(id, urlCopy);
        urlCopyTransferListeners.put(id, urlCopyTransferListener);
        ret[i] = id;
        final GridFTPClient checkClient = srcClient;
        Thread t = new Thread(){
          public void run(){
            try{
              // Check if file is cached and the cache is up to date
              if(urlCopy.getSourceUrl().getProtocol().equalsIgnoreCase("gsiftp") &&
                  urlCopy.getDestinationUrl().getProtocol().equalsIgnoreCase("file")){
                Debug.debug("Checking cache...", 2);
                if(checkCache(checkClient, urlCopy)){
                  Debug.debug("Cache ok, not starting the actual transfer...", 2);
                  //urlCopy.cancel();
                  ((UrlCopyTransferListener) urlCopyTransferListeners.get(id)).transferCompleted();
                }
                else{
                  // Start the transfer.
                  Debug.debug("Starting the actual transfer...", 2);
                  urlCopy.copy();
                  ((UrlCopyTransferListener) urlCopyTransferListeners.get(id)).transferCompleted();
                }
              }
            }
            catch(UrlCopyException ue){
              try{
                ue.printStackTrace();
                GridPilot.getClassMgr().getLogFile().addMessage((ue instanceof Exception ? "Exception" : "Error") +
                    " from plugin gsiftp" +
                    " while starting download", ue);
                ((UrlCopyTransferListener) urlCopyTransferListeners.get(id)).transferError(ue);
                //this.finalize();
              }
              catch(Throwable ee){
              }
            }
          }
        };
        t.start();
      }
      catch(Exception e){
        e.printStackTrace();
        // cancel all jobs
        for(int j=0; j<=i; ++j){
          try{
            cancel(srcUrls[j]+" "+destUrls[j]);
          }
          catch(Exception ee){
          }
        }
        throw new UrlCopyException("A copy job failed starting, " +
            "cancelled this batch. "+e.getMessage());
      }
    }
    return ret;
  }

  public String getStatus(String fileTransferID) throws Exception {
    // Wait, Transfer, Error, Done
    Debug.debug("Getting status for transfer "+fileTransferID, 2);
    Debug.debug("urlCopyTransferListeners: "+
        Util.arrayToString(urlCopyTransferListeners.entrySet().toArray()), 2);
    String ret = ((UrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getStatus();
    Debug.debug("Got status "+ret, 2);
    // TODO: consider returning "Wait" instead of "Error" to avoid the initial "errors".
    if(ret==null){
      ret = "Error";
    }
    return ret;
  }

  public String getFullStatus(String fileTransferID) throws Exception {
    String ret = "Status: "+((UrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getStatus();
    String error = ((UrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getError();
    if(error!=null && error.length()>0){
      ret += "\nError: "+error;
    }
    return ret;
  }

  public int getPercentComplete(String fileTransferID) throws Exception {
    long comp = ((UrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getPercentComplete();
    Debug.debug("Got percent complete "+comp, 3);
    return (int) comp;
  }

  public long getBytesTransferred(String fileTransferID) throws Exception {
    long comp = ((UrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getBytesTransferred();
    return comp;
  }

  public void cancel(String fileTransferID) throws Exception {
    if(!((UrlCopy) jobs.get(fileTransferID)).isCanceled()){
      Debug.debug("Cancelling gsiftp transfer "+fileTransferID, 2);
      ((UrlCopy) jobs.get(fileTransferID)).cancel();
      jobs.remove(fileTransferID);
    }
  }

  public void finalize(String fileTransferID) throws Exception {
    jobs.remove(fileTransferID);
  }

  public void deleteFile(GlobusURL destUrl) throws Exception {
    deleteFiles(new GlobusURL [] {destUrl});
  }

  /**
   * Maps
   * 
   * Wait, Transfer, Error, Done
   * -> 
   * FileTransfer.STATUS_DONE, FileTransfer.STATUS_ERROR, FileTransfer.STATUS_FAILED,
   * FileTransfer.STATUS_RUNNING, FileTransfer.STATUS_WAIT
   */
  public int getInternalStatus(String id, String ftStatus) throws Exception{
    Debug.debug("Mapping status "+ftStatus, 2);
    int ret = -1;
    if(ftStatus==null || ftStatus.equals("")){
      // TODO: Should this be STATUS_ERROR?
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Error")){
      ret = FileTransfer.STATUS_ERROR;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Cancelled")){
      ret = FileTransfer.STATUS_ERROR;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Wait")){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Transfer")){
      ret = FileTransfer.STATUS_RUNNING;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Done")){
      ret = FileTransfer.STATUS_DONE;
    }
    return ret;
  }

}
