package gridpilot.ftplugins.https;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Vector;

import javax.swing.JProgressBar;

import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.ServerException;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.io.urlcopy.UrlCopyException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;

import gridpilot.Debug;
import gridpilot.FileTransfer;
import gridpilot.LocalStaticShellMgr;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.Util;

public class HTTPSFileTransfer implements FileTransfer {
  
  private String user = null;
  private HashMap jobs = null;
  private HashMap urlCopyTransferListeners = null;
  private HashMap fileTransfers = null;
  
  private static String pluginName;
  
  public HTTPSFileTransfer(){
    pluginName = "https";
    if(!GridPilot.firstRun){
      Debug.debug("getting identity", 3);
      user = Util.getGridSubject();
    }
    jobs = new HashMap();
    urlCopyTransferListeners = new HashMap();
    fileTransfers = new HashMap();
  }

  public String getUserInfo(){
    return user;
  }
  
  public boolean checkURLs(GlobusURL [] srcUrls, GlobusURL [] destUrls){
    Debug.debug("srcUrls.length: "+srcUrls.length, 3);
    Debug.debug("destUrls.length: "+destUrls.length, 3);
    Debug.debug("srcUrls[0].getProtocol(): "+srcUrls[0].getProtocol(), 3);
    Debug.debug("destUrls[0].getProtocol(): "+destUrls[0].getProtocol(), 3);
    return (srcUrls.length==destUrls.length && (
        srcUrls[0].getProtocol().equalsIgnoreCase("https") &&
           destUrls[0].getProtocol().equalsIgnoreCase("file") ||
        srcUrls[0].getProtocol().equalsIgnoreCase("file") &&
           destUrls[0].getProtocol().equalsIgnoreCase("https") ||
        srcUrls[0].getProtocol().equalsIgnoreCase("https") &&
           destUrls[0].getProtocol().equalsIgnoreCase("https")
          ));
  }

  /**
   * Connect to server and set environment.
   * This method must be synchronized: before there were problems with
   * simultaneous GPSS submissions, i.e. connecting in parallel to the same
   * host.
   */
  private synchronized UrlCopy connect(GlobusURL srcUrl, GlobusURL destUrl) throws IOException{
    
    UrlCopy urlCopy = null;
    
    try{
      /*GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      if(srcUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setSourceCredentials(credential);
      }
      if(destUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setDestinationCredentials(credential);
      }*/
      urlCopy = new UrlCopy();
      urlCopy.setSourceUrl(srcUrl);
      urlCopy.setDestinationUrl(destUrl);
      if(srcUrl.getProtocol().equalsIgnoreCase("https") &&
          destUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setUseThirdPartyCopy(true);
      }
    }
    catch(Exception e){
      Debug.debug("Could not connect "+e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    return urlCopy;
  }

  /**
   * Connect to server and set environment with the aim of deleting srcUrl.
   * This method must be synchronized: before there were problems with
   * simultaneous GPSS submissions, i.e. connecting in parallel to the same
   * host.
   */
  private synchronized MyUrlCopy myConnect(GlobusURL srcUrl) throws IOException{
    
    MyUrlCopy urlCopy = null;
    
    try{
      urlCopy = new MyUrlCopy();
      /*GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      if(srcUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setSourceCredentials(credential);
      }*/
      urlCopy.setSourceUrl(srcUrl);
    }
    catch(Exception e){
      Debug.debug("Could not connect "+e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    return urlCopy;
  }

  /**
   * Quick and dirty method to just get a file - bypassing
   * caching, queueing and monitoring. Notice, that it does NOT
   * start a separate thread.
   */
  public void getFile(GlobusURL globusUrl, File downloadDirOrFile,
      StatusBar statusBar, JProgressBar pb)
     throws ClientException, ServerException, UrlCopyException, IOException {
    
    if(globusUrl.getURL().endsWith("/")){
      throw new IOException("ERROR: cannot download a directory. ");
    }
    
    Debug.debug("Get "+globusUrl.getURL(), 3);

    String id = globusUrl.getURL()+"::"+downloadDirOrFile.getCanonicalPath();
    
    File downloadFile = null;
    String fileName = globusUrl.getPath().replaceFirst(".*/([^/]+)", "$1");
    if(downloadDirOrFile.isDirectory()){
      downloadFile = new File(downloadDirOrFile.getAbsolutePath(), fileName);
    }
    else{
      downloadFile = downloadDirOrFile;
    }
    
    UrlCopy urlCopy = connect(globusUrl, new GlobusURL("file:///"+downloadFile.getCanonicalPath()));
    fileTransfers.put(id, urlCopy);

    Debug.debug("Downloading "+globusUrl.getURL()+"->"+downloadFile.getAbsolutePath(), 3);
    urlCopy.copy();
   
    // if we don't get an exception, the file got downloaded
    if(statusBar!=null){
      statusBar.setLabel("Download done");
    }
    Debug.debug(globusUrl.getURL()+" downloaded.", 2);
  }
  
  /**
   * Quick and dirty method to just put a file - bypassing
   * caching, queueing and monitoring. Notice, that it does NOT
   * start a separate thread.
   */
  public void putFile(File file, GlobusURL globusFileUrl,
      StatusBar statusBar, JProgressBar pb) throws UrlCopyException, IOException{
    
    String id = file.getCanonicalPath() +"::"+ globusFileUrl.getURL();
    
    String fileName = file.getName();
    GlobusURL uploadUrl = null;
    if(globusFileUrl.getURL().endsWith("/")){
      uploadUrl = new  GlobusURL(globusFileUrl.getURL()+fileName);
    }
    else{
      uploadUrl = globusFileUrl;
    }
    Debug.debug("put "+uploadUrl.getURL(), 3);
    
    UrlCopy urlCopy = connect(new GlobusURL("file:///"+file.getCanonicalPath()),
        uploadUrl);
    fileTransfers.put(id, urlCopy);
    urlCopy.copy();
    // if we don't get an exception, the file got written...
    Debug.debug("File or directory "+globusFileUrl.getURL()+" written.", 2);
  }
  
  /**
   * Cancels a running transfer from fileTransfers.
   * These are transfers initiated by getFile or putFile.
   * @param id the ID of the transfer.
   * @throws IOException 
   * @throws ServerException 
   */
  private void abortTransfer(String id) throws ServerException, IOException{
    UrlCopy urlCopy = ((UrlCopy) fileTransfers.get(id));
    urlCopy.cancel();
    fileTransfers.remove(id);
  }
  
  /**
   * Delete files on server. They MUST all be on the same server.
   */
  public void deleteFiles(GlobusURL [] globusUrls) throws
     IOException, UrlCopyException{
    
    Debug.debug("delete "+Util.arrayToString(globusUrls), 3);
    
    GlobusURL globusUrl = null;
    for(int i=0; i<globusUrls.length; ++i){
      try{
        deleteFile(globusUrls[i]);
      }
      catch(Exception e){
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: Could not delete "+globusUrls[i], e);
        e.printStackTrace();
      }
      // if we don't get an exception, the file got deleted
      Debug.debug("File or directory "+globusUrl.getURL()+" deleted.", 2);
    }
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
   * @throws Exception 
   */
  public void write(GlobusURL globusUrl, String text)
    throws Exception{
    Debug.debug("write "+globusUrl.getURL(), 3);
    File tmpFile = null;
    try{
      if(globusUrl.getPath().endsWith("/") && (text==null || text.equals(""))){
        mkdir(globusUrl);
      }
      else if(!globusUrl.getPath().endsWith("/")){
        tmpFile = File.createTempFile("gridpilot.", ".txt");
        LocalStaticShellMgr.writeFile(tmpFile.getAbsolutePath(), text, false);
        Debug.debug("Created temp file "+tmpFile, 3);
        String fileName = globusUrl.getPath().replaceFirst(".*/([^/]+)", "$1");
        Debug.debug("Uploading "+tmpFile.getAbsolutePath()+" --> "+fileName, 3);
        putFile(tmpFile, globusUrl, null, null);
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
      Debug.debug("Could not write "+globusUrl, 1);
      throw e;
    }
    finally{
      try{
        tmpFile.delete();
      }
      catch(Exception e){
      }
    }
  }
  
  public long getFileBytes(GlobusURL url) throws Exception {
    try{
      Vector vec = list(url, null, null);
      String line = (String) vec.get(0);
      String [] arr = Util.split(line);
      return Long.parseLong(arr[0]);
    }
    catch(Exception e){
      e.printStackTrace();
      return -1;
    }
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
  private boolean checkCache(UrlCopy urlCopy){
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
      long fileSize = urlCopy.getSourceLength();
      // TODO:
      //Date modificationDate = getLastModified(urlCopy.getDestinationUrl().getPath());
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
        //if(modificationDate!=null && fileSize>-1){
          if(/*cachedDate.equals(modificationDate) &&*/ cachedSize==fileSize){
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
        //}
      }
      if(!cacheOk /*&& modificationDate!=null*/ && fileSize>-1){
        // write the file size and modification date to .gridpilot_cache/.<file name>
        LocalStaticShellMgr.writeFile(cacheInfoFile.getAbsolutePath(),
            "date: "/*+makeDateString(modificationDate)*/, false);
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
    Debug.debug("Copying "+srcUrls.length+" files", 2);
    for(int i=0; i<srcUrls.length; ++i){
      try{
        final UrlCopy urlCopy = connect(srcUrls[i], destUrls[i]);
        urlCopyTransferListener = new UrlCopyTransferListener();
        urlCopy.addUrlCopyListener(urlCopyTransferListener);
        // The transfer id is chosen to be "https-{get|put|copy}::'srcUrl' 'destUrl'"
        final String id = pluginName + "-copy::'" + srcUrls[i].getURL()+"' '"+destUrls[i].getURL()+"'";
        Debug.debug("ID: "+id, 3);
        jobs.put(id, urlCopy);
        urlCopyTransferListeners.put(id, urlCopyTransferListener);
        ret[i] = id;
        Thread t = new Thread(){
          public void run(){
            try{
              // Check if file is cached and the cache is up to date
              if(urlCopy.getSourceUrl().getProtocol().equalsIgnoreCase("https") &&
                  urlCopy.getDestinationUrl().getProtocol().equalsIgnoreCase("file")){
                Debug.debug("Checking cache...", 2);
                if(checkCache(urlCopy)){
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
                    " from plugin https" +
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

  public void deleteFile(GlobusURL srcUrl) throws Exception {
    MyUrlCopy urlCopy = myConnect(srcUrl);
    urlCopy.execute("DELETE");
  }

  public void mkdir(GlobusURL srcUrl) throws Exception {
    MyUrlCopy urlCopy = myConnect(srcUrl);
    urlCopy.execute("MKCOL");
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

  /**
   * Parses the XML returned by webdav for PROPFIND
   * ("Depth: 1" is printed in the header by MyHTTPProtocol).
   */
  public Vector list(GlobusURL globusUrl, String filter, StatusBar statusBar) throws Exception {
    MyUrlCopy urlCopy = myConnect(globusUrl);
    urlCopy.execute("PROPFIND");
    String res = urlCopy.getResult();
    String [] lines = Util.split(res, "(?s)[\n\r]");
    Vector resVec = new Vector();
    String hrefPattern = "(?i)<d:href>(.*)</d:href>";
    String sizePattern = "(?i)<lp1:getcontentlength>(.*)</lp1:getcontentlength>";
    String line = null;
    for(int i=0; i<lines.length; ++i){
      if(lines[i].matches(hrefPattern)){
        line = lines[i].replaceFirst(hrefPattern, "$1").trim();
        if(line.endsWith("/")){
          line += " " + "4096";
          resVec.add(line);
        }
      }
      if(lines[i].matches(sizePattern)){
        line += " " + lines[i].replaceFirst(sizePattern, "$1").trim();
        resVec.add(line);
      }
    }
    return resVec;
  }

}
