package gridpilot.ftplugins.https;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Vector;

import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.ServerException;
import org.globus.io.urlcopy.UrlCopy;
import org.globus.io.urlcopy.UrlCopyException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;

import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.ResThread;
import gridpilot.GridPilot;
import gridpilot.StatusBar;
import gridpilot.MyUtil;

public class HTTPSFileTransfer implements FileTransfer {
  
  private String user = null;
  private HashMap jobs = null;
  private HashMap urlCopyTransferListeners = null;
  private HashMap fileTransfers = null;
  
  // "Thu, 07 Jun 2007 20:37:24 GMT"
  private static String GMT_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss z";
  // "2008-08-20 09:38:24"
  private static String HTTP_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static String PLUGIN_NAME;
  private static int COPY_TIMEOUT = 10000;

  protected final static String STATUS_WAIT = "Wait";
  protected final static String STATUS_TRANSFER = "Transfer";
  protected final static String STATUS_DONE = "Done";
  protected final static String STATUS_ERROR = "Error";


  public HTTPSFileTransfer() throws IOException, GeneralSecurityException{
    PLUGIN_NAME = "https";
    if(!GridPilot.firstRun){
      Debug.debug("getting identity", 3);
      user = GridPilot.getClassMgr().getSSL().getGridSubject();
    }
    jobs = new HashMap();
    urlCopyTransferListeners = new HashMap();
    fileTransfers = new HashMap();
  }

  public String getUserInfo(){
    return user;
  }
  
  public boolean checkURLs(GlobusURL [] srcUrls, GlobusURL [] destUrls){
    String firstSrcProtocol = srcUrls[0].getProtocol();
    String firstDestProtocol = destUrls[0].getProtocol();
    Debug.debug("srcUrls.length: "+srcUrls.length, 3);
    Debug.debug("destUrls.length: "+destUrls.length, 3);
    Debug.debug("firstSrcProtocol: "+firstSrcProtocol, 3);
    Debug.debug("firstDestProtocol: "+firstDestProtocol, 3);
    if(srcUrls.length!=destUrls.length || !(
        firstSrcProtocol.equalsIgnoreCase("https") &&
        firstDestProtocol.equalsIgnoreCase("file") ||
           firstSrcProtocol.equalsIgnoreCase("file") &&
           firstDestProtocol.equalsIgnoreCase("https")
          )){
      return false;
    }
    for(int i=0; i<srcUrls.length; ++i){
      if(!firstSrcProtocol.equalsIgnoreCase(srcUrls[0].getProtocol()) ||
             !firstDestProtocol.equalsIgnoreCase(destUrls[0].getProtocol())){
        return false;
      }
    }
    return true;
  }

  /**
   * Connect to server and set environment.
   * This method must be synchronized: before there were problems with
   * simultaneous GridFactory submissions, i.e. connecting in parallel to the same
   * host.
   */
  private synchronized MyUrlCopy myConnect(GlobusURL srcUrl, GlobusURL destUrl) throws IOException{
    
    MyUrlCopy urlCopy = null;
    
    try{
      GridPilot.getClassMgr().getSSL().activateSSL();
      GSSCredential credential = GridPilot.getClassMgr().getSSL().getCredential();
      urlCopy = new MyUrlCopy();
      urlCopy.setSourceUrl(srcUrl);
      urlCopy.setDestinationUrl(destUrl);
      if(srcUrl.getProtocol().equalsIgnoreCase("https") &&
          destUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setUseThirdPartyCopy(true);
      }
      urlCopy.setSourceCredentials(credential);
      urlCopy.setDestinationCredentials(credential);
      /*if(srcUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setSourceCredentials(credential);
        urlCopy.setSourceAuthorization(new IdentityAuthorization(
            ((GlobusGSSCredentialImpl)credential).getGlobusCredential().getIdentity()));
      }
      if(destUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setDestinationCredentials(credential);
        urlCopy.setDestinationAuthorization(new IdentityAuthorization(
            ((GlobusGSSCredentialImpl)credential).getGlobusCredential().getIdentity()));
      }
      urlCopy.setSourceAuthorization(null);
      urlCopy.setDestinationAuthorization(null);*/
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
   * simultaneous GridFactory submissions, i.e. connecting in parallel to the same
   * host.
   */
  private synchronized MyUrlCopy myConnect(GlobusURL srcUrl) throws IOException{
    
    Debug.debug("Connecting to "+srcUrl.getURL(), 2);
    
    MyUrlCopy urlCopy = null;
    
    try{
      GridPilot.getClassMgr().getSSL().activateSSL();
      urlCopy = new MyUrlCopy();
      urlCopy.setSourceUrl(srcUrl);
      GSSCredential credential = GridPilot.getClassMgr().getSSL().getCredential();
      if(srcUrl.getProtocol().equalsIgnoreCase("https")){
        urlCopy.setSourceCredentials(credential);
        urlCopy.setDestinationCredentials(credential);
        //urlCopy.setSourceAuthorization(null);
        //urlCopy.setSourceAuthorization(new IdentityAuthorization(
        //"/O=GRID-FR/C=CH/O=CSCS/OU=CC-LCG/CN=grid00.unige.ch"));
        //urlCopy.setDestinationAuthorization(new IdentityAuthorization(
        //    ((GlobusGSSCredentialImpl)credential).getGlobusCredential().getIdentity()));
        //urlCopy.setDestinationAuthorization(HostAuthorization.getInstance());
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
   * Quick and dirty method to just get a file - bypassing
   * caching, queueing and monitoring. Notice, that it does NOT
   * start a separate thread.
   */
  public void getFile(final GlobusURL globusUrl, File downloadDirOrFile,
      final StatusBar statusBar) throws Exception {
    
    // TODO: implement wildcard
    
    if(globusUrl.getURL().endsWith("/")){
      throw new IOException("ERROR: cannot download a directory. ");
    }
    
    Debug.debug("Get "+globusUrl.getURL(), 3);

    final String id = globusUrl.getURL()+"::"+downloadDirOrFile.getAbsolutePath();
    
    Debug.debug("Getting "+globusUrl.getURL(), 3);
    (new ResThread(){
      public void run(){
        if(statusBar!=null){
          statusBar.setLabel("Getting "+globusUrl.getURL());
        }
      }
    }).run();               

    File downloadFile = null;
    String fileName = globusUrl.getPath().replaceFirst(".*/([^/]+)", "$1");
    if(downloadDirOrFile.isDirectory()){
      downloadFile = new File(downloadDirOrFile.getAbsolutePath(), fileName);
    }
    else{
      downloadFile = downloadDirOrFile;
    }
        
    // Leave this outside of thread to avoid deadlock when querying for password.
    GridPilot.getClassMgr().getSSL().activateSSL();

    final MyUrlCopy urlCopy = myConnect(globusUrl, new GlobusURL("file:///"+downloadFile.getAbsolutePath()));
    fileTransfers.put(id, urlCopy);

    Debug.debug("Downloading "+globusUrl.getURL()+"->"+downloadFile.getAbsolutePath(), 3);
    ResThread t = new ResThread(){
      public void run(){
        try{
          urlCopy.copy();
        }
        catch(UrlCopyException e){
          this.setException(e);
          e.printStackTrace();
        }
      }
    };
    t.start();
    if(!MyUtil.myWaitForThread(t, "https", COPY_TIMEOUT, "getFile")){
      if(statusBar!=null){
        statusBar.setLabel("Download cancelled");
      }
      return;
    }
    if(t.getException()!=null){
      if(statusBar!=null){
        statusBar.setLabel("Download failed");
      }
      Debug.debug("Download failed. "+t.getException().getMessage(), 2);
      throw t.getException();
    }
   
    // if we didn't get an exception, the file got downloaded
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
  public void putFile(File file, final GlobusURL globusFileUrl,
      final StatusBar statusBar) throws Exception{
    
    final String id = file.getAbsolutePath() +"::"+ globusFileUrl.getURL();
    
    (new ResThread(){
      public void run(){
        if(statusBar!=null){
          statusBar.setLabel("Uploading "+globusFileUrl.getURL());
        }
      }
    }).run();               

    String fileName = file.getName();
    GlobusURL uploadUrl = null;
    if(globusFileUrl.getURL().endsWith("/")){
      uploadUrl = new GlobusURL(globusFileUrl.getURL()+fileName);
    }
    else{
      uploadUrl = globusFileUrl;
    }
    GlobusURL fileURL = new GlobusURL("file:///"+file.getCanonicalPath());
    Debug.debug("put "+fileURL.getURL()+" --> "+uploadUrl.getURL(), 3);
 
    // Leave this outside of thread to avoid deadlock when querying for password.
    GridPilot.getClassMgr().getSSL().activateSSL();

    final MyUrlCopy urlCopy = myConnect(fileURL, uploadUrl);

    ResThread t = new ResThread(){
      public void run(){
        try{
          fileTransfers.put(id, urlCopy);
          urlCopy.myCopy();
        }
        catch(Exception e){
          this.setException(e);
        }
      }
    };
    t.start();
    if(!MyUtil.myWaitForThread(t, "https", COPY_TIMEOUT, "putFile")){
      if(statusBar!=null){
        statusBar.setLabel("Upload cancelled");
      }
      return;
    }
    if(t.getException()!=null){
      if(statusBar!=null){
        statusBar.setLabel("Upload failed");
      }
      throw t.getException();
    }

    // if we didn't get an exception, the file got written.
    if(statusBar!=null){
      statusBar.setLabel("Upload done");
    }
    Debug.debug("File or directory "+globusFileUrl.getURL()+" written.", 2);
  }
  
  /**
   * Cancels a running transfer from fileTransfers.
   * These are transfers initiated by getFile or putFile.
   * @param id the ID of the transfer.
   * @throws IOException 
   * @throws ServerException 
   */
  public void abortTransfer(String id) throws ServerException, IOException{
    MyUrlCopy urlCopy = ((MyUrlCopy) fileTransfers.get(id));
    urlCopy.cancel();
    fileTransfers.remove(id);
  }
  
  /**
   * Delete files on server. They MUST all be on the same server.
   */
  public void deleteFiles(GlobusURL [] globusUrls) throws
     IOException, UrlCopyException{
    
    Debug.debug("delete "+MyUtil.arrayToString(globusUrls), 3);
    
    for(int i=0; i<globusUrls.length; ++i){
      try{
        deleteFile(globusUrls[i]);
      }
      catch(Exception e){
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: Could not delete "+globusUrls[i], e);
        e.printStackTrace();
      }
      // if we didn't get an exception, the file got deleted
      Debug.debug("File or directory "+globusUrls[i].getURL()+" deleted.", 2);
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
        LocalStaticShell.writeFile(tmpFile.getAbsolutePath(), text, false);
        Debug.debug("Created temp file "+tmpFile, 3);
        String fileName = globusUrl.getPath().replaceFirst(".*/([^/]+)", "$1");
        Debug.debug("Uploading "+tmpFile.getAbsolutePath()+" --> "+fileName, 3);
        putFile(tmpFile, globusUrl, null);
        tmpFile.delete();
      }
      else{
        throw new IOException("ERROR: Cannot write text to a directory.");
      }
      // if we didn't get an exception, the file got written...
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
      String [] arr = MyUtil.split(line);
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
      SimpleDateFormat df = new SimpleDateFormat(GMT_DATE_FORMAT, Locale.US);
      date = df.parse(dateInput);
    }
    catch(Throwable e){
      try{
        SimpleDateFormat df = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        date = df.parse(dateInput);
      }
      catch(Throwable ee){
        Debug.debug("Could not set date. "+e.getMessage(), 1);
        ee.printStackTrace();
      }
    }
    return date;
  }

  private String makeDateString(Date date){
    String dateString =  null;
    try{
      SimpleDateFormat df = new SimpleDateFormat(GridPilot.dateFormatString, Locale.US);
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
      if(cacheInfoFile.exists()){
        // Parse the file. It has the format:
        // date: <date>
        // size: <size>
        RandomAccessFile cacheRAF = new RandomAccessFile(cacheInfoFile.getAbsolutePath(), "rw");
        String line = "";
        while(line!=null){
           line = cacheRAF.readLine();
           if(line==null){
             break;
           }
           if(line.startsWith("date: ")){
             cachedDate = makeDate(line.replaceFirst("^date: (.*)", "$1"));
           }
           if(line.startsWith("size: ")){
             cachedSize = Long.parseLong(line.replaceFirst("^size: (.*)", "$1"));
           }
        }
        cacheRAF.close();
      }
      long fileSize = -1;
      try{
        fileSize = getContentLength(urlCopy.getSourceUrl());
      }
      catch(Exception e){
      }
      Date modificationDate = null;
      try{
        modificationDate = getLastModified(urlCopy.getSourceUrl());
      }
      catch(Exception ee){
      }
      if(destinationFile.exists() && cachedDate!=null && cachedSize>-1){
        if(modificationDate!=null && fileSize>-1){
          long localSize = destinationFile.length();
          if(cachedDate.equals(modificationDate) && cachedSize==fileSize &&
              localSize==cachedSize){
            cacheOk = true;
          }
          else{
            Debug.debug("Cache not ok "+cachedSize+":"+fileSize+":"+localSize+
                " --- "+cachedDate+":"+modificationDate, 2);
            // if the file is there, but not up to date, move it out of the way
            try{
              destinationFile.renameTo(new File(destinationFile.getAbsolutePath()+".old"));
            }
            catch(Exception e){
            }
          }
        }
      }
      if(!cacheOk && modificationDate!=null && fileSize>-1){
        // write the file size and modification date to .gridpilot_cache/.<file name>
        LocalStaticShell.writeFile(cacheInfoFile.getAbsolutePath(),
            "date: "+makeDateString(modificationDate)+"\n", false);
        LocalStaticShell.writeFile(cacheInfoFile.getAbsolutePath(),
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
  
  private HashMap<String, String> getHttpHeaders(URL url) throws IOException{

    URLConnection c = url.openConnection();
    HashMap<String, String> headers = new HashMap<String, String>();
  
    for(int i=0; ; i++){
      String name = c.getHeaderFieldKey(i);
      String value = c.getHeaderField(i);
      if(name==null && value==null){
        break;         
      }
      if(name == null){
        // First line of headers
        Debug.debug("Server HTTP version, Response code: "+value, 3);
      }
      else{
        Debug.debug(name.toLowerCase()+": "+value, 3);
        headers.put(name.toLowerCase(), value);
      }
    }
    return headers;
  }

  private Date getLastModified(GlobusURL globusUrl) throws IOException, UrlCopyException{
    try{
      String date = null;
      date = getHttpHeaders(new URL(globusUrl.getURL())).get("last-modified");
      if(date!=null){
        return makeDate(date);
      }
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not get " +
            "modification date of "+globusUrl, e);
      e.printStackTrace();
    }
    return null;
  }

  private long getContentLength(GlobusURL globusUrl) throws IOException, UrlCopyException{
    try{
      String lnStr = null;
      lnStr = getHttpHeaders(new URL(globusUrl.getURL())).get("content-length");
      if(lnStr!=null){
        return Long.parseLong(lnStr);
      }
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not get " +
            "content length of "+globusUrl, e);
      e.printStackTrace();
    }
    return -1L;
  }

  private Date getLastModifiedWebDav(GlobusURL globusUrl) throws IOException, UrlCopyException{
    try{
      String path = "/"+globusUrl.getPath();
      MyUrlCopy urlCopy = myConnect(globusUrl);
      urlCopy.execute("PROPFIND");
      String res = urlCopy.getResult();
      String [] lines = MyUtil.split(res, "(?s)[\n\r]");
      String hrefPattern = "(?i)<d:href>"+path+"</d:href>";
      String datePattern = "(?i)<lp1:getlastmodified>(.*)</lp1:getlastmodified>";
      String date = "";
      boolean ok = false;
      for(int i=0; i<lines.length; ++i){
        if(lines[i].matches(hrefPattern)){
          ok = true;
          Debug.debug("Line: "+lines[i], 3);
        }
        if(ok && lines[i].matches(datePattern)){
          date += lines[i].replaceFirst(datePattern, "$1").trim();
          Debug.debug("Date: "+date, 2);
        }
      }
      if(date!=null){
        return makeDate(date);
      }
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("WARNING: could not get " +
            "modification date of "+globusUrl, e);
      e.printStackTrace();
    }
    return null;
  }

  public String[] startCopyFiles(GlobusURL[] srcUrls, GlobusURL[] destUrls)
     throws UrlCopyException {
    Debug.debug("", 2);
    MyUrlCopyTransferListener urlCopyTransferListener = null;
    String [] ret = new String[srcUrls.length];
    Debug.debug("Copying "+srcUrls.length+" files", 2);
    for(int i=0; i<srcUrls.length; ++i){
      try{
        final MyUrlCopy urlCopy = myConnect(srcUrls[i], destUrls[i]);
        urlCopyTransferListener = new MyUrlCopyTransferListener();
        urlCopy.addUrlCopyListener(urlCopyTransferListener);
        // The transfer id is chosen to be "https-{get|put|copy}::'srcUrl' 'destUrl'"
        final String id = PLUGIN_NAME + "-copy::'" + srcUrls[i].getURL()+"' '"+destUrls[i].getURL()+"'";
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
                  ((MyUrlCopyTransferListener) urlCopyTransferListeners.get(id)).transferCompleted();
                }
                else{
                  // Start the transfer.
                  Debug.debug("Starting the actual transfer...", 2);
                  urlCopy.copy();
                  ((MyUrlCopyTransferListener) urlCopyTransferListeners.get(id)).transferCompleted();
                }
              }
            }
            catch(UrlCopyException ue){
              try{
                ue.printStackTrace();
                GridPilot.getClassMgr().getLogFile().addMessage((ue instanceof Exception ? "Exception" : "Error") +
                    " from plugin https" +
                    " while starting download", ue);
                ((MyUrlCopyTransferListener) urlCopyTransferListeners.get(id)).transferError(ue);
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
    Debug.debug("returning IDs: "+MyUtil.arrayToString(ret), 3);
    return ret;
  }

  public String getStatus(String fileTransferID) throws Exception {
    // Wait, Transfer, Error, Done
    Debug.debug("Getting status for transfer "+fileTransferID, 2);
    Debug.debug("urlCopyTransferListeners: "+
        MyUtil.arrayToString(urlCopyTransferListeners.entrySet().toArray()), 2);
    String ret = ((MyUrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getStatus();
    Debug.debug("Got status "+ret, 2);
    // TODO: consider returning "Wait" instead of "Error" to avoid the initial "errors".
    if(ret==null){
      ret = STATUS_ERROR;
    }
    return ret;
  }

  public String getFullStatus(String fileTransferID) throws Exception {
    String ret = "Status: "+((MyUrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getStatus();
    String error = ((MyUrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getError();
    if(error!=null && error.length()>0){
      ret += "\nError: "+error;
    }
    return ret;
  }

  public int getPercentComplete(String fileTransferID) throws Exception {
    long comp = ((MyUrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getPercentComplete();
    Debug.debug("Got percent complete "+comp, 3);
    return (int) comp;
  }

  public long getBytesTransferred(String fileTransferID) throws Exception {
    long comp = ((MyUrlCopyTransferListener) 
        urlCopyTransferListeners.get(fileTransferID)).getBytesTransferred();
    return comp;
  }

  public void cancel(String fileTransferID) throws Exception {
    if(!((UrlCopy) jobs.get(fileTransferID)).isCanceled()){
      Debug.debug("Cancelling https transfer "+fileTransferID, 2);
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
    else if(ftStatus==null || ftStatus.equalsIgnoreCase(STATUS_ERROR)){
      ret = FileTransfer.STATUS_ERROR;
    }
    // Not used I believe...
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Cancelled")){
      ret = FileTransfer.STATUS_ERROR;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase(STATUS_WAIT)){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase(STATUS_TRANSFER)){
      ret = FileTransfer.STATUS_RUNNING;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase(STATUS_DONE)){
      ret = FileTransfer.STATUS_DONE;
    }
    return ret;
  }

  /**
   * Parses the XML returned by webdav for PROPFIND
   * ("Depth: 1" is printed in the header by MyHTTPProtocol).
   */
  public Vector list(GlobusURL globusUrl, String filter, StatusBar statusBar) throws Exception {

    String baseDir = "";
    
    // jglobus does not like URLs like https://grid00.unige.ch/
    // - we append ./
    if(globusUrl.getURL().endsWith("/") && globusUrl.getPath()==null){
      globusUrl = new GlobusURL(globusUrl.getURL()+"./");
      baseDir = "^/";
    }
    else{
      baseDir = "^/"+globusUrl.getPath();
    }
    
    final GlobusURL url = globusUrl;
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
    
    filter = filter.replaceAll("\\.", "\\\\.");
    filter = filter.replaceAll("\\*", ".*");
    Debug.debug("Filtering with "+filter, 3);
    Debug.debug("Using baseDir "+baseDir, 2);
    
    //  Leave this outside of thread to avoid deadlock when querying for password.
    GridPilot.getClassMgr().getSSL().activateSSL();
    
    ResThread t = new ResThread(){
      String res = null;
      MyUrlCopy urlCopy = null;
      public void run(){
        try{
          urlCopy = myConnect(url);
          urlCopy.execute("PROPFIND");
        }
        catch(Exception e){
          e.printStackTrace();
          this.setException(e);
          return;
        }
        res = urlCopy.getResult();
        Debug.debug("List result: ", 2);
      }
      public String getStringRes(){
        return res;
      }
    };
    t.start();
    if(!MyUtil.myWaitForThread(t, "https", COPY_TIMEOUT, "list", new Boolean(true))){
      if(statusBar!=null){
        statusBar.setLabel("List cancelled");
      }
      throw new IOException("List timed out");
    }
    if(t.getException()!=null){
      if(statusBar!=null){
        statusBar.setLabel("List failed");
      }
      throw t.getException();
    }
    String res = t.getStringRes();
    String [] lines = MyUtil.split(res, "(?s)[\n\r]");
    Vector resVec = new Vector();
    String hrefPattern = "(?i)<d:href>(.*)</d:href>";
    String sizePattern = "(?i)<lp1:getcontentlength>(.*)</lp1:getcontentlength>";
    String line = null;
    int directories = 0;
    int files = 0;
    for(int i=0; i<lines.length; ++i){
      if(lines[i].matches(hrefPattern)){
        line = lines[i].replaceFirst(hrefPattern, "$1").trim();
        line = line.replaceFirst(baseDir, "");
        Debug.debug("Line: "+lines[i], 3);
        if(line.length()==0 || line.equals("/")){
          continue;
        }
        if(line.endsWith("/") && line.matches(filter)){
          line += " " + "4096";
          resVec.add(line);
          ++directories;
        }
      }
      if(line!=null && !onlyDirs && line.matches(filter) && lines[i].matches(sizePattern)){
        line += " " + lines[i].replaceFirst(sizePattern, "$1").trim();
        line = line.replaceFirst(baseDir, "");
        Debug.debug("Line: "+lines[i], 2);
        if(line.length()==0){
          continue;
        }
        resVec.add(line);
        ++files;
      }
    }
    if(statusBar!=null){
      statusBar.setLabel(directories+" directories, "+files+" files");
    }
    return resVec;
  }

  public void getFile(GlobusURL arg0, File arg1) throws Exception {
    getFile(arg0, arg1, null);
  }

  public Vector list(GlobusURL arg0, String arg1) throws Exception {
    return list(arg0, arg1, null);
  }

  public void putFile(File arg0, GlobusURL arg1) throws Exception {
    putFile(arg0, arg1, null);
  }
  
  public String getName() {
    return PLUGIN_NAME;
  }

}
