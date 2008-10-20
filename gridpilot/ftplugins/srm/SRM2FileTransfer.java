package gridpilot.ftplugins.srm;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Vector;

import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.ServerException;
import org.globus.util.GlobusURL;

import gov.lbl.srm.client.wsdl.SRMRequest;
import gov.lbl.srm.client.wsdl.SRMRequestStatus;
import gov.lbl.srm.client.wsdl.SRMServer;

import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalStaticShell;
import gridpilot.MySSL;
import gridpilot.StatusBar;
import gridpilot.MyTransferControl;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

/**
 * Implementation of SRM version 2 support, using the BeStMan jar.
 * Some of the code below is taken from gov.fnal.srm.util.SRMDispatcher,
 * which is a class providing support for the dCache command line utilities
 * (which we don't use), and  gov.fnal.srm.util.SRMCopyClientV1.
 */
public class SRM2FileTransfer implements FileTransfer {
  
  private String error = "";
  private Vector pendingIDs = new Vector();
  private String user = null;
  private MyTransferControl transferControl;
  private HashMap<String,SRMServer> serverMap;
  private HashMap<String,SRMRequest> requestMap;
  private int currentMaxRequestKey = 0;
  
  // Default to trying 5 checks after submitting a transfer
  private static int checkRetries = 5;
  // Default to sleeping 10 seconds between each check retry
  private static long checkRetrySleep = 10000;

  private static String copyRetries = "0";
  private static String copyRetryTimeout = "120";
  
  private static final int SRM_URL = 0x1;
  private static final int FILE_URL = 0x8;
  private static final int SUPPORTED_PROTOCOL_URL = 0x4;
  private static final int EXISTS_FILE_URL = 0x10;
  private static final int CAN_READ_FILE_URL = 0x20;
  private static final int CAN_WRITE_FILE_URL = 0x40;
  private static final int DIRECTORY_URL = 0x80;
  private static final int UNKNOWN_URL = 0x100;
  private static final int GSIFTP_URL = 0x200;
  
  private static String pluginName;

  public SRM2FileTransfer() throws IOException, GeneralSecurityException{
    pluginName = "srm";
    if(!GridPilot.firstRun){
      user = GridPilot.getClassMgr().getSSL().getGridSubject();
    }
    
    serverMap = new HashMap<String,SRMServer>();
    requestMap = new HashMap<String,SRMRequest>();
      
    transferControl = GridPilot.getClassMgr().getTransferControl();

    //System.setProperty("X509_CERT_DIR",
    //    Util.getProxyFile().getParentFile().getAbsolutePath());
    if(GridPilot.globusTcpPortRange==null || GridPilot.globusTcpPortRange.equals("")){
      error = "WARNING: globus tcp port range is not set. SRM may not work.";
      Debug.debug(error, 1);
      GridPilot.globusTcpPortRange = "";
    }
    else{
      System.setProperty("org.globus.tcp.port.range", GridPilot.globusTcpPortRange);
    }
    copyRetries = GridPilot.getClassMgr().getConfigFile().getValue("File transfer systems",
       "copy retries");
    copyRetryTimeout = GridPilot.getClassMgr().getConfigFile().getValue("File transfer systems",
       "copy retry timeout");
    
    String maxIterations = GridPilot.getClassMgr().getConfigFile().getValue("SRM",
    "submit check retries");
    String sleep = GridPilot.getClassMgr().getConfigFile().getValue("SRM",
    "submit check sleep");
    if(maxIterations!=null){
      checkRetries = Integer.parseInt(maxIterations);
    };
    if(sleep!=null){
      checkRetrySleep = Long.parseLong(sleep);
    };
  }
  
  public String getUserInfo(){
    return user;
  }
  
  /**
   * Connect to the SRM server.
   * @param srmUrl URL of the SRM server.
   */
  private SRMServer connect(GlobusURL srmUrl) throws Exception {
    SRMServer server = null;
    GridPilot.getClassMgr().getSSL().activateProxySSL();
    if(!serverMap.containsKey(srmUrl.getHost())){
      server = new SRMServer(MySSL.getProxyFile().getAbsolutePath(),
          null, createTmpLog4jConfigFile().getParent(), Debug.DEBUG_LEVEL>1);
    }
    else{
      server = serverMap.get(srmUrl.getURL());
    }
    String serviceUrl = "https://"+srmUrl.getHost()+":8443"+"/srm/managerv2";
    if(!server.connect(serviceUrl)){
      throw new IOException("Could not connect to URL "+serviceUrl);
    }
    if(server==null){
       throw new IOException("ERROR: Cannot create SRMServer object.");
    }
    return server;
  }
  
  private File createTmpLog4jConfigFile() throws IOException{
    File log4jDir= File.createTempFile(MyUtil.getTmpFilePrefix(), "");
    log4jDir.delete();
    log4jDir.mkdirs();
    File log4jFile = new File(log4jDir, "log4j.properties");
    String config = "log4j.rootCategory=INFO, CONSOLE\n"+
    "#log4j.rootCategory=INFO, CONSOLE, LOGFILE\n"+
    "\n"+
    "# Set the enterprise logger category to FATAL and its only appender to CONSOLE.\n"+
    "log4j.logger.org.apache.axis.enterprise=FATAL, CONSOLE\n"+
    "\n"+
    "# CONSOLE is set to be a ConsoleAppender using a PatternLayout.\n"+
    "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n"+
    "log4j.appender.CONSOLE.Threshold=INFO\n"+
    "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n"+
    "log4j.appender.CONSOLE.layout.ConversionPattern=- %m%n\n"+
    "\n"+
    "# LOGFILE is set to be a File appender using a PatternLayout.\n"+
    "log4j.appender.LOGFILE=gridpilot.MyFileAppender\n"+
    "log4j.appender.LOGFILE.Name=GridPilot Log\n"+
    "log4j.appender.LOGFILE.File="+GridPilot.logFileName+"\n"+
    "log4j.appender.LOGFILE.Append=true\n"+
    "log4j.appender.LOGFILE.Threshold=INFO\n"+
    "log4j.appender.LOGFILE.layout=org.apache.log4j.PatternLayout\n"+
    "log4j.appender.LOGFILE.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n\n";
    LocalStaticShell.writeFile(log4jFile.getAbsolutePath(), config, true);
    return log4jFile;
  };
  
  private SRMRequest getSRMRequest(int requestKey){
    return requestMap.get(Integer.toString(requestKey));
  }
  
  private synchronized int setSRMRequest(SRMRequest req){
    ++currentMaxRequestKey;
    requestMap.put(Integer.toString(currentMaxRequestKey), req);
    return currentMaxRequestKey;
  }

  /**
   * Get the status of a specific file transfer.
   * Returns one of "Pending", "Ready", "Running", "Done", "Failed".
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   *                           Format: srm-{get|put|copy}::srm request id:transfer index::'srcTurl' 'destTurl' 'srmSurl'
   *
   * Change: when getting status from SRM, instead of "Ready" the TURL is now returned.
   * @throws IOException 
   */
  public String getStatus(String fileTransferID) throws IOException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    int statusIndex = Integer.parseInt(idArr[3]);
    String surl = idArr[6];
    String shortID = idArr[7];

    String status = null;
    String turl = null;

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        status = transferControl.getStatus(shortID);
        Debug.debug("Got status from subsystem: "+status, 2);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("WARNING: could not get status from subsystem for "+
            shortID+". "+e.getMessage(), 1);
      }
    }
    
    if(status!=null && status.length()>0){
      return(status);
    }
    else{
      try{
        SRMRequestStatus rs = getSRMRequest(requestId).getStatus();
        status = (String) rs.getFileStatuses().get(surl);
        // TODO: how does ont get the TURLs? With getPathDetails() ??
        turl = (String) rs.getPathDetails().get(surl);
        Debug.debug("Got status from SRM server: "+statusIndex+" : "+status+
            " : "+turl, 2);
        if(status.equals("Ready") && turl!=null){
          return turl;
        }
        else{
          return status;
        }
      }
      catch(Exception e){
        throw new IOException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
      }
    }
  }
  
  public String getFullStatus(String fileTransferID) throws Exception {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    String surl = idArr[6];
    String shortID = idArr[7];

    String status = "";

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      Debug.debug("Getting status of "+shortID+" from subsystem", 3);
      try{
        status += "GSIFTP Status: "+transferControl.getStatus(shortID);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not get status from subsystem for "+
            fileTransferID+". "+e.getMessage(), 1);
      }
    }
    
    try{
      Debug.debug("Getting status of "+shortID+" from SRM server "+surl, 3);
      SRMRequestStatus rs = getSRMRequest(requestId).getStatus();
      status += "\nSRM Status: "+rs.getFileStatuses().get(surl);
      status += "\nEstimated time to start transfer: "+rs.getRemainingDeferredStartTime();
      status += "\nEstimated transfer finishing time: "+rs.getRemainingTotalRequestTime();
      status += "\nTime stamp: "+rs.getTimeStamp();
    }
    catch(Exception e){
      status += "\nERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage();
    }
    return status;
  }

  /**
   * Parse the file transfer ID into
   * {protocol, requestType (get|put|copy), requestId, statusIndex, srcTurl, destTurl, srmSurl, shortID}.
   * @param fileTransferID   the unique ID of this transfer.
   */
  private String [] parseFileTransferID(String fileTransferID) throws IOException {
    
    String protocol = null;
    String requestType = null;
    String requestId = null;
    String statusIndex = null;
    String srcTurl = null;
    String destTurl = null;
    String srmSurl = null;
    String shortID = null;

    String [] idArr = MyUtil.split(fileTransferID, "::");
    String [] head = MyUtil.split(idArr[0], "-");
    if(idArr.length<3){
      throw new IOException("ERROR: malformed ID "+fileTransferID);
    }
    try{
      protocol = head[0];
      requestType = head[1];
      requestId = idArr[1];
      statusIndex = idArr[2];
      String turls = fileTransferID.replaceFirst(idArr[0]+"::", "");
      turls = turls.replaceFirst(idArr[1]+"::", "");
      turls = turls.replaceFirst(idArr[2]+"::", "");
      String [] turlArray = MyUtil.split(turls, "' '");
      srcTurl = turlArray[0].replaceFirst("'", "");
      destTurl = turlArray[1].replaceFirst("'", "");
      srmSurl = turlArray[2].replaceFirst("'", "");
    }
    catch(Exception e){
      throw new IOException("ERROR: could not parse ID "+fileTransferID+". "+e.getMessage());
    }
    try{
      // First resolve transport protocol
      GlobusURL url = null;
      String transportProtocol = null;
      if(requestType.equals("get")){
        url = new GlobusURL(srcTurl);
        transportProtocol = url.getProtocol();
        shortID = transportProtocol+"-copy"+"::'"+srcTurl+"' '"+destTurl+"'";
      }
      else if(requestType.equals("put")){
        url = new GlobusURL(destTurl);
        transportProtocol = url.getProtocol();
        shortID = transportProtocol+"-copy"+"::'"+srcTurl+"' '"+destTurl+"'";
      }
      Debug.debug("Found short ID: "+shortID, 3);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get short ID for "+fileTransferID+"; SRM probably not ready.", 2);
    }
    return new String [] {protocol, requestType, requestId, statusIndex, srcTurl, destTurl, srmSurl, shortID};
  }
  
  /**
   * Get the percentage of the file that has been copied.
   * Returns a number between 0 and 100.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   */
  public int getPercentComplete(String fileTransferID) throws IOException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    String shortID = idArr[7];

    int percentComplete = -1;

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        percentComplete = transferControl.getPercentComplete(shortID);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not call getPercentComplete from subsystem for "+
            fileTransferID+". "+e.getMessage(), 1);
      }
      return percentComplete;
    }
    else{
      try{
        SRMRequest request = this.getSRMRequest(requestId);
        long remain = request.getStatus().getRemainingTotalRequestTime();
        Date currentDate = new Date();
        Date startDate = request.getStatus().getTimeStamp();
        long total = currentDate.getTime() - startDate.getTime() + remain;
        long diff = currentDate.getTime() - startDate.getTime();
        float percents = diff / total;
        Debug.debug("Got percent complete "+percents, 3);
        return (int) (percents*100);
      }
      catch(Exception e){
        throw new IOException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
      }
    }
  }
  
  /**
   * Get the number of bytes of the file that has been copied.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   */
  public long getBytesTransferred(String fileTransferID) throws IOException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String shortID = idArr[7];
    long bytes = -1;

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // Get status from GSIFTPFileTransfer (or whichever protocol the SRM uses).    
      try{
        bytes = transferControl.getBytesTransferred(shortID);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not call getBytesTransferred from subsystem for "+
            fileTransferID+". "+e.getMessage(), 1);
      }
    }
    
    if(bytes>-1){
      return(bytes);
    }
    else{
      return 0;
    }
  }
  
  /**
   * Get the size of a file in bytes.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   */
  public long getFileBytes(String fileTransferID) throws Exception {
    String [] idArr = parseFileTransferID(fileTransferID);
    int requestId = Integer.parseInt(idArr[2]);
    String surl = idArr[6];   
    SRMRequest request = getSRMRequest(requestId);
    // TODO: how does one get the size of a file??
    String fSize = (String) request.getStatus().getFileStatuses().get(surl);
    Debug.debug("Got the following size "+fSize, 3);
    return Long.parseLong(fSize);
  }
  
  /**
   * Release the file on the SRM server.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   * @throws Exception 
   */
  public void finalize(String fileTransferID) throws Exception {    
    String [] idArr = parseFileTransferID(fileTransferID);
    int requestId = Integer.parseInt(idArr[2]);
    String requestType = idArr[1];
    String surl = idArr[6];   
    SRMRequest request = getSRMRequest(requestId);
    Debug.debug("Finalizing request "+requestId, 2);
    request.releaseFiles(new String[] {surl});
    if(requestType.equalsIgnoreCase("put")){
      request.putDone(new String[] {surl});
    }
    HashMap statusMap = new HashMap();
    statusMap.put(surl, "Done");
    request.getStatus().setFileStatuses(statusMap);
  }
  
  /**
   * Cancel the transfer and release the file on the SRM server.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   * @throws Exception 
   */
  public void cancel(String fileTransferID) throws Exception {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    String surl = idArr[6];
    String shortID = idArr[7];
    Debug.debug("Cancelling request "+requestId, 2);
    SRMRequest request = getSRMRequest(requestId);
    request.abortFiles(new String[] {surl});
    if(request.getFiles()!=null && request.getFiles().length==0){
      request.abortRequest();
    }
    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // cancel GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        Debug.debug("Cancelling "+shortID, 1);
        transferControl.cancel(shortID);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("WARNING: could not cancel "+shortID+". "+e.getMessage(), 1);
      }
    }

  }

  // Wait for all files to be ready on the SRM server before beginning the
  // transfers. This may be reconsidered...
  private String [] waitForOK(Vector _thesePendingIDs) throws IOException {
    synchronized(pendingIDs){
      Vector thesePendingIDs = _thesePendingIDs;
      String status = null;
      String [] idArray = new String[thesePendingIDs.size()];
      String [] turlArray = new String[thesePendingIDs.size()];
      for(int i=0; i<thesePendingIDs.size(); ++i){
        idArray[i] = (String) thesePendingIDs.get(i);
      }
      int i = 0;
      pendingIDs.addAll(thesePendingIDs);
      while(true){
        if(thesePendingIDs.size()==0){
          break;
        }
        ++i;
        for(int j=0; j<thesePendingIDs.size(); ++j){
          Debug.debug("Checking status "+i+" - "+j+":"+thesePendingIDs.size(), 3);
          String id = (String) thesePendingIDs.get(j);
          Debug.debug("of --> "+id, 3);
          try{
            status = getStatus(id);
            Debug.debug("Got status "+status, 3);
            if(status.matches("^\\w+://.*")){
              for(int k=0; k<idArray.length; ++k){
                if(idArray[k].equals(id)){
                  turlArray[k] = status;
                  break;
                }
              }
              pendingIDs.remove(id);
              thesePendingIDs.remove(id);
            }
            else if(status.equalsIgnoreCase("Ready")){
              pendingIDs.remove(id);
              thesePendingIDs.remove(id);
            }
            else if(status.equalsIgnoreCase("Failed")){
              for(int k=0; k<idArray.length; ++k){
                if(idArray[k].equals(id)){
                  turlArray[k] = "Failed";
                  break;
                }
              }
              pendingIDs.remove(id);
              thesePendingIDs.remove(id);
            }
          }
          catch(Exception e){
            Debug.debug("WARNING: could not get status of "+id, 1);
            e.printStackTrace();
          }
        }
        if(thesePendingIDs.isEmpty()){
          break;
        }
        if(i>checkRetries+1){
          throw new IOException("ERROR: file(s) were not ready within "+
              (checkRetries*checkRetrySleep/1000)+" seconds");
        }
        Debug.debug("Waiting...", 3);
        try{
          Thread.sleep(checkRetrySleep);
        }
        catch(InterruptedException e){
          e.printStackTrace();
          break;
        }
      }
      return turlArray;
    }
  }

  public boolean checkURLs(GlobusURL [] srcUrls, GlobusURL [] destUrls)
     throws ClientException, ServerException, FTPException, IOException {
    
    int fromType = getUrlType(srcUrls[0]);
    int toType = getUrlType(destUrls[0]);
    
    if(getUrlType(srcUrls[0])==UNKNOWN_URL ||
        getUrlType(destUrls[0])==UNKNOWN_URL ||
        !srcUrls[0].getProtocol().equals("srm") && !destUrls[0].getProtocol().equals("srm") ){
      return false;
    }
    else{
      try{
        checkURLsUniformity(fromType, srcUrls, true);
        checkURLsUniformity(toType, destUrls, false);
      }
      catch(Exception e){
        return false;
      }
      return true;
    }
    
  }

  /**
   * Get the size of the file in bytes.
   * @throws Exception 
   */
  public long getFileBytes(GlobusURL globusUrl) throws Exception {
    return getSRMFileBytes(new GlobusURL [] {globusUrl})[0];
  }
  
  /**
   * Get sizes files in bytes.
   */
  private long [] getSRMFileBytes(GlobusURL [] globusUrls) throws Exception {
    SRMServer server = connect(globusUrls[0]);
    SRMRequest request = new SRMRequest();
    String [] urlStrs = new String [globusUrls.length];
    long [] fileSizes = new long [globusUrls.length];
    Object [] fSizes= new String [globusUrls.length];
    for(int i=0; i<globusUrls.length; ++i){
      urlStrs[i] = globusUrls[i].getURL();
    }
    request.setSRMServer(server);
    request.addFiles(urlStrs, null, null);
    request.srmLs();
    // Very unclear from the javadocs whether these have to be set and to what (all are strings).
    //request.setRequestToken("");
    //request.setRequestType("LS");
    //request.setAuthID("");
    request.submit();
    int i = 0;
    while(true){
      ++i;
      for(int j=0; j<urlStrs.length; ++j){
        try{
          // TODO: how does one get the file sizes? By issuing an Ls and getting the statuses??
          fSizes = request.getStatus().getFileStatuses().values().toArray();
          Debug.debug("Got file sizes: "+MyUtil.arrayToString(fSizes), 2);
          if(request.checkStatus()){
            fileSizes[i] = Long.parseLong((String) request.getStatus().getFileStatuses().get(urlStrs[j]));
          }
        }
        catch(Exception e){
          Debug.debug("WARNING: could not get status", 1);
          e.printStackTrace();
        }
      }
      if(i>checkRetries+1){
        throw new IOException("ERROR: file(s) were not ready within "+
            (checkRetries*checkRetrySleep/1000)+" seconds");
      }
      Debug.debug("Waiting...", 3);
      try{
        Thread.sleep(checkRetrySleep);
      }
      catch(InterruptedException e){
        e.printStackTrace();
        break;
      }
    }
    return fileSizes;
  }
  
  /**
   * Initiate transfers and return identifiers:
   * "srm-{get|put|copy}::srm request id::transfer index::'srcTurl' 'destTurl' 'srmSurl'"
   * @param   srcUrls    the source URLs
   * @param   destUrls   the destination URLs
   */
  public String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls)
     throws ClientException, ServerException, FTPException, IOException {

    try{      
      int fromType = getUrlType(srcUrls[0]);
      checkURLsUniformity(fromType, srcUrls, true);
      int toType = getUrlType(destUrls[0]);
      checkURLsUniformity(toType, destUrls, false);
      
      if(fromType==SRM_URL &&
          ((toType & FILE_URL)==FILE_URL || (toType & GSIFTP_URL)==GSIFTP_URL)){
        return copySrmToGsiftp(srcUrls, destUrls);
      }
      else if(fromType==SRM_URL && toType!=SRM_URL){
        return copySrmToOther(srcUrls, destUrls);
      }
      else if((fromType & FILE_URL)==FILE_URL && toType==SRM_URL){
        return copyFileToSrm(srcUrls, destUrls);
      }
      else if(fromType!=SRM_URL && toType==SRM_URL){
        return copyOtherToSrm(srcUrls, destUrls);
      }
      else if(fromType==SRM_URL && toType==SRM_URL){
        return copySrmToSrm(srcUrls, destUrls);
      } 
      else{
        throw new IllegalArgumentException(
            "neither source nor destination are SRM URLs :"+
            srcUrls[0].getURL()+" "+destUrls[0].getURL());
      }
    }
    catch(FTPException e){
      Debug.debug("ERROR: Problem with gridftp when copying.", 1);
      throw e;
    }
    catch(Exception e){
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    finally{
      try{
        // No cleanup needed...
      }
      catch(Exception e){
      }
    }
  }
  
  // if the source is srm and the destination is file or gsiftp we get
  private String[] copySrmToGsiftp(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws Exception {
    String sources[] = new String[srcUrls.length];
    String dests[] = new String[srcUrls.length];
    for(int i = 0; i<srcUrls.length; ++i){
      GlobusURL srmSrc = srcUrls[i];
      GlobusURL fileDest = destUrls[i];
      if(fileDest.getPath().endsWith("/") || fileDest.getPath().endsWith("\\") ||
          (new File(fileDest.getPath())).isDirectory()){
        throw new IOException("ERROR: destination is directory "+ fileDest.getURL());
      }
      String fsPath = fileDest.getPath();
      fsPath = fsPath.replaceFirst("^/+", "/");
      fsPath = fsPath.replaceFirst("^/(\\w:/+)", "$1");
      try{
        fsPath = URLDecoder.decode(fsPath, "utf-8");
      }
      catch (UnsupportedEncodingException e){
        e.printStackTrace();
      }
      if(destUrls[i].getProtocol().equalsIgnoreCase("file") && !(new File(fsPath)).getParentFile().canWrite()){
        throw new IOException("ERROR: destination directory is not writeable. "+ fsPath);
      }
      sources[i] = srmSrc.getURL();
      Debug.debug("source file #"+i+" : "+sources[i], 2);
      dests[i] = fsPath;
    }
    
    long [] fSizes = getSRMFileBytes(srcUrls);
    SRMServer server = connect(srcUrls[0]);
    SRMRequest request = new SRMRequest();
    request.setSRMServer(server);
    request.setRequestType("get");
    request.addFiles(sources, dests, fSizes);
    request.submit();
    int requestId = setSRMRequest(request);
    SRMRequestStatus rs = getSRMRequest(requestId).getStatus();
    if(rs==null){
      throw new IOException("ERROR: null request status");
    }
    GlobusURL [] turls = new GlobusURL[srcUrls.length];
    String [] ids = new String[srcUrls.length];
    String [] assignedTurls = null;
    // First, assign temporary ids (with TURL null) and wait for ready
    Vector thesePendingIDs = new Vector();
    for(int i=0; i<srcUrls.length; ++i){
      thesePendingIDs.add(pluginName+"-get::"+requestId+"::"+i+"::'"+null+"' '"+destUrls[i].getURL()+
      "' '"+srcUrls[0].getURL()+"'");
    }
    Debug.debug("Transfer request submitted for get. Waiting for ok.", 2);
    // show message on status bar on monitoring frame
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    statusBar.setLabel("Waiting for file(s) to be ready...");
    assignedTurls = waitForOK(thesePendingIDs);
    Debug.debug("Assigned TURLs: "+MyUtil.arrayToString(assignedTurls), 2);
    // Now, assign the real ids (with TURL not null)
    Debug.debug("Request: "+rs.toString(), 3);
    Debug.debug("File statuses: "+MyUtil.arrayToString(rs.getFileStatuses().values().toArray()), 3);
    for(int i=0; i<srcUrls.length; ++i){
      if(assignedTurls==null || assignedTurls[i]==null || assignedTurls[i].equals("Failed")){
        throw new IOException("Failed to get TURL.");
      }
      else{
        turls[i] = new GlobusURL(assignedTurls[i]);
      }
      ids[i] = pluginName+"-get::"+requestId+"::"+i+"::'"+turls[i].getURL()+"' '"+destUrls[i].getURL()+
      "' '"+srcUrls[0].getURL()+"'";
    }
    // if no exception was thrown, all is ok and we can set files to "Running"
    HashMap statusMap = new HashMap();
    try{
      for(int i=0; i<srcUrls.length; ++i){
        statusMap.put(srcUrls[i], "Running");
      }
      rs.setFileStatuses(statusMap);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "WARNING: could not set status of files to Running.", e);
    }
    // show message on status bar on monitoring frame
    statusBar.setLabel("File(s) ready, starting download.");
    try{
      // Now use some other plugin - depending on the TURL returned
      transferControl.startCopyFiles(turls, destUrls);
    }
    catch(Exception e){
      for(int i=0; i<ids.length; ++i){
        try{
          cancel(ids[i]);
        }
        catch(Exception ee){
        }
      }
      e.printStackTrace();
      throw new IOException("ERROR: problem queueing transfers "+ e.getMessage());
    }
    return ids;
  }
  
  // if the source is srm and the destination is something other
  // than SRM or GSIFTP, but not file, we push from the source
  private String[] copySrmToOther(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws Exception {
    String sources[] = new String[srcUrls.length];
    String dests[] = new String[srcUrls.length];
    for(int i = 0; i<srcUrls.length; ++i){
      sources[i] = srcUrls[i].getURL();
      dests[i] = destUrls[i].getURL();
    }
    long [] fSizes = getSRMFileBytes(srcUrls);
    SRMServer server = connect(srcUrls[0]);
    SRMRequest request = new SRMRequest();
    request.setSRMServer(server);
    request.setRequestType("copy");
    request.addFiles(sources, dests, fSizes);
    request.submit();
    int requestId = setSRMRequest(request);
    String [] ids = new String[srcUrls.length];
    for(int i=0; i<srcUrls.length; ++i){
      ids[i] = pluginName+"-copy::"+requestId+"::"+i+"::'"+
         /*TURL*/request.getStatus().getPathDetails().get(srcUrls[i])+"' '"+destUrls[i].getURL()+
         "' '"+srcUrls[0].getURL()+"'";
    }
    Debug.debug("Returning IDs "+MyUtil.arrayToString(ids), 2);
    return ids;
  }
  
  // if the destination is srm and the source is file we put
  private String[] copyFileToSrm(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws Exception {
    String sources[] = new String[srcUrls.length];
    String dests[] = new String[srcUrls.length];
    long fSizes[] = new long[srcUrls.length];
    for(int i = 0; i<srcUrls.length; ++i){
      GlobusURL fileSrc = srcUrls[i];
      GlobusURL srmDest = destUrls[i];
      if(fileSrc.getPath().endsWith("/") || fileSrc.getPath().endsWith("\\") ||
          (new File(fileSrc.getPath())).isDirectory()){
        throw new IOException("ERROR: source is directory "+ fileSrc.getURL());
      }
      if(!(new File(fileSrc.getPath())).canRead()){
        throw new IOException("ERROR: source is not readable "+ fileSrc.getURL());
      }
      sources[i] = fileSrc.getPath();
      Debug.debug("source file #"+i+" : "+sources[i], 2);
      File f = new File(sources[i]);
      fSizes[i] = f.length();
      dests[i] = srmDest.getURL();
    }
    SRMServer server = connect(srcUrls[0]);
    SRMRequest request = new SRMRequest();
    request.setSRMServer(server);
    request.setRequestType("put");
    request.addFiles(sources, dests, fSizes);
    request.submit();
    int requestId = setSRMRequest(request);
    SRMRequestStatus rs = getSRMRequest(requestId).getStatus();
    if(rs==null){
      throw new IOException("ERROR: null request status");
    }
    GlobusURL [] turls = new GlobusURL[srcUrls.length];
    String [] ids = new String[srcUrls.length];
    String [] assignedTurls = null;
    // First, assign temporary ids (with TURL null) and wait for ready
    Vector thesePendingIDs = new Vector();
    for(int i=0; i<srcUrls.length; ++i){
      thesePendingIDs.add(pluginName+"-get::"+requestId+"::"+i+"::'"+srcUrls[i].getURL()+"' '"+null+
      "' '"+destUrls[0].getURL()+"'");
    }
    Debug.debug("Transfer request submitted for put. Waiting for ok.", 2);
    // show message on status bar on monitoring frame
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    statusBar.setLabel("Waiting for file(s) to be ready...");
    assignedTurls = waitForOK(thesePendingIDs);
    // Now, assign the real ids (with TURL not null)
    for(int i=0; i<srcUrls.length; ++i){
      if(assignedTurls!=null && assignedTurls[i]!=null){
        if(assignedTurls[i].equals("Failed")){
          throw new IOException("Failed to get TURL.");
        }
        turls[i] = new GlobusURL(assignedTurls[i]);
      }
      else{
        throw new IOException("ERROR: problem getting TURLS.");
      }
      ids[i] = pluginName+"-get::"+requestId+"::"+i+"::'"+srcUrls[i].getURL()+"' '"+turls[i].getURL()+
      "' '"+destUrls[0].getURL()+"'";
    }
    // if no exception was thrown, all is ok and we can set files to "Running"
    HashMap statusMap = new HashMap();
    try{
      for(int i=0; i<srcUrls.length; ++i){
        statusMap.put(srcUrls[i], "Running");
      }
      rs.setFileStatuses(statusMap);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "WARNING: could not set status of files to Running.", e);
    }
    // show message on status bar on monitoring frame
    statusBar.setLabel("File(s) ready, starting upload.");
    try{
      // Now use some other plugin - depending on the TURL returned
      transferControl.startCopyFiles(srcUrls, turls);
    }
    catch(Exception e){
      for(int i=0; i<ids.length; ++i){
        try{
          cancel(ids[i]);
        }
        catch(Exception ee){
        }
      }
      throw new IOException("ERROR: problem queueing transfers "+ e.getMessage());
    }
    return ids;
  }

  // if the destination is srm and the source is something other
  // than SRM, but not file, we pull from the destination
  private String[] copyOtherToSrm(GlobusURL[] srcUrls, GlobusURL[] destUrls) throws Exception {
    String sources[] = new String[srcUrls.length];
    String dests[] = new String[srcUrls.length];
    for(int i = 0; i<srcUrls.length; ++i){
      sources[i] = srcUrls[i].getURL();
      dests[i] = destUrls[i].getURL();
    }
    long [] fSizes = getSRMFileBytes(srcUrls);
    SRMServer server = connect(destUrls[0]);
    SRMRequest request = new SRMRequest();
    request.setSRMServer(server);
    request.setRequestType("copy");
    request.addFiles(sources, dests, fSizes);
    request.submit();
    int requestId = setSRMRequest(request);
    String [] ids = new String[srcUrls.length];
    Arrays.fill(ids, Integer.toString(requestId));
    for(int i=0; i<srcUrls.length; ++i){
      ids[i] = pluginName+"-copy::"+requestId+"::"+i+"::'"+
      srcUrls[i].getURL()+"' '"+/*TURL*/request.getStatus().getPathDetails().get(dests[i])+
      "' '"+destUrls[0].getURL()+"'";
    }
    return ids;
  }

  // both source(s) and destination(s) are srm urls
  // we can either push or pull - we pull.
  // In this case we cannot know the source TURL (gsiftp://...) and use
  // the source SURL (srm://...) for the ID string.
  // TODO: perhaps we should try both...
  private String[] copySrmToSrm(GlobusURL[] srcUrls, GlobusURL[] destUrls) throws Exception {
    String sources[] = new String[srcUrls.length];
    String dests[] = new String[srcUrls.length];
    for(int i = 0; i<srcUrls.length; ++i){
      sources[i] = srcUrls[i].getURL();
      dests[i] = destUrls[i].getURL();
    }
    long [] fSizes = getSRMFileBytes(srcUrls);
    SRMServer server = connect(destUrls[0]);
    SRMRequest request = new SRMRequest();
    request.setSRMServer(server);
    request.setRequestType("copy");
    request.addFiles(sources, dests, fSizes);
    request.submit();
    int requestId = setSRMRequest(request);
    String [] ids = new String[srcUrls.length];
    for(int i=0; i<srcUrls.length; ++i){
      ids[i] = pluginName+"-copy::"+requestId+"::"+i+"::'"+sources[i]+"' '"+
      /*TURL*/request.getStatus().getPathDetails().get(dests[i])+"' '"+destUrls[0].getURL()+"'";
    }
    return ids;
  }

  /**
   * Delete a list of files.
   * @param destUrls list of files to be deleted on the SRM server.
   * @throws Exception 
   */
  public void deleteFiles(GlobusURL [] destUrls) throws Exception {
    for(int i=0; i<destUrls.length; ++i){
      if(!destUrls[i].getHost().equals(destUrls[0].getHost())){
        throw new IOException("ERROR: all files to be deleted must be on the same server. "+
            destUrls[i]+" <-> "+destUrls[0]);
      }
    }    
    String [] urls = new String [destUrls.length];
    for(int i=0; i<destUrls.length; ++i){
      urls[i] = destUrls[i].getURL();
    }
    SRMServer server = connect(destUrls[0]);
    SRMRequest request = new SRMRequest();
    request.setSRMServer(server);
    request.removeFiles(urls);
    request.submit();
  }

  public static int getUrlType(GlobusURL url) throws IOException {
    String prot = url.getProtocol();
    if(prot!=null ) {
       if(prot.equals("srm")) {
          return SRM_URL;
       }
       else if(prot.equals("http") ||
          prot.equals("ftp")    ||
          //prot.equals("gsiftp") ||
          prot.equals("gridftp")||
          prot.equals("https")  ||
          prot.equals("ldap")   ||
          prot.equals("ldaps")  ||
          prot.equals("dcap")   ||
          prot.equals("rfio")) {
          return SUPPORTED_PROTOCOL_URL;
       }
       else if(prot.equals("gsiftp")) {
         return GSIFTP_URL;
      }
       else if(!prot.equals("file")) {
          return UNKNOWN_URL;
       }
    }
    
    File f = new File(url.getPath());
    f = f.getCanonicalFile();
    int rc = FILE_URL;
    if(f.exists()) {
       rc |= EXISTS_FILE_URL;
    }
    if(f.canRead()) {
       rc |= CAN_READ_FILE_URL;
    }
    if(f.canWrite()) {
       rc |= CAN_WRITE_FILE_URL;
    }
    if(f.isDirectory()) {
       rc |= DIRECTORY_URL;
    }
    return rc;
  }

  public static void checkURLsUniformity(int type, GlobusURL urls[],
      boolean areSources) throws IllegalArgumentException, IOException {
    int number_of_sources = urls.length;
    if (number_of_sources==0) { 
          throw new IllegalArgumentException("No URL(s) specified ");
    }
    String host = urls[0].getHost();
    int port = urls[0].getPort();
    if(type ==SRM_URL  || type==GSIFTP_URL ||
        ((type & SUPPORTED_PROTOCOL_URL)==SUPPORTED_PROTOCOL_URL)){
      if(host==null || host.equals("") || port<0){
        String error = "illegal source url: "+
        urls[0].getURL();
        Debug.debug(error, 2);
        throw new IllegalArgumentException(error );
       }
       
       for(int i=0; i<number_of_sources; ++i){
          if(type!=getUrlType(urls[i])){
            String error ="if specifying multiple sources/destinations,"+
            " sources/destinations must be of the same type, incorrect url: "+
            urls[i];
            Debug.debug(error, 2);
            throw new IllegalArgumentException(error);
          }
          if(!host.equals(urls[i].getHost())){
            String error = "if specifying multiple  sources, "+
            "all sources must have same host. "+
            urls[i].getURL();
            Debug.debug(error, 2);
            throw new IllegalArgumentException(error);
          }
          if(port!=urls[i].getPort()){
            String error =
              "if specifying multiple  sources, "+
              "all sources must have same port. "+
              urls[i] ;
            Debug.debug(error, 2);
            throw new IllegalArgumentException(error);
          }
       }
    }
    else if((type & FILE_URL)==FILE_URL){
      for(int i=1; i<number_of_sources; ++i){
        int thisTypeI =
           (i==0?type:getUrlType(urls[i]));
        if((thisTypeI & FILE_URL)!=FILE_URL){
          String error =
            "If specifying multiple sources, sources must be " +
            "of the same type.  Incorrect source: "+
            urls[i] ;
          Debug.debug(error, 2);
          throw new IllegalArgumentException(error);
        }
        if((thisTypeI & DIRECTORY_URL)==DIRECTORY_URL){
          String error = "source/destination file is directory"+
          urls[i].getURL();
          Debug.debug(error, 2);
          throw new IllegalArgumentException(error);
        }
        if(areSources && ((thisTypeI & EXISTS_FILE_URL)==0)){
          String error = "source file does not exist"+
          urls[i].getURL();
          Debug.debug(error, 2);
          throw new IllegalArgumentException(error);
        }
        if(areSources &&((thisTypeI & CAN_READ_FILE_URL)==0)){
          String error = "source file is not readable"+
          urls[i].getURL();
          Debug.debug(error, 2);
          throw new IllegalArgumentException(error);
        }
      }
    }
    else{
      String error = "Unknown type of source(s) or destination(s)";
      Debug.debug(error, 2);
      throw new IllegalArgumentException(error);
    }
    for(int i=0; i<number_of_sources; ++i){
      for(int j=0;j<number_of_sources; ++j){
        if(i!=j && (urls[i].getPath().equals(urls[j].getPath()))){
          String error = "list of sources contains the same url twice "+
             "url#"+i+" is "+urls[i].getURL() +
             " and url#"+j+" is "+urls[j].getURL();
          Debug.debug(error, 2);
          throw new IllegalArgumentException(error);
        }
      }
    }
  }

  /**
   * Maps
   * 
   * "Pending", "Ready", "Running", "Done", "Failed"
   * -> 
   * FileTransfer.STATUS_DONE, FileTransfer.STATUS_ERROR, FileTransfer.STATUS_FAILED,
   * FileTransfer.STATUS_RUNNING, FileTransfer.STATUS_WAIT
   */
  public int getInternalStatus(String fileTransferID, String status) throws Exception{
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String shortID = idArr[7];

    int internalStatus = -1;

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        internalStatus = transferControl.getInternalStatus(shortID, status);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("WARNING: could not get status from subsystem for "+
            shortID+". "+e.getMessage(), 1);
      }
    }

    if(internalStatus>-1){
      return internalStatus;
    }
    
    // If transfer has not yet started or this is a srm copy transfer,
    // use srm to get status.
    
    String ftStatus = getStatus(fileTransferID);
    int ret = -1;
    if(ftStatus==null || ftStatus.equals("")){
      // TODO: should this be STATUS_WAIT?
      ret = FileTransfer.STATUS_ERROR;
    }
    else if(ftStatus==null && ftStatus.equalsIgnoreCase("Pending")){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus==null && (ftStatus.equalsIgnoreCase("Ready") ||
        status.matches("^\\w+://.*"))){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus==null && ftStatus.equalsIgnoreCase("Running")){
      ret = FileTransfer.STATUS_RUNNING;
    }
    else if(ftStatus==null && ftStatus.equalsIgnoreCase("Done")){
      ret = FileTransfer.STATUS_DONE;
    }
    else if(ftStatus==null && ftStatus.equalsIgnoreCase("Failed")){
      ret = FileTransfer.STATUS_FAILED;
    }
    else{
      throw new Exception("ERROR: status "+ftStatus+" not found.");
    }
    return ret;
  }

  public void getFile(GlobusURL globusUrl, File downloadDirOrFile) throws Exception {
    // No point in implementing this. SRM is anyway not browsable.
    throw new IOException("getFile not supported by SRM plugin.");
  }

  public void putFile(File file, GlobusURL globusUrl) throws Exception {
    // No point in implementing this. SRM is anyway not browsable.
    throw new IOException("getFile not supported by SRM plugin.");
  }

  public Vector list(GlobusURL globusUrl, String filter) throws Exception {
    // No point in implementing this. SRM is anyway not browsable.
    throw new IOException("list not supported by SRM plugin.");
  }
  
  public void abortTransfer(String id) throws ServerException, IOException {
    //  No point in implementing this. SRM is anyway not browsable.
  }

  public void write(GlobusURL globusUrl, String text) throws Exception {
    //  No point in implementing this. SRM is anyway not browsable.
  }

}
