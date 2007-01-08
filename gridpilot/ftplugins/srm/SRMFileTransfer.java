package gridpilot.ftplugins.srm;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Date;
import java.util.Vector;

import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.ServerException;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;

import org.dcache.srm.SRMException;
import org.dcache.srm.client.SRMClientV1;

import diskCacheV111.srm.ISRM;
import diskCacheV111.srm.RequestStatus;

import gridpilot.Debug;
import gridpilot.StatusBar;
import gridpilot.TransferControl;
import gridpilot.FileTransfer;
import gridpilot.GridPilot;
import gridpilot.Util;

/**
 * Implementation of SRM version 1 support, using the dCache jar.
 * Most of the code below is taken from gov.fnal.srm.util.SRMDispatcher,
 * which is a class providing support for the dCache command line utilities
 * (which we don't use), and  gov.fnal.srm.util.SRMCopyClientV1.
 */
public class SRMFileTransfer implements FileTransfer {
  
  private String error = "";
  private Vector pendingIDs = new Vector();
  private String user = null;
  
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

  public SRMFileTransfer(){
    pluginName = "srm";
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
   * @param   srmUrl    URL of the SRM server.
   */
  private static ISRM connect(GlobusURL srmUrl) throws Exception {
    ISRM srm;
    try{
       srm = new SRMClientV1(
           srmUrl,
           GridPilot.getClassMgr().getGridCredential(),
           Long.parseLong(Integer.toString(
               1000*Integer.parseInt(copyRetryTimeout))),
           Integer.parseInt(copyRetries),
           new SRMLogger(true),
           /*doDelegation*/true,
           /*fullDelegation*/true,
           /*gss_expected_name*/"host",
           "srm/managerv1.wsdl");
       Debug.debug("connected to server, obtained proxy of type "+srm.getClass(), 2);
    }
    catch(Exception srme){
      srme.printStackTrace();
       //throw new IOException(srme.toString());
    }
    try{
      srm = new SRMClientV1(
          srmUrl,
          GridPilot.getClassMgr().getGridCredential(),
          Long.parseLong(Integer.toString(
              1000*Integer.parseInt(copyRetryTimeout))),
          Integer.parseInt(copyRetries),
          new SRMLogger(true),
          /*doDelegation*/true,
          /*fullDelegation*/true,
          /*gss_expected_name*/"host",
          "srm/managerv1");
      Debug.debug("connected to server, obtained proxy of type "+srm.getClass(), 2);
    }
    catch(Exception srme){
      throw new IOException(srme.toString());
    }
    if(srm==null){
       throw new IOException("ERROR: Cannot get SRM connection.");
    }
    return srm;
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
   */
  public String getStatus(String fileTransferID) throws SRMException {
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
        status = TransferControl.getStatus(shortID);
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
        ISRM srm = connect(new GlobusURL(surl));
        RequestStatus rs = srm.getRequestStatus(requestId);
        status = rs.fileStatuses[statusIndex].state;
        turl = rs.fileStatuses[statusIndex].TURL;
        Debug.debug("Got status from SRM server: "+statusIndex+" : "+status+
            " : "+rs.fileStatuses[statusIndex].TURL, 2);
        if(status.equals("Ready") && turl!=null){
          return turl;
        }
        else{
          return status;
        }
      }
      catch(Exception e){
        throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
      }
    }
  }
  
  public String getFullStatus(String fileTransferID) throws Exception {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    int statusIndex = Integer.parseInt(idArr[3]);
    String surl = idArr[6];
    String shortID = idArr[7];

    String status = "";

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      Debug.debug("Getting status of "+shortID+" from subsystem", 3);
      try{
        status += "GSIFTP Status: "+TransferControl.getStatus(shortID);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not get status from subsystem for "+
            fileTransferID+". "+e.getMessage(), 1);
      }
    }
    
    try{
      Debug.debug("Getting status of "+shortID+" from SRM server "+surl, 3);
      ISRM srm = connect(new GlobusURL(surl));
      RequestStatus rs = srm.getRequestStatus(requestId);
      status += "\nSRM Status: "+rs.fileStatuses[statusIndex].state;
      status += "\nEstimated time to start transfer: "+rs.estTimeToStart;
      status += "\nEstimated time to start file transfer: "+rs.fileStatuses[statusIndex].estSecondsToStart;
      status += "\nEstimated transfer finishing time: "+rs.finishTime;
      status += "\nChecksum type: "+rs.fileStatuses[statusIndex].checksumType;
      status += "\nChecksum: "+rs.fileStatuses[statusIndex].checksumValue;
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
  private String [] parseFileTransferID(String fileTransferID) throws SRMException {
    
    String protocol = null;
    String requestType = null;
    String requestId = null;
    String statusIndex = null;
    String srcTurl = null;
    String destTurl = null;
    String srmSurl = null;
    String shortID = null;

    String [] idArr = Util.split(fileTransferID, "::");
    String [] head = Util.split(idArr[0], "-");
    if(idArr.length<3){
      throw new SRMException("ERROR: malformed ID "+fileTransferID);
    }
    try{
      protocol = head[0];
      requestType = head[1];
      requestId = idArr[1];
      statusIndex = idArr[2];
      String turls = fileTransferID.replaceFirst(idArr[0]+"::", "");
      turls = turls.replaceFirst(idArr[1]+"::", "");
      turls = turls.replaceFirst(idArr[2]+"::", "");
      String [] turlArray = Util.split(turls, "' '");
      srcTurl = turlArray[0].replaceFirst("'", "");
      destTurl = turlArray[1].replaceFirst("'", "");
      srmSurl = turlArray[2].replaceFirst("'", "");
    }
    catch(Exception e){
      throw new SRMException("ERROR: could not parse ID "+fileTransferID+". "+e.getMessage());
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
  public int getPercentComplete(String fileTransferID) throws SRMException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    String surl = idArr[6];
    String shortID = idArr[7];

    int percentComplete = -1;

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        percentComplete = TransferControl.getPercentComplete(shortID);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not call getPercentComplete from subsystem for "+
            fileTransferID+". "+e.getMessage(), 1);
      }
      return percentComplete;
    }
    else{
      try{
        ISRM srm = connect(new GlobusURL(surl));
        RequestStatus rs = srm.getRequestStatus(requestId);
        Date finishDate = rs.finishTime;
        Date currentDate = new Date();
        Date startDate = rs.submitTime;
        long total = finishDate.getTime() - startDate.getTime();
        long diff = currentDate.getTime() - startDate.getTime();
        float percents = diff / total;
        Debug.debug("Got percent complete "+percents, 3);
        return (int) (percents*100);
      }
      catch(Exception e){
        throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
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
  public long getBytesTransferred(String fileTransferID) throws SRMException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String shortID = idArr[7];
    long bytes = -1;

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // Get status from GSIFTPFileTransfer (or whichever protocol the SRM uses).    
      try{
        bytes = TransferControl.getBytesTransferred(shortID);
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
   * Get the size of the file in bytes.
   * Returns a number between 0 and 100.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   */
  public long getFileBytes(String fileTransferID) throws SRMException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    int statusIndex = Integer.parseInt(idArr[3]);
    String surl = idArr[6];
    try{
      ISRM srm = connect(new GlobusURL(surl));
      RequestStatus rs = srm.getRequestStatus(requestId);
      return rs.fileStatuses[statusIndex].size;
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+
          ". "+e.getMessage());
    }
  }
  
  /**
   * Get the size of the file in bytes.
   * Returns a number between 0 and 100.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   */
  public long getFileBytes(GlobusURL surl) throws SRMException {
    try{
      ISRM srm = connect(surl);
      return srm.getFileMetaData(new String [] {surl.getURL()})[0].size;
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem with getting file size for "+surl.getURL()+
          ". "+e.getMessage());
    }
  }
  
  /**
   * Release the file on the SRM server.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   */
  public void finalize(String fileTransferID) throws SRMException {    
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    int statusIndex = Integer.parseInt(idArr[3]);
    String surl = idArr[6];

    try{
      // getStatus
      ISRM srm = connect(new GlobusURL(surl));
      RequestStatus rs = srm.getRequestStatus(requestId);
      Debug.debug("Finalizing request "+rs.requestId+" : "+rs.state+" : "+Util.arrayToString(rs.fileStatuses), 2);
      // state should be one of "Ready", "Pending", "Active", "Done", "Failed".
      //RequestStatus types: “Get”, “Put”, “Copy”, …
      //RequestStatus states: “Pending”, “Active”, “Done”, “Failed”
      //RequestFileStatus: “Pending”, “Ready”, “Running”, “Done”, “Failed”
      //if(rs.state.equalsIgnoreCase("Done") || rs.state.equalsIgnoreCase("Failed")){
        srm.setFileStatus(requestId, rs.fileStatuses[statusIndex].fileId, "Done");
      //}
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
    }
  }
  
  /**
   * Cancel the transfer and release the file on the SRM server.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   */
  public void cancel(String fileTransferID) throws SRMException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    int statusIndex = Integer.parseInt(idArr[3]);
    String surl = idArr[6];
    String shortID = idArr[7];

    try{
      ISRM srm = connect(new GlobusURL(surl));
      // This cancels and sets to Done.
      RequestStatus rs = srm.getRequestStatus(requestId);
      srm.setFileStatus(requestId, rs.fileStatuses[statusIndex].fileId, "Done");
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
    }

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // cancel GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        Debug.debug("Cancelling "+shortID, 1);
        TransferControl.cancel(shortID);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("WARNING: could not cancel "+shortID+". "+e.getMessage(), 1);
      }
    }

  }

  // Wait for all files to be ready on the SRM server before beginning the
  // transfers. This may be reconsidered...
  private String [] waitForOK(Vector _thesePendingIDs) throws SRMException {
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
          throw new SRMException("ERROR: file(s) were not ready within "+
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
  throws ClientException, ServerException, FTPException, IOException,
  SRMException {
    
    int fromType = getUrlType(srcUrls[0]);
    checkURLsUniformity(fromType, srcUrls, true);
    int toType = getUrlType(destUrls[0]);
    checkURLsUniformity(toType, destUrls, false);
    
    if(getUrlType(srcUrls[0])==UNKNOWN_URL ||
        getUrlType(destUrls[0])==UNKNOWN_URL ||
        !srcUrls[0].getProtocol().equals("srm") && !destUrls[0].getProtocol().equals("srm") ){
      return false;
    }
    else{
      return true;
    }
    
  }
  
  /**
   * Initiate transfers and return identifiers:
   * "srm-{get|put|copy}::srm request id::transfer index::'srcTurl' 'destTurl' 'srmSurl'"
   * @param   srcUrls    the source URLs
   * @param   destUrls   the destination URLs
   */
  public String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls)
     throws ClientException, ServerException, FTPException, IOException,
     SRMException {
    
    ISRM srm = null;

    try{
      String[] srcStrs = new String[srcUrls.length];
      String[] destStrs = new String[destUrls.length];
      for(int i=0;i<srcUrls.length;++i){
        srcStrs[i] = srcUrls[i].getURL();
      }
      for(int i=0;i<destUrls.length;++i){
        destStrs[i] = destUrls[i].getURL();
      }
      
      boolean[] wantPerm = new boolean[srcUrls.length];
      // Don't request permanency.
      // TODO: make this configurable.
      Arrays.fill(wantPerm, false);
      
      // TODO: check location and choose protocol. Choose among registered FT plugins.
      String [] protocols = {"gsiftp"};
      
      int fromType = getUrlType(srcUrls[0]);
      checkURLsUniformity(fromType, srcUrls, true);
      int toType = getUrlType(destUrls[0]);
      checkURLsUniformity(toType, destUrls, false);
      
      if(fromType==SRM_URL &&
          ((toType & FILE_URL)==FILE_URL || (toType & GSIFTP_URL)==GSIFTP_URL)){
        // if the source is srm and the destination is file or gsiftp we get
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
          if((toType & FILE_URL)==FILE_URL && !(new File(fsPath)).getParentFile().canWrite()){
            throw new IOException("ERROR: destination directory is not writeable. "+ fsPath);
          }
          sources[i] = srmSrc.getURL();
          Debug.debug("source file #"+i+" : "+sources[i], 2);
          dests[i] = fsPath;
        }   
        srm = connect(srcUrls[0]);
        RequestStatus rs = srm.get(sources, protocols);
        if(rs==null){
          throw new IOException("ERROR: null request status");
        }
        GlobusURL [] turls = new GlobusURL[srcUrls.length];
        String [] ids = new String[srcUrls.length];
        String [] assignedTurls = null;
        // First, assign temporary ids (with TURL null) and wait for ready
        Vector thesePendingIDs = new Vector();
        for(int i=0; i<srcUrls.length; ++i){
          thesePendingIDs.add(pluginName+"-get::"+rs.requestId+"::"+i+"::'"+null+"' '"+destUrls[i].getURL()+
          "' '"+srcUrls[0].getURL()+"'");
        }
        try{
          Debug.debug("Transfer request submitted for get. Waiting for ok.", 2);
          // show message on status bar on monitoring frame
          StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
          statusBar.setLabel("Waiting for file(s) to be ready...");
          assignedTurls = waitForOK(thesePendingIDs);
          Debug.debug("Assigned TURLs: "+Util.arrayToString(assignedTurls), 2);
        }
        catch(Exception e){
          e.printStackTrace();
          throw new IOException("ERROR: problem getting transfers ready confirmation" +
              "from server. "+ e.getMessage());
        }
        // Now, assign the real ids (with TURL not null)
        try{
          Debug.debug("Request: "+rs.toString(), 3);
          Debug.debug("File statuses: "+srcUrls.length+":"+rs.fileStatuses.length, 3);
          Debug.debug("File status 0: "+rs.fileStatuses[0], 3);
          for(int i=0; i<srcUrls.length; ++i){
            if(assignedTurls!=null && assignedTurls[i]!=null){
              if(assignedTurls[i].equals("Failed")){
                throw new SRMException("Failed to get TURL.");
              }
              turls[i] = new GlobusURL(assignedTurls[i]);
            }
            else{
              turls[i] = new GlobusURL(rs.fileStatuses[i].TURL);
            }
            ids[i] = pluginName+"-get::"+rs.requestId+"::"+i+"::'"+turls[i].getURL()+"' '"+destUrls[i].getURL()+
            "' '"+srcUrls[0].getURL()+"'";
          }
        }
        catch(Exception e){
          e.printStackTrace();
          throw new IOException("ERROR: problem getting TURLS " +
              e.getMessage());
        }
        // if no exception was thrown, all is ok and we can set files to "Running"
        for(int i=0; i<srcUrls.length; ++i){
          try{
            srm.setFileStatus(rs.requestId, rs.fileStatuses[i].fileId, "Running");
            Debug.debug("Status is now "+rs.fileStatuses[i].state, 3);
          }
          catch(Exception e){
            GridPilot.getClassMgr().getLogFile().addMessage(
                "WARNING: could not set file to Running. "+turls[i], e);
          }
        }
        // show message on status bar on monitoring frame
        StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        statusBar.setLabel("File(s) ready, starting download.");
        try{
          // Now use some other plugin - depending on the TURL returned
          TransferControl.startCopyFiles(turls, destUrls);
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
      else if(fromType==SRM_URL && toType!=SRM_URL){
        // if the source is srm and the destination is something other
        // than SRM or GSIFTP, but not file, we push from the source
        srm = connect(srcUrls[0]);
        RequestStatus rs = srm.copy(srcStrs, destStrs, wantPerm);
        if(rs==null){
          throw new IOException("ERROR: null requests status");
        }
        String [] ids = new String[srcUrls.length];
        for(int i=0; i<srcUrls.length; ++i){
          ids[i] = pluginName+"-copy::"+rs.requestId+"::"+i+"::'"+
             rs.fileStatuses[i].TURL+"' '"+destUrls[i].getURL()+
             "' '"+srcUrls[0].getURL()+"'";
        }
        Debug.debug("Returning TURLS "+Util.arrayToString(ids), 2);
        return ids;
      }
      else if((fromType & FILE_URL)==FILE_URL && toType==SRM_URL){
        // if the destination is srm and the source is file we put
        String sources[] = new String[srcUrls.length];
        long sizes[] = new long[srcUrls.length];
        String dests[] = new String[srcUrls.length];
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
          sizes[i] = f.length();
          dests[i] = srmDest.getURL();
        }   
        srm = connect(destUrls[0]);
        RequestStatus rs = srm.put(dests, dests, sizes, wantPerm, protocols);
        if(rs==null){
          throw new IOException("ERROR: null requests status");
        }
        GlobusURL [] turls = new GlobusURL[srcUrls.length];
        String [] ids = new String[srcUrls.length];
        String [] assignedTurls = null;
        // First, assign temporary ids (with TURL null) and wait for ready
        Vector thesePendingIDs = new Vector();
        for(int i=0; i<srcUrls.length; ++i){
          thesePendingIDs.add(pluginName+"-get::"+rs.requestId+"::"+i+"::'"+srcUrls[i].getURL()+"' '"+null+
          "' '"+destUrls[0].getURL()+"'");
        }
        try{
          Debug.debug("Transfer request submitted for put. Waiting for ok.", 2);
          // show message on status bar on monitoring frame
          StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
          statusBar.setLabel("Waiting for file(s) to be ready...");
          assignedTurls = waitForOK(thesePendingIDs);
        }
        catch(Exception e){
          throw new IOException("ERROR: problem getting transfers ready confirmation" +
              "from server."+ e.getMessage());
        }
        // Now, assign the real ids (with TURL not null)
        try{
          for(int i=0; i<srcUrls.length; ++i){
            if(assignedTurls!=null && assignedTurls[i]!=null){
              if(assignedTurls[i].equals("Failed")){
                throw new SRMException("Failed to get TURL.");
              }
              turls[i] = new GlobusURL(assignedTurls[i]);
            }
            else{
              turls[i] = new GlobusURL(rs.fileStatuses[i].TURL);
            }
            ids[i] = pluginName+"-get::"+rs.requestId+"::"+i+"::'"+srcUrls[i].getURL()+"' '"+turls[i].getURL()+
            "' '"+destUrls[0].getURL()+"'";
          }
        }
        catch(Exception e){
          throw new IOException("ERROR: problem getting TURLS" +
              e.getMessage());
        }
        // if no exception was thrown, all is ok and we can set files to "Running"
        for(int i=0; i<srcUrls.length; ++i){
          try{
            srm.setFileStatus(rs.requestId, rs.fileStatuses[i].fileId, "Running");
          }
          catch(Exception e){
            GridPilot.getClassMgr().getLogFile().addMessage(
                "WARNING: could not set file to Running. "+turls[i], e);
          }
        }
        // show message on status bar on monitoring frame
        StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
        statusBar.setLabel("File(s) ready, starting download.");
        try{
          // Now use some other plugin - depending on the TURL returned
          TransferControl.startCopyFiles(srcUrls, turls);
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
      else if(fromType!=SRM_URL && toType==SRM_URL){
        // if the destination is srm and the source is something other
        // than SRM, but not file, we pull from the destination
        srm = connect(destUrls[0]);
        RequestStatus rs = srm.copy(srcStrs, destStrs, wantPerm);
        if(rs==null){
          throw new IOException("ERROR: null requests status");
        }
        String [] ids = new String[srcUrls.length];
        Arrays.fill(ids, Integer.toString(rs.requestId));
        for(int i=0; i<srcUrls.length; ++i){
          ids[i] = pluginName+"-copy::"+rs.requestId+"::"+i+"::'"+
          srcUrls[i].getURL()+"' '"+rs.fileStatuses[i].TURL+
          "' '"+destUrls[0].getURL()+"'";
        }
        return ids;
      }
      else if(fromType==SRM_URL && toType==SRM_URL){
        // both source(s) and destination(s) are srm urls
        // we can either push or pull - we pull.
        // In this case we cannot know the source TURL (gsiftp://...) and use
        // the source SURL (srm://...) for the ID string.
        // TODO: perhaps we should try both...
        srm = connect(destUrls[0]);
        RequestStatus rs = srm.copy(srcStrs, destStrs, wantPerm);
        if(rs==null){
          throw new IOException("ERROR: null requests status");
        }
        String [] ids = new String[srcUrls.length];
        for(int i=0; i<srcUrls.length; ++i){
          ids[i] = pluginName+"-copy::"+rs.requestId+"::"+i+"::'"+srcStrs[i]+"' '"+
          rs.fileStatuses[i].TURL+"' '"+destUrls[0].getURL()+"'";
        }
        return ids;
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
      throw new SRMException(e.getMessage());
    }
    finally{
      try{
        // No cleanup needed...
      }
      catch(Exception e){
      }
    }
  }
  
  /**
   * Delete a list of files.
   * @param   destUrls    list of files to be deleted on the SRM server.
   */
  public void deleteFiles(GlobusURL [] destUrls)
     throws ClientException, ServerException, FTPException, IOException,
     SRMException {

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

    ISRM srm = null;
    try{
      srm = connect(destUrls[0]);
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem deleting files. Could not connect "+
          Util.arrayToString(urls)+". "+e.getMessage());
    }
    
    try{
      srm.advisoryDelete(urls);
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem deleting files "+
          Util.arrayToString(urls)+". "+e.getMessage());
    }
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

  public static void  checkURLsUniformity(int type, GlobusURL urls[],
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
        internalStatus = TransferControl.getInternalStatus(shortID, status);
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

}
