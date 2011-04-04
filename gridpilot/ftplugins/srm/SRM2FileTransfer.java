package gridpilot.ftplugins.srm;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.rmi.RemoteException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.HashMap;
import java.util.Vector;

import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.FTPException;
import org.globus.ftp.exception.ServerException;
import org.globus.util.GlobusURL;

import org.apache.axis.types.URI;
import org.apache.axis.types.UnsignedLong;
import org.dcache.srm.SRMException;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.v2_2.*;

import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.StatusBar;
import gridpilot.MyTransferControl;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

/**
 * Implementation of SRM version 2 support, using the dCache jar.
 */
public class SRM2FileTransfer implements FileTransfer {
  
  private Vector<String> pendingIDs = new Vector<String> ();
  private String user = null;
  
  // Default to trying 5 checks after submitting a transfer
  private int checkRetries = 5;
  // Default to sleeping 10 seconds between each check retry
  private long checkRetrySleep = 10000;

  private String copyRetries = "0";
  private String copyRetryTimeout = "120";
  // TODO: check location and choose protocol. Choose among registered FT plugins.
  private String [] supportedTransferProtocols = {"gsiftp"};
  
  private static final int SRM_URL = 0x1;
  private static final int FILE_URL = 0x8;
  private static final int SUPPORTED_PROTOCOL_URL = 0x4;
  private static final int EXISTS_FILE_URL = 0x10;
  private static final int CAN_READ_FILE_URL = 0x20;
  private static final int CAN_WRITE_FILE_URL = 0x40;
  private static final int DIRECTORY_URL = 0x80;
  private static final int UNKNOWN_URL = 0x100;
  private static final int GSIFTP_URL = 0x200;
  
  private String pluginName;
  private HashMap<String, Long> startDates;

  public SRM2FileTransfer() throws IOException, GeneralSecurityException{
    pluginName = "srm";
    if(!GridPilot.IS_SETUP_RUN){
      user = GridPilot.getClassMgr().getSSL().getGridSubject();
    }

    //System.setProperty("X509_CERT_DIR",
    //    Util.getProxyFile().getParentFile().getAbsolutePath());
    if(GridPilot.GLOBUS_TCP_PORT_RANGE==null || GridPilot.GLOBUS_TCP_PORT_RANGE.equals("")){
      Debug.debug("WARNING: globus tcp port range is not set. SRM may not work.", 1);
      GridPilot.GLOBUS_TCP_PORT_RANGE = "";
    }
    else{
      System.setProperty("org.globus.tcp.port.range", GridPilot.GLOBUS_TCP_PORT_RANGE);
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
    startDates = new HashMap<String, Long>();
  }
  
  public String getName() {
    return "srm";
  }

  public String getUserInfo(){
    return user;
  }
  
  private TTransferParameters getTransferParameters(){
    TTransferParameters tTransferParameters = new TTransferParameters();
    ArrayOfString protocols = new ArrayOfString();
    protocols.setStringArray(supportedTransferProtocols);
    tTransferParameters.setArrayOfTransferProtocols(protocols);
    return tTransferParameters;
  }
  
  /**
   * Connect to the SRM server.
   * @param srmUrl URL of the SRM server.
   */
  private ISRM connect(GlobusURL srmUrl) throws Exception {
    ISRM srm = null;
    try{
      GridPilot.getClassMgr().getSSL().activateProxySSL();
       srm = new SRMClientV2(
           srmUrl,
           GridPilot.getClassMgr().getSSL().getGridCredential(),
           Long.parseLong(Integer.toString(
               1000*Integer.parseInt(copyRetryTimeout))),
           Integer.parseInt(copyRetries),
           new SRMLogger(true),
           /*doDelegation*/true,
           /*fullDelegation*/true,
           /*gss_expected_name*/"host",
           "srm/managerv2");
       Debug.debug("connected to server, obtained proxy of type "+srm.getClass(), 2);
    }
    catch(Exception srme){
      srme.printStackTrace();
       //throw new IOException(srme.toString());
    }
    if(srm!=null){
      return srm;
    }
    try{
      srm = new SRMClientV2(
          srmUrl,
          GridPilot.getClassMgr().getSSL().getGridCredential(),
          Long.parseLong(Integer.toString(
              1000*Integer.parseInt(copyRetryTimeout))),
          Integer.parseInt(copyRetries),
          new SRMLogger(true),
          /*doDelegation*/true,
          /*fullDelegation*/true,
          /*gss_expected_name*/"host",
          "srm/managerv2.wsdl");
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
   * @throws Exception 
   * @throws MalformedURLException 
   */
  public String getStatus(String fileTransferID) throws Exception {
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String shortID = idArr[7];
    
    String status = null;
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();

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
      return status;
    }
    else{
      return getSRMStatus(fileTransferID).statusStr;
    }
  }
  
  private SRMStatus getSRMStatus(String fileTransferID) throws Exception{
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String requestId = idArr[2];
    int statusIndex = Integer.parseInt(idArr[3]);
    String surl = idArr[6];
    ISRM srm = connect(new GlobusURL(surl));
    SRMStatus status = null;
    if(requestType.equalsIgnoreCase("get")){
      status = getGetStatus(requestId, statusIndex, srm);
    }
    else if(requestType.equalsIgnoreCase("put")){
      status = getPutStatus(requestId, statusIndex, srm);
    }
    else if(requestType.equalsIgnoreCase("copy")){
      status = getCopyStatus(requestId, statusIndex, srm);
    }
    return status;
  }
  
  private SRMStatus getGetStatus(String requestId, int statusIndex, ISRM srm) throws RemoteException {
    SrmStatusOfGetRequestRequest sgreq = new SrmStatusOfGetRequestRequest();
    sgreq.setRequestToken(requestId);
    SrmStatusOfGetRequestResponse sgresp = srm.srmStatusOfGetRequest(sgreq);
    ArrayOfTGetRequestFileStatus statuses = sgresp.getArrayOfFileStatuses();
    TGetRequestFileStatus fileStatus = statuses.getStatusArray()[statusIndex];
    String statusStr = fileStatus.getStatus().getStatusCode().getValue();
    TStatusCode statusCode = fileStatus.getStatus().getStatusCode();
    String turl = fileStatus.getTransferURL()==null?null:fileStatus.getTransferURL().toString();
    Integer estimatedWaitTime = fileStatus.getEstimatedWaitTime();
    UnsignedLong fileSize = fileStatus.getFileSize();
    Debug.debug("Got status from SRM server: "+statusIndex+" : "+statusStr+
        " : "+turl, 2);
    if((statusCode==TStatusCode.SRM_FILE_PINNED ||
        statusCode==TStatusCode.SRM_FILE_IN_CACHE) && turl!=null){
      statusStr = turl;
    }
    return new SRMStatus(
        statusStr,
        statusCode,
        turl,
        estimatedWaitTime==null?-1:estimatedWaitTime,
        fileSize
    );
  }

  private SRMStatus getPutStatus(String requestId, int statusIndex, ISRM srm) throws RemoteException {
    SrmStatusOfPutRequestRequest sgreq = new SrmStatusOfPutRequestRequest();
    sgreq.setRequestToken(requestId);
    SrmStatusOfPutRequestResponse sgresp = srm.srmStatusOfPutRequest(sgreq);
    ArrayOfTPutRequestFileStatus statuses = sgresp.getArrayOfFileStatuses();
    TPutRequestFileStatus fileStatus = statuses.getStatusArray()[statusIndex];   
    String statusStr = fileStatus.getStatus().getStatusCode().getValue();
    TStatusCode statusCode = fileStatus.getStatus().getStatusCode();
    String turl = fileStatus.getTransferURL().toString();
    Integer estimatedWaitTime = fileStatus.getEstimatedWaitTime();
    UnsignedLong fileSize = fileStatus.getFileSize();
    Debug.debug("Got status from SRM server: "+statusIndex+" : "+statusStr+
        " : "+turl, 2);
    if((statusCode==TStatusCode.SRM_FILE_IN_CACHE || statusCode==TStatusCode.SRM_SUCCESS) && turl!=null){
      statusStr = turl;
    }
    return new SRMStatus(statusStr, statusCode, turl, estimatedWaitTime, fileSize);
  }

  private SRMStatus getCopyStatus(String requestId, int statusIndex, ISRM srm) throws RemoteException {
    SrmStatusOfCopyRequestRequest sgreq = new SrmStatusOfCopyRequestRequest();
    sgreq.setRequestToken(requestId);
    SrmStatusOfCopyRequestResponse sgresp = srm.srmStatusOfCopyRequest(sgreq);
    ArrayOfTCopyRequestFileStatus statuses = sgresp.getArrayOfFileStatuses();
    TCopyRequestFileStatus fileStatus = statuses.getStatusArray()[statusIndex];   
    String statusStr = fileStatus.getStatus().getStatusCode().getValue();
    TStatusCode statusCode = fileStatus.getStatus().getStatusCode();
    // Use the target SURL as TURL for the GridPilot identifier
    String turl = fileStatus.getTargetSURL().toString();
    Integer estimatedWaitTime = fileStatus.getEstimatedWaitTime();
    UnsignedLong fileSize = fileStatus.getFileSize();
    Debug.debug("Got status from SRM server: "+statusIndex+" : "+statusStr+
        " : "+turl, 2);
    if((statusCode==TStatusCode.SRM_FILE_IN_CACHE || statusCode==TStatusCode.SRM_SUCCESS) && turl!=null){
      statusStr = turl;
    }
    return new SRMStatus(statusStr, statusCode, turl, estimatedWaitTime, fileSize);
  }
  
  private class SRMStatus{
    String statusStr;
    TStatusCode statusCode;
    String turl;
    int estimatedWaitTime;
    UnsignedLong fileSize;
    SRMStatus(String _statusStr, TStatusCode _statusCode, String _turl, int _estimatedWaitTime, UnsignedLong _fileSize){
      statusStr = _statusStr;
      statusCode = _statusCode;
      turl = _turl;
      estimatedWaitTime = _estimatedWaitTime;
      fileSize = _fileSize;
    }
  }

  public String getFullStatus(String fileTransferID) throws Exception {
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String surl = idArr[6];
    String shortID = idArr[7];
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();

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
      SRMStatus srmStatus = getSRMStatus(fileTransferID);
      status += "\nSRM Status: "+srmStatus.statusStr;
      status += "\nEstimated time waiting time: "+srmStatus.estimatedWaitTime;
      status += "\nFile size: "+srmStatus.fileSize;
      status += "\nTransport URL: "+srmStatus.turl;
    }
    catch(Exception e){
      status += "\nERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage();
    }
    return status;
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
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String shortID = idArr[7];
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();

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
        SRMStatus srmStatus = getSRMStatus(fileTransferID);
        int waitTime = srmStatus.estimatedWaitTime;
        Date currentDate = new Date();
        long startDate = startDates.get(fileTransferID);
        long total = currentDate.getTime() + waitTime - startDate;
        long diff = currentDate.getTime() - startDate;
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
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String shortID = idArr[7];
    long bytes = -1;
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();

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
  /*private long getFileBytes(String fileTransferID) throws Exception {
    SRMStatus srmStatus = getSRMStatus(fileTransferID);
    return srmStatus.fileSize.longValue();
  }*/
  
  /**
   * Get the size of a file in bytes.
   * @throws Exception 
   */
  public long getFileBytes(GlobusURL surl) throws Exception {
    return srmLs(new GlobusURL[] {surl})[0].getSize().longValue();
  }
    
  private TMetaDataPathDetail[] srmLs(GlobusURL [] surls) throws Exception{
    ISRM srm = connect(surls[0]);
    SrmLsRequest lsReq = new SrmLsRequest();
    ArrayOfAnyURI aSurls = new ArrayOfAnyURI();
    URI[] uris = new URI[surls.length];
    for(int i=0; i<surls.length; ++i){
      uris[i] = new URI(surls[i].getURL());
    }
    aSurls.setUrlArray(uris);
    lsReq.setArrayOfSURLs(aSurls);
    SrmLsResponse lsResp = srm.srmLs(lsReq);
    return lsResp.getDetails().getPathDetailArray();
  }
  
  /**
   * Release the file on the SRM server.
   * @param   fileTransferID   the unique ID of this transfer, unique to GridPilot
   *                           and not to be confused with the request id or the
   *                           index of the file in question in the array
   *                           RequestStatus.fileStatuses.
   * @throws IOException 
   */
  public void finalize(String fileTransferID) throws Exception {    
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String requestId = idArr[2];
    String destUrl = idArr[5];
    String surl = idArr[6];

    Debug.debug("Finalizing request "+requestId, 2);
    ISRM srm = connect(new GlobusURL(surl));
    startDates.remove(fileTransferID);
    if(requestType.equalsIgnoreCase("put")){
      SrmPutDoneRequest putDoneReq = new SrmPutDoneRequest();
      putDoneReq.setRequestToken(requestId);
      srm.srmPutDone(putDoneReq);
    }
    SrmReleaseFilesRequest releaseFilesReq = new SrmReleaseFilesRequest();
    releaseFilesReq.setRequestToken(requestId);
    srm.srmReleaseFiles(releaseFilesReq);

    File destinationFile = new File((new GlobusURL(destUrl)).getPath());
    try{
      GridPilot.getClassMgr().getFileCacheMgr().writeCacheInfo(destinationFile);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("Could not write cache information for "+destinationFile);
    }
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
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String requestId = idArr[2];
    String surl = idArr[6];
    String shortID = idArr[7];
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();

    startDates.remove(fileTransferID);
    ISRM srm = connect(new GlobusURL(surl));
    SrmAbortRequestRequest srmAbortRequestRequest = new SrmAbortRequestRequest();
    srmAbortRequestRequest.setRequestToken(requestId);
    SrmAbortRequestResponse srmAbortRequestResponse = srm.srmAbortRequest(srmAbortRequestRequest);
    if(srmAbortRequestResponse.getReturnStatus().getStatusCode()!=TStatusCode.SRM_SUCCESS){
      GridPilot.getClassMgr().getLogFile().addMessage("Problem aborting request "+requestId+"."+
          srmAbortRequestResponse.getReturnStatus().getExplanation());
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
        GridPilot.getClassMgr().getLogFile().addMessage(
            "WARNING: could not cancel "+shortID+". "+e.getMessage(), e);
      }
    }

  }

  // Wait for all files to be ready on the SRM server before beginning the
  // transfers. This may be reconsidered...
  private String [] waitForOK(Vector<String> _thesePendingIDs) throws SRMException {
    synchronized(pendingIDs){
      Vector<String> thesePendingIDs = _thesePendingIDs;
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
          String id = thesePendingIDs.get(j);
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
            else if(status.equalsIgnoreCase(TStatusCode._SRM_FILE_IN_CACHE)){
              pendingIDs.remove(id);
              thesePendingIDs.remove(id);
            }
            else if(status.equalsIgnoreCase(TStatusCode._SRM_FAILURE)){
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
   * Initiate transfers and return identifiers:
   * "srm-{get|put|copy}::srm request id::transfer index::'srcTurl' 'destTurl' 'srmSurl'"
   * @param   srcUrls    the source URLs
   * @param   destUrls   the destination URLs
   * @throws Exception 
   */
  public String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws Exception {
    int fromType = getUrlType(srcUrls[0]);
    checkURLsUniformity(fromType, srcUrls, true);
    int toType = getUrlType(destUrls[0]);
    checkURLsUniformity(toType, destUrls, false);
    
    if(fromType==SRM_URL &&
        ((toType & FILE_URL)==FILE_URL || (toType & GSIFTP_URL)==GSIFTP_URL)){
      return copySrmToFileOrGsiftp(srcUrls, destUrls);
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
  
  // if the source is srm and the destination is file or gsiftp we get
  private String[] copySrmToFileOrGsiftp(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws Exception {
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();
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
    ISRM srm = connect(srcUrls[0]);
    TGetFileRequest [] tGetFileRequests = new TGetFileRequest[srcUrls.length];
    for(int i=0; i<srcUrls.length; ++i){
      tGetFileRequests[i] = new TGetFileRequest();
      tGetFileRequests[i].setSourceSURL(new URI(srcUrls[i].getURL()));
    }
    ArrayOfTGetFileRequest arrayOfFileRequests = new ArrayOfTGetFileRequest();
    arrayOfFileRequests.setRequestArray(tGetFileRequests);
    SrmPrepareToGetRequest prepareToGetReq = new SrmPrepareToGetRequest();
    prepareToGetReq.setTransferParameters(getTransferParameters());
    prepareToGetReq.setArrayOfFileRequests(arrayOfFileRequests);
    SrmPrepareToGetResponse srmPrepareToGetResponse = srm.srmPrepareToGet(prepareToGetReq);
    String requestId = srmPrepareToGetResponse.getRequestToken();
    String info = "Got SrmPrepareToGetResponse "+
       srmPrepareToGetResponse.getReturnStatus().getStatusCode().getValue()+
       " --> "+srmPrepareToGetResponse.getReturnStatus().getExplanation();
    Debug.debug(info, 2);
    if(requestId==null){
      throw new IOException(info);
    }
    GlobusURL [] turls = new GlobusURL[srcUrls.length];
    String [] ids = new String[srcUrls.length];
    String [] assignedTurls = null;
    // First, assign temporary ids (with TURL null) and wait for ready
    Vector<String> thesePendingIDs = new Vector<String>();
    for(int i=0; i<srcUrls.length; ++i){
      thesePendingIDs.add(pluginName+"-get::"+requestId+"::"+i+"::'"+null+"' '"+destUrls[i].getURL()+
      "' '"+srcUrls[0].getURL()+"'");
    }
    Debug.debug("Transfer request submitted for get. Waiting for ok.", 2);
    // show message on status bar on monitoring frame
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar();
    statusBar.setLabel("Waiting for file(s) to be ready...");
    assignedTurls = waitForOK(thesePendingIDs);
    Debug.debug("Assigned TURLs: "+MyUtil.arrayToString(assignedTurls), 2);
    // Now, assign the real ids (with TURL not null)
    Debug.debug("File statuses: "+MyUtil.arrayToString(
        srmPrepareToGetResponse.getArrayOfFileStatuses().getStatusArray()), 3);
    for(int i=0; i<srcUrls.length; ++i){
      if(assignedTurls==null || assignedTurls[i]==null || assignedTurls[i].equals("Failed")){
        throw new IOException("Failed to get TURL.");
      }
      else{
        turls[i] = new GlobusURL(assignedTurls[i]);
      }
      ids[i] = pluginName+"-get::"+requestId+"::"+i+"::'"+turls[i].getURL()+"' '"+destUrls[i].getURL()+
         "' '"+srcUrls[0].getURL()+"'";
      startDates.put(ids[i], MyUtil.getDateInMilliSeconds()*1000);
    }
    // if no exception was thrown, all is ok and we can set files to "File busy"
    try{
      ArrayOfTGetRequestFileStatus arrayOfFileStatuses = new ArrayOfTGetRequestFileStatus();
      TGetRequestFileStatus [] tGetRequestFileStatuses = new TGetRequestFileStatus [srcUrls.length];
      TReturnStatus tReturnStatus;
      for(int i=0; i<srcUrls.length; ++i){
        tGetRequestFileStatuses[i] = new TGetRequestFileStatus();
        tReturnStatus = new TReturnStatus();
        tReturnStatus.setStatusCode(TStatusCode.SRM_FILE_BUSY);
        tGetRequestFileStatuses[i].setStatus(tReturnStatus);
      }
      arrayOfFileStatuses.setStatusArray(tGetRequestFileStatuses);
      srmPrepareToGetResponse.setArrayOfFileStatuses(arrayOfFileStatuses);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "WARNING: could not set status of files to busy.", e);
    }
    // show message on status bar on monitoring frame
    statusBar.setLabel("File(s) ready, starting download.");
    try{
      // Now use some other plugin - depending on the TURL returned
      Debug.debug("Starting to copy files with transferControl "+transferControl+"<--"+GridPilot.getClassMgr().getTransferControl(), 3);
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
    ISRM srm = connect(srcUrls[0]);
    TPutFileRequest [] tPutFileRequests = new TPutFileRequest[srcUrls.length];
    for(int i=0; i<srcUrls.length; ++i){
      tPutFileRequests[i] = new TPutFileRequest();
      tPutFileRequests[i].setTargetSURL(new URI(srcUrls[i].getURL()));
    }
    ArrayOfTPutFileRequest arrayOfFileRequests = new ArrayOfTPutFileRequest();
    arrayOfFileRequests.setRequestArray(tPutFileRequests);
    SrmPrepareToPutRequest prepareToPutReq = new SrmPrepareToPutRequest();
    prepareToPutReq.setArrayOfFileRequests(arrayOfFileRequests);
    prepareToPutReq.setTransferParameters(getTransferParameters());
    SrmPrepareToPutResponse srmPrepareToPutResponse = srm.srmPrepareToPut(prepareToPutReq);
    String requestId = srmPrepareToPutResponse.getRequestToken();
    String info = "Got SrmPrepareToGetResponse "+
       srmPrepareToPutResponse.getReturnStatus().getStatusCode().getValue()+
       " --> "+srmPrepareToPutResponse.getReturnStatus().getExplanation();
    Debug.debug(info, 2);
    if(requestId==null){
      throw new IOException(info);
    }
    GlobusURL [] turls = new GlobusURL[srcUrls.length];
    String [] ids = new String[srcUrls.length];
    String [] assignedTurls = null;
    // First, assign temporary ids (with TURL null) and wait for ready
    Vector<String> thesePendingIDs = new Vector<String>();
    for(int i=0; i<srcUrls.length; ++i){
      thesePendingIDs.add(pluginName+"-get::"+requestId+"::"+i+"::'"+srcUrls[i].getURL()+"' '"+null+
      "' '"+destUrls[0].getURL()+"'");
    }
    Debug.debug("Transfer request submitted for put. Waiting for ok.", 2);
    // show message on status bar on monitoring frame
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar();
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
      startDates.put(ids[i], MyUtil.getDateInMilliSeconds()*1000);
    }
    // if no exception was thrown, all is ok and we can set files to "File busy"
    try{
      ArrayOfTPutRequestFileStatus arrayOfFileStatuses = new ArrayOfTPutRequestFileStatus();
      TPutRequestFileStatus [] tPutRequestFileStatuses = new TPutRequestFileStatus [srcUrls.length];
      TReturnStatus tReturnStatus;
      for(int i=0; i<srcUrls.length; ++i){
        tPutRequestFileStatuses[i] = new TPutRequestFileStatus();
        tReturnStatus = new TReturnStatus();
        tReturnStatus.setStatusCode(TStatusCode.SRM_FILE_BUSY);
        tPutRequestFileStatuses[i].setStatus(tReturnStatus);
      }
      arrayOfFileStatuses.setStatusArray(tPutRequestFileStatuses);
      srmPrepareToPutResponse.setArrayOfFileStatuses(arrayOfFileStatuses);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "WARNING: could not set status of files to busy.", e);
    }
    // show message on status bar on monitoring frame
    statusBar.setLabel("File(s) ready, starting upload.");
    MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();
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

  // if the source is srm and the destination is something other
  // than SRM or GSIFTP, but not file, we push from the source
  //TODO: how does one set push mode with SRM-2 ?
  private String[] copySrmToOther(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws Exception {
    ISRM srm = connect(srcUrls[0]);
    return copyToOrFromSrm(srm, srcUrls, destUrls);
  }

  // if the destination is srm and the source is something other
  // than SRM, but not file, we pull from the destination...
  // TODO: how does one set pull mode with SRM-2 ?
  private String[] copyOtherToSrm(GlobusURL[] srcUrls, GlobusURL[] destUrls) throws Exception {
    ISRM srm = connect(destUrls[0]);
    return copyToOrFromSrm(srm, srcUrls, destUrls);
  }

  // both source(s) and destination(s) are srm urls
  // we can either push or pull - we pull...
  // TODO: how does one set pull mode with SRM-2 ?
  // In this case we cannot know the source TURL (gsiftp://...) and use
  // the source SURL (srm://...) for the ID string.
  // TODO: perhaps we should try both...
  private String[] copySrmToSrm(GlobusURL[] srcUrls, GlobusURL[] destUrls) throws Exception {
    ISRM srm = connect(srcUrls[0]);
    return copyToOrFromSrm(srm, srcUrls, destUrls);
  }
  
  // Here the SRM server is expected to take care of the copy operation.
  // This is a utility method for the two methods above.
  private String[] copyToOrFromSrm(ISRM srm, GlobusURL[] srcUrls, GlobusURL[] destUrls) throws Exception {
    TCopyFileRequest [] tCopyFileRequests = new TCopyFileRequest[srcUrls.length];
    for(int i=0; i<srcUrls.length; ++i){
      tCopyFileRequests[i] = new TCopyFileRequest();
      tCopyFileRequests[i].setSourceSURL(new URI(srcUrls[i].getURL()));
      tCopyFileRequests[i].setTargetSURL(new URI(destUrls[i].getURL()));
    }
    ArrayOfTCopyFileRequest arrayOfFileRequests = new ArrayOfTCopyFileRequest();
    arrayOfFileRequests.setRequestArray(tCopyFileRequests);
    SrmCopyRequest copyReq = new SrmCopyRequest();
    copyReq.setArrayOfFileRequests(arrayOfFileRequests);
    SrmCopyResponse srmCopyResponse = srm.srmCopy(copyReq);
    String requestId = srmCopyResponse.getRequestToken();
    String [] ids = new String[srcUrls.length];
    String info = "Got SrmPrepareToGetResponse "+
       srmCopyResponse.getReturnStatus().getStatusCode().getValue()+
       " --> "+srmCopyResponse.getReturnStatus().getExplanation();
    Debug.debug(info, 2);
    if(requestId==null){
      throw new IOException(info);
    }
    for(int i=0; i<srcUrls.length; ++i){
      ids[i] = pluginName+"-copy::"+requestId+"::"+i+"::'"+srcUrls[i].getURL()+"' '"+
         destUrls[i].getURL()+"' '"+destUrls[i].getURL()+"'";
      startDates.put(ids[i], MyUtil.getDateInMilliSeconds()*1000);
    }
    return ids;
  }
  
  /**
   * Delete a list of files.
   * @param   destUrls    list of files to be deleted on the SRM server.
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

    ISRM srm = connect(destUrls[0]);
    
    SrmRmRequest srmRmRequest = new SrmRmRequest();
    ArrayOfAnyURI aSurls = new ArrayOfAnyURI();
    srmRmRequest.setArrayOfSURLs(aSurls);
    SrmRmResponse srmRmResponse = srm.srmRm(srmRmRequest);
    if(srmRmResponse.getReturnStatus().getStatusCode()!=TStatusCode.SRM_SUCCESS){
      throw new IOException("Problem deleting files "+MyUtil.arrayToString(destUrls)+"."+
          srmRmResponse.getReturnStatus().getExplanation());
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
    String [] idArr = MyUtil.parseSrmFileTransferID(fileTransferID);
    String requestType = idArr[1];
    String shortID = idArr[7];

    int internalStatus = -1;

    if(!pendingIDs.contains(fileTransferID) && (
        requestType.equals("get") || requestType.equals("put"))){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      MyTransferControl transferControl = GridPilot.getClassMgr().getTransferControl();
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
    else if(ftStatus!=null && ftStatus.equalsIgnoreCase("Pending")){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus!=null && (ftStatus.equalsIgnoreCase("Ready") ||
        status.matches("^\\w+://.*"))){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus!=null && ftStatus.equalsIgnoreCase("Running")){
      ret = FileTransfer.STATUS_RUNNING;
    }
    else if(ftStatus!=null && ftStatus.equalsIgnoreCase("Done")){
      ret = FileTransfer.STATUS_DONE;
    }
    else if(ftStatus!=null && ftStatus.equalsIgnoreCase("Failed")){
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

  public void putFile(File file, GlobusURL globusFileUrl) throws Exception {
      // No point in implementing this. SRM is anyway not browsable.
     throw new IOException("getFile not supported by SRM plugin.");
  }

  public Vector<String> list(GlobusURL globusUrl, String filter) throws Exception {
    // No point in implementing this. SRM is anyway not browsable.
    throw new IOException("list not supported by SRM plugin.");
  }
  
  public Vector<String> find(GlobusURL globusUrl, String filter) throws Exception {
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
