package gridpilot.ftplugins.srm;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
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
    
  private String user = null;

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
   *                           Format: srm-{get|put|copy}:srm request id:transfer index:srcTurl destTurl srmSurl
   */
  public String getStatus(String fileTransferID) throws SRMException {
    String [] idArr = parseFileTransferID(fileTransferID);
    String requestType = idArr[1];
    int requestId = Integer.parseInt(idArr[2]);
    int statusIndex = Integer.parseInt(idArr[3]);
    //String srcTurl = idArr[4];
    //String destTurl = idArr[5];
    String surl = idArr[6];

    String status = null;

    if(requestType.equals("get") || requestType.equals("put")){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        //status = TransferControl.getStatus(srcTurl+" "+destTurl);
        status = TransferControl.getStatus(fileTransferID);
      }
      catch(Exception e){
        Debug.debug("ERROR: could not get status from subsystem for "+
            fileTransferID+". "+e.getMessage(), 1);
      }
    }
    
    if(status!=null){
      return(status);
    }
    else{
      try{
        ISRM srm = connect(new GlobusURL(surl));
        RequestStatus rs = srm.getRequestStatus(requestId);
        status = rs.fileStatuses[statusIndex].state;
        status += ";"+rs.estTimeToStart+":"+
                      rs.fileStatuses[statusIndex].estSecondsToStart+":"+
                      rs.finishTime;
        return status;
      }
      catch(Exception e){
        throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
      }
    }
  }
  
  /**
   * Parse the file transfer ID into
   * {protocol, requestType (get|put|copy), requestId, statusIndex, srcTurl, destTurl, srmSurl}.
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

    String [] idArr = Util.split(fileTransferID, ":");
    String [] head = Util.split(idArr[0], "-");
    if(idArr.length<3){
      throw new SRMException("ERROR: malformed ID "+fileTransferID);
    }
    try{
      protocol = head[0];
      requestType = head[1];
      requestId = idArr[1];
      statusIndex = idArr[2];
      String turls = fileTransferID.replaceFirst(idArr[0]+":", "");
      turls = turls.replaceFirst(idArr[1]+":", "");
      turls = turls.replaceFirst(idArr[2]+":", "");
      String [] turlArray = Util.split(turls);
      srcTurl = turlArray[0];
      destTurl = turlArray[1];
      srmSurl = turlArray[2];
    }
    catch(Exception e){
      throw new SRMException("ERROR: could not parse ID "+fileTransferID+". "+e.getMessage());
    }
    return new String [] {protocol, requestType, requestId, statusIndex, srcTurl, destTurl, srmSurl};
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
    //int statusIndex = Integer.parseInt(idArr[3]);
    //String srcTurl = idArr[4];
    //String destTurl = idArr[5];
    String surl = idArr[6];

    int status = -1;

    if(requestType.equals("get") || requestType.equals("put")){
      // get status from GSIFTPFileTransfer (or whichever protocol the SRM uses)
      try{
        //status = TransferControl.getStatus(srcTurl+" "+destTurl);
        status = TransferControl.getPercentComplete(fileTransferID);
      }
      catch(Exception e){
        Debug.debug("ERROR: could not get getPercentComplete from subsystem for "+
            fileTransferID+". "+e.getMessage(), 1);
      }
    }
    
    if(status>-1){
      return(status);
    }
    else{
      try{
        ISRM srm = connect(new GlobusURL(surl));
        RequestStatus rs = srm.getRequestStatus(requestId);
        // TODO: use getEstGetTime, getEstPutTime
        //int tts = rs.estTimeToStart;
        //int sts = rs.fileStatuses[statusIndex].estSecondsToStart;
        Date finishDate = rs.finishTime;
        Date currentDate = new Date();
        Date startDate = rs.submitTime;
        long total = finishDate.getTime() - startDate.getTime();
        long diff = currentDate.getTime() - startDate.getTime();
        float percents = diff / total;
        return (int) percents;
      }
      catch(Exception e){
        throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
      }
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
      ISRM srm = connect(new GlobusURL(surl));
      RequestStatus rs = srm.getRequestStatus(requestId);
      // state should be one of "Pending", "Active", "Done", "Failed".
      if(rs.state.equalsIgnoreCase("Done") || rs.state.equalsIgnoreCase("Failed")){
        srm.setFileStatus(requestId, statusIndex, "Done");
      }
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

    try{
      ISRM srm = connect(new GlobusURL(surl));
      // This cancels and sets to Done.
      srm.setFileStatus(requestId, statusIndex, "Done");
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem with "+requestType+" "+fileTransferID+". "+e.getMessage());
    }
  }

  // Wait for all files to be ready on the SRM server before beginning the
  // transfers. This may be reconsidered...
  private void waitForOK(String [] fileTransferIDs) throws SRMException {
    Vector ids = new Vector();
    Collections.addAll(ids, fileTransferIDs);
    String status = null;
    String id = null;
    int i = 0;
    // This defines a timeout of 10 minutes.
    // TODO: make this configurable.
    int maxIterations = 30;
    long sleepInterval = 20000;
    while(true){
      ++i;
      try{
        Thread.sleep(sleepInterval);
      }
      catch(InterruptedException e){
        break;
      }
      for(Iterator it=ids.iterator(); it.hasNext();){
        id = (String) it.next();
        try{
          status = getStatus(id);
          if(status.equalsIgnoreCase("Ready")){
            ids.remove(id);
          }
        }
        catch(Exception e){
          Debug.debug("WARNING: could not get status of "+id, 1);
        }
      }
      if(ids.size()==0){
        break;
      }
      if(i>maxIterations){
        throw new SRMException("ERROR: files were not ready within "+
            (maxIterations*sleepInterval/1000)+" seconds");
      }
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
        getUrlType(destUrls[0])==UNKNOWN_URL){
      return false;
    }
    else{
      return true;
    }
    
  }
  
  /**
   * Initiate transfers and return identifiers:
   * "srm-{get|put|copy}:srm request id:transfer index:srcTurl destTurl srmSurl"
   * @param   srcUrls    the source URLs
   * @param   destUrls   the destination URLs
   */
  public String [] startCopyFiles(GlobusURL [] srcUrls, GlobusURL [] destUrls)
     throws ClientException, ServerException, FTPException, IOException,
     SRMException {
    
    ISRM srm = null;

    // TODO: implement wildcard *
    
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
      // Don't request any permanent files.
      // TODO: make this configurable.
      Arrays.fill(wantPerm, false);
      
      // TODO: check location and choose protocol
      String [] protocols = {"http", "gsiftp"};
      
      int fromType = getUrlType(srcUrls[0]);
      checkURLsUniformity(fromType, srcUrls, true);
      int toType = getUrlType(destUrls[0]);
      checkURLsUniformity(toType, destUrls, false);
      
      if(fromType==SRM_URL && (toType & FILE_URL)==FILE_URL){
        // if the source is srm and the destination is file we get
        String sources[] = new String[srcUrls.length];
        String dests[] = new String[srcUrls.length];
        for(int i = 0; i<srcUrls.length; ++i){
          GlobusURL srmSrc = srcUrls[i];
          GlobusURL fileDest = destUrls[i];
          if(fileDest.getPath().endsWith("/") || fileDest.getPath().endsWith("\\") ||
              (new File(fileDest.getPath())).isDirectory()){
            throw new IOException("ERROR: destination is directory "+ fileDest.getURL());
          }
          if(!(new File(fileDest.getPath())).getParentFile().canWrite()){
            throw new IOException("ERROR: destination directory is not writeable "+ fileDest.getURL());
          }
          sources[i] = srmSrc.getURL();
          Debug.debug("source file #"+i+" : "+sources[i], 2);
          dests[i] = fileDest.getPath();
        }   
        srm = connect(srcUrls[0]);
        RequestStatus rs = srm.get(sources, protocols);
        if(rs==null){
          throw new IOException("ERROR: null requests status");
        }
        GlobusURL [] turls = new GlobusURL[srcUrls.length];
        String [] ids = new String[srcUrls.length];
        try{
          for(int i=0; i<srcUrls.length; ++i){
            turls[i] = new GlobusURL(rs.fileStatuses[i].TURL);
            ids[i] = pluginName+"-get:"+rs.requestId+":"+i+":"+turls[i]+" "+destUrls[i]+
            " "+srcUrls[0];
          }
        }
        catch(Exception e){
          throw new IOException("ERROR: problem getting TURLS" +
              e.getMessage());
        }
        try{
          waitForOK(ids);
        }
        catch(Exception e){
          throw new IOException("ERROR: problem getting transfers ready confirmation" +
              "from server."+ e.getMessage());
        }
        // if no exception was thrown, all is ok and we can set files to "Running"
        for(int i=0; i<srcUrls.length; ++i){
          try{
            srm.setFileStatus(rs.requestId, i, "Running");
          }
          catch(Exception e){
            Debug.debug("WARNING: could not set file to Running. "+turls[i], 3);
          }
        }
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
          throw new IOException("ERROR: problem queueing transfers "+ e.getMessage());
        }
        return ids;
      }
      else if(fromType==SRM_URL && toType!=SRM_URL){
        // if the source is srm and the destination is something other
        // than SRM, but not file, we push from the source
        srm = connect(srcUrls[0]);
        RequestStatus rs = srm.copy(srcStrs, destStrs, wantPerm);
        if(rs==null){
          throw new IOException("ERROR: null requests status");
        }
        String [] ids = new String[srcUrls.length];
        for(int i=0; i<srcUrls.length; ++i){
          ids[i] = pluginName+"-copy:"+rs.requestId+":"+i+":"+
             rs.fileStatuses[i].TURL+" "+destUrls[i]+
             " "+srcUrls[0];
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
        try{
          for(int i=0; i<srcUrls.length; ++i){
            turls[i] = new GlobusURL(rs.fileStatuses[i].TURL);
            ids[i] = pluginName+"-get:"+rs.requestId+":"+i+":"+srcUrls[i]+" "+turls[i]+
            " "+destUrls[0];
          }
        }
        catch(Exception e){
          throw new IOException("ERROR: problem getting TURLS" +
              e.getMessage());
        }
        try{
          waitForOK(ids);
        }
        catch(Exception e){
          throw new IOException("ERROR: problem getting transfers ready confirmation" +
              "from server."+ e.getMessage());
        }
        // if no exception was thrown, all is ok and we can set files to "Running"
        for(int i=0; i<srcUrls.length; ++i){
          try{
            srm.setFileStatus(rs.requestId, i, "Running");
          }
          catch(Exception e){
            Debug.debug("WARNING: could not set file to Running. "+turls[i], 3);
          }
        }
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
          ids[i] = pluginName+"-copy:"+rs.requestId+":"+i+":"+
          srcUrls[i]+" "+rs.fileStatuses[i].TURL+
          " "+destUrls[0];
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
          ids[i] = pluginName+"-copy:"+rs.requestId+":"+i+":"+srcStrs[i]+" "+
          rs.fileStatuses[i].TURL+" "+destUrls[0];
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
    String [] urls = new String [destUrls.length];
    for(int i=0; i<destUrls.length; ++i){
      urls[i] = destUrls[i].getURL();
    }
    try{
      ISRM srm = connect(destUrls[0]);
      srm.advisoryDelete(urls);
    }
    catch(Exception e){
      throw new SRMException("ERROR: SRM problem deleting files "+
          Util.arrayToString(urls)+". "+e.getMessage());
    }
  }

  public static int getUrlType(GlobusURL url) throws IOException {
    String prot = url.getProtocol();
    if(prot != null ) {
       if(prot.equals("srm")) {
          return SRM_URL;
       } else if(prot.equals("http")   ||
          prot.equals("ftp")    ||
          prot.equals("gsiftp") ||
          prot.equals("gridftp")||
          prot.equals("https")  ||
          prot.equals("ldap")   ||
          prot.equals("ldaps")  ||
          prot.equals("dcap")   ||
          prot.equals("rfio")) {
          return SUPPORTED_PROTOCOL_URL;
       } else if(!prot.equals("file")) {
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
    if(type ==SRM_URL  || ((type & SUPPORTED_PROTOCOL_URL)==SUPPORTED_PROTOCOL_URL)){
      if( host==null || host.equals("") || port<0){
        String error = "illegal source url for multiple sources mode"+
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
            "all sources must have same host"+
            urls[i].getURL();
            Debug.debug(error, 2);
            throw new IllegalArgumentException(error);
          }
          if(port!=urls[i].getPort()){
            String error =
              "if specifying multiple  sources, "+
              "all sources must have same port"+
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
          String error = "source file does not exists"+
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
  public int getInternalStatus(String ftStatus) throws Exception{
    int ret = -1;
    if(ftStatus==null || ftStatus.equals("")){
      // TODO: should this be STATUS_WAIT
      ret = FileTransfer.STATUS_ERROR;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Pending")){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Ready")){
      ret = FileTransfer.STATUS_WAIT;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Running")){
      ret = FileTransfer.STATUS_RUNNING;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Done")){
      ret = FileTransfer.STATUS_DONE;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase("Failed")){
      ret = FileTransfer.STATUS_FAILED;
    }
    return ret;
  }

}
