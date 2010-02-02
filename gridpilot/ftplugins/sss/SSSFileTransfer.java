package gridpilot.ftplugins.sss;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.crypto.NoSuchPaddingException;
import javax.swing.JOptionPane;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.NTCredentials;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScheme;
import org.apache.commons.httpclient.auth.CredentialsNotAvailableException;
import org.apache.commons.httpclient.auth.CredentialsProvider;
import org.apache.commons.httpclient.auth.NTLMScheme;
import org.apache.commons.httpclient.auth.RFC2617Scheme;

import org.globus.util.GlobusURL;

import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.io.BytesProgressWatcher;
import org.jets3t.service.io.GZipDeflatingInputStream;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.multithread.DownloadPackage;
import org.jets3t.service.multithread.S3ServiceEventListener;
import org.jets3t.service.multithread.S3ServiceMulti;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.security.EncryptionUtil;
import org.jets3t.service.utils.FileComparer;
import org.jets3t.service.utils.FileComparerResults;
import org.jets3t.service.utils.Mimetypes;
import org.jets3t.service.utils.ServiceUtils;

import gridfactory.common.ConfirmBox;
import gridfactory.common.Debug;
import gridfactory.common.FileCacheMgr;
import gridfactory.common.FileTransfer;
import gridfactory.common.LogFile;
import gridfactory.common.ResThread;
import gridfactory.common.StatusBar;
import gridfactory.common.TransferInfo;
import gridfactory.common.Util;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

/**
 * Portions of code in this class taken from org.jets3t.samples and org.jets3t.apps.cockpit
 * @author fjob
 *
 */

public class SSSFileTransfer implements FileTransfer, CredentialsProvider{
  
  private AWSCredentials awsCredentials = null;
  private S3Service s3Service = null;
  private S3ServiceMulti s3ServiceMulti = null;
  private LogFile logFile = null;
  private boolean filesWorldReadable = false;
  private boolean compressUploads = false;
  private boolean encryptUploads = false;
  private String encryptionPassword = null;
  private Map filesAlreadyInDownloadDirectoryMap = null;
  private Map s3DownloadObjectsMap = null;
  private Map filesForUploadMap = null;
  private Map s3ExistingObjectsMap = null;
  // Map of listener objctes. One object for each batch of transfers:
  // {id1, id2, id3, ...} -> s3ServiceEventListener, i.e. the keys are arrays of ids
  private HashMap<TransferInfo [], MyS3ServiceEventListener> s3ServiceEventListeners = null;
  private HashMap<String, S3Bucket> myBuckets = null;
  private S3Object[] s3Objects = new S3Object [] {};
  private String accessKey = null;
  // Use to keep track of single-threaded transfers
  private HashSet<TransferInfo> fileTransfers = null;
  private FileCacheMgr fileCacheMgr;
  
  private static boolean S3FOX_DIRECTORY_MODE = false;
  private static String S3FOX_DIRECTORY_SUFFIX = "_$folder$";
  private static String PLUGIN_NAME;
  private static int COPY_TIMEOUT = 10000;
  public static final String APPLICATION_DESCRIPTION = "GridPilot";

  protected final static String STATUS_WAIT = "Wait";
  protected final static String STATUS_TRANSFER = "Transfer";
  protected final static String STATUS_DONE = "Done";
  protected final static String STATUS_ERROR = "Error";

  // Load the AWS credentials. 3s does not support X509 credentials..
  public SSSFileTransfer(){
    PLUGIN_NAME = "sss";
    logFile = GridPilot.getClassMgr().getLogFile();
    fileCacheMgr = GridPilot.getClassMgr().getFileCacheMgr();
    fileTransfers = new HashSet<TransferInfo>();
    myBuckets = new HashMap<String, S3Bucket>();
    s3ServiceEventListeners = new HashMap<TransferInfo [], MyS3ServiceEventListener>();
    accessKey = GridPilot.getClassMgr().getConfigFile().getValue(PLUGIN_NAME,
       "AWS access key id");
    String secretKey = GridPilot.getClassMgr().getConfigFile().getValue(PLUGIN_NAME,
       "AWS secret access key");
    String readableStr = GridPilot.getClassMgr().getConfigFile().getValue(PLUGIN_NAME,
       "World readable files");
    filesWorldReadable = readableStr!=null &&
       (readableStr.equalsIgnoreCase("yes") || readableStr.equalsIgnoreCase("true"));
    String s3foxMode = GridPilot.getClassMgr().getConfigFile().getValue(PLUGIN_NAME,
       "S3fox directory mode");
    S3FOX_DIRECTORY_MODE = s3foxMode!=null &&
       (s3foxMode.equalsIgnoreCase("yes") || s3foxMode.equalsIgnoreCase("true"));
    String compressUploadsStr = GridPilot.getClassMgr().getConfigFile().getValue(PLUGIN_NAME,
       "Compress uploads");
    compressUploads = compressUploadsStr!=null &&
       (compressUploadsStr.equalsIgnoreCase("yes") || compressUploadsStr.equalsIgnoreCase("true"));
    encryptionPassword = GridPilot.getClassMgr().getConfigFile().getValue(PLUGIN_NAME,
       "Encryption password");
    encryptUploads = encryptionPassword!=null && !encryptionPassword.equalsIgnoreCase("");
    awsCredentials = new AWSCredentials(accessKey, secretKey);
    try{
      s3Service = new RestS3Service(awsCredentials, APPLICATION_DESCRIPTION, this);
      S3ServiceEventListener s3Listener = new MyS3ServiceEventListener();
      s3ServiceMulti = new S3ServiceMulti(s3Service, s3Listener);
    }
    catch(S3ServiceException e){
      e.printStackTrace();
      logFile.addMessage("Could not initialize S3 service with the given credentials.", e);
    }
  }
  
  private S3Bucket getBucket(String bucketName, boolean createIfNotThere) throws IOException, S3ServiceException{
    if(myBuckets.containsKey(bucketName)){
      return myBuckets.get(bucketName);
    }
    refreshMyBuckets();
    S3Bucket bucket = null;
    for(Iterator it=myBuckets.keySet().iterator(); it.hasNext();){
      bucket = myBuckets.get(it.next());
      if(bucket.getName().equals(bucketName)){
        return bucket;
      }
    }
    if(createIfNotThere){
      logFile.addInfo("Creating bucket "+bucketName);
      s3Service.createBucket(bucketName);
      return bucket;
    }
    else{
      throw new IOException("Bucket "+bucketName+" not found");
    }
  }
  
  private void refreshMyBuckets() throws S3ServiceException{
    S3Bucket [] myBucketsArr = s3Service.listAllBuckets();
    Debug.debug("Number of buckets in S3? " + myBucketsArr.length, 1);
    S3Bucket bucket = null;
    for(int i=0; i<myBucketsArr.length; ++i){
      bucket = myBucketsArr[i];
      myBuckets.put(bucket.getName(), bucket);
    }
  }

  public boolean checkURLs(GlobusURL[] srcUrls, GlobusURL[] destUrls)
      throws Exception{
    String firstSrcProtocol = srcUrls[0].getProtocol();
    String firstDestProtocol = destUrls[0].getProtocol();
    if(srcUrls.length!=destUrls.length || !(
        firstSrcProtocol.equalsIgnoreCase(PLUGIN_NAME) &&
        firstDestProtocol.equalsIgnoreCase("file") ||
           firstSrcProtocol.equalsIgnoreCase("file") &&
           firstDestProtocol.equalsIgnoreCase(PLUGIN_NAME)
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
   * The URLs are of the form sss://<bucket name>/some/file/name
   */
  public String[] startCopyFiles(GlobusURL[] srcUrls, GlobusURL[] destUrls)
      throws Exception{
    srcUrls = fixUrls(srcUrls);
    destUrls = fixUrls(destUrls);
    TransferInfo [] transfers;
    // Choose upload or download. We assume uniformity of URLs (should've been taken
    // Choose upload or download. We assume uniformity of URLs (should've been taken
    // care of by TransferControl via checkUrls).
    if(srcUrls.length==0){
      return new String [] {};
    }
    if(srcUrls[0].getProtocol().equalsIgnoreCase("file") &&
        destUrls[0].getProtocol().equalsIgnoreCase(PLUGIN_NAME)){
      transfers = startUpload(srcUrls, destUrls);
    }
    else if(srcUrls[0].getProtocol().equalsIgnoreCase(PLUGIN_NAME) &&
        destUrls[0].getProtocol().equalsIgnoreCase("file")){
      transfers = startDownload(srcUrls, destUrls);
    }
    else{
      throw new IOException("Only download or upload is supported by this plugin.");
    }
    String [] ids = new String[transfers.length];
    for(int i=0; i<transfers.length; ++i){
      ids[i] = getTransferID(transfers[i]);
    }
    return ids;
  }
  
  private String getTransferID(TransferInfo transfer) {
    return PLUGIN_NAME + "-copy::'" + transfer.getSource().getURL()+"' '"+transfer.getDestination().getURL()+"'";
  }
  
  private TransferInfo [] startUpload(GlobusURL[] srcUrls, final GlobusURL[] destUrls) throws IOException, S3ServiceException {
    final TransferInfo [] transfers = new TransferInfo[srcUrls.length];
    TransferInfo transfer = null;
    // Check that all are destined for the same bucket
    final S3Bucket bucket = getBucket(getBucketName(destUrls[0]), true);
    S3Bucket tmpbucket = null;
    for(int i=0; i<destUrls.length; ++i){
      tmpbucket = getBucket(getBucketName(destUrls[i]), false);
      if(tmpbucket==null || !tmpbucket.equals(bucket)){
        throw new IOException("All uploads must be to the same bucket. "+tmpbucket);
      }
    }
    // Construct Files
    final File [] uploadFiles = new File[srcUrls.length];
    for(int i=0; i<srcUrls.length; ++i){
      transfer = new TransferInfo(srcUrls[i], destUrls[i]);
      transfers[i] = transfer;
      uploadFiles[i] = new File(MyUtil.clearFile(srcUrls[i].getURL()));
    }
    ResThread t = new ResThread(){
      public void run(){
        uploadObjects(uploadFiles, destUrls, GridPilot.getClassMgr().getStatusBar(), bucket, transfers);
      }
    };
    t.start();
    return transfers;
  }
  
  private File getDownloadDir(GlobusURL srcUrl, GlobusURL destUrl) throws IOException{
    String path = MyUtil.clearFile(destUrl.getPath());
    File downloadFile = new File(path);
    File downloadDir;
    if(downloadFile.getAbsolutePath().endsWith("/") || downloadFile.isDirectory()){
      String srcName =srcUrl.getURL().replaceFirst("^.*/([^/]+)$", "$1");
      String destName =srcUrl.getURL().replaceFirst("^.*/([^/]+)$", "$1");
      if(srcName.equals(destName)){
        downloadDir = downloadFile;
      }
      else{
        downloadDir = downloadFile.getParentFile();
      }
    }
    else{
      downloadDir = downloadFile.getParentFile();
      if(!srcUrl.getPath().endsWith("/"+downloadFile.getName())){
        logFile.addInfo("WARNING: Download destination "+downloadFile.getAbsolutePath()+
            " has a different file name than the source "+srcUrl.getURL());
      }
    }
    if(!downloadDir.exists()){
      logFile.addInfo("Download directory "+downloadDir.getAbsolutePath()+" does not exist. Creating.");
      downloadDir.mkdirs();
    }
    return downloadDir;
  }

  private TransferInfo [] startDownload(GlobusURL [] srcUrls, GlobusURL [] destUrls) throws IOException, S3ServiceException{
    // The URLs are of the form sss://bucket/some/file/name
    // getHost() --> bucket, getPath() --> some/file/name
    
    // Map of the form dir -> {object1, object2, ...}
    HashMap<File, Vector<S3Object>> objectsMap = new HashMap<File, Vector<S3Object>> ();
    S3Object [] tmpObjects = null;
    S3Object [] dirContent = null;
    File dir;
    String dirName;
    HashMap<S3Object, TransferInfo> transferMap = new HashMap<S3Object, TransferInfo>();
    S3Bucket bucket;
    for(int i=0; i<srcUrls.length; ++i){
      dir = getDownloadDir(srcUrls[i], destUrls[i]);
      Debug.debug("Got download dir "+dir.getAbsolutePath()+" for "+destUrls[i].getURL(), 3);
      if(!objectsMap.containsKey(dir)){
        objectsMap.put(dir, new Vector<S3Object>());
      }
      bucket = getBucket(getBucketName(srcUrls[i]), false);
      // Check if we are dealing with a directory
      tmpObjects = s3Service.listObjects(bucket, srcUrls[i].getPath(), null);
      if(tmpObjects.length==1){
        // This is not a directory
        objectsMap.get(dir).add(tmpObjects[0]);
        transferMap.put(tmpObjects[0], new TransferInfo(srcUrls[i], destUrls[i]));
      }
      else{
        dirContent = s3Service.listObjects(bucket, srcUrls[i].getPath()+"/", null);
        if(dirContent.length>1){
          // This is a directory
          dirName = srcUrls[i].getURL().replaceFirst("^.*/([^/]+)$", "$1");
          dir = new File(dir, dirName);
          if(!objectsMap.containsKey(dir)){
            objectsMap.put(dir, new Vector<S3Object>());
          }
        }
      }
    }
    Vector<TransferInfo> allTransfers = new Vector<TransferInfo>();
    Debug.debug("Will download in "+objectsMap.keySet().size()+" batches", 2);
    for(Iterator<File> it=objectsMap.keySet().iterator(); it.hasNext();){
      final File dlDir = it.next();
      final S3Object [] objectsOnServer = objectsMap.get(dlDir).toArray(new S3Object[objectsMap.get(dlDir).size()]);
      final TransferInfo [] transfers = new TransferInfo[objectsOnServer.length];
      for(int i=0; i<objectsOnServer.length; ++i){
        transfers[i] = transferMap.get(objectsOnServer[i]);
        Debug.debug("Downloading "+transfers[i].getSourceURL()+"-->"+transfers[i].getDestination().getURL()+
            " to "+dlDir.getAbsolutePath(), 2);
        allTransfers.add(transfers[i]);
      }
      // We're assuming that all objects with the same destination directory also
      // come from the same bucket.
      // It should not be possible to avoid this with the GridPilot GUI...
      final S3Bucket finBucket = getBucket(getBucketName(transfers[0].getSource()), false);
      ResThread t = new ResThread(){
        public void run(){
          try{
            downloadObjects(objectsOnServer, GridPilot.getClassMgr().getStatusBar(), finBucket,
               transfers, dlDir);
          }
          catch(IOException e){
            logFile.addMessage("Problem downloading from S3.", e);
          }
        }
      };
      t.start();
    }
    return allTransfers.toArray(new TransferInfo[allTransfers.size()]);
  }
  
  public String getUserInfo() throws Exception{
    return accessKey;
  }

  public String getFullStatus(String fileTransferID) throws Exception{
    TransferInfo [] transfers = null;
    String ret = "";
    MyS3ServiceEventListener s3Listener = null;
    for(Iterator<TransferInfo []> it=s3ServiceEventListeners.keySet().iterator(); it.hasNext();){
      transfers = it.next();
      for(int i=0; i<transfers.length; ++i){
        if(getTransferID(transfers[i]).equals(fileTransferID)){
          s3Listener = ((MyS3ServiceEventListener) s3ServiceEventListeners.get(transfers));
          ret += "Status: "+s3Listener.getStatus();
          ret += "\nThreads: "+s3Listener.getThreadWatcher().getThreadCount();
          ret += "\nCompleted threads: "+s3Listener.getThreadWatcher().getCompletedThreads();
          ret += "\nBytes per second: "+s3Listener.getThreadWatcher().getBytesPerSecond();
          if(s3Listener.getThreadWatcher().isBytesTransferredInfoAvailable()){
            ret += "\nBytes total: "+s3Listener.getThreadWatcher().getBytesTotal();
          }
          if(s3Listener.getThreadWatcher().isBytesTransferredInfoAvailable()){
            ret += "\nBytes transferred: "+s3Listener.getThreadWatcher().getBytesTransferred();
          }
          if(s3Listener.getThreadWatcher().isTimeRemainingAvailable()){
            ret += "\nTime remaining: "+s3Listener.getThreadWatcher().getTimeRemaining();
          }
          return ret;
        }
      }
    }
    Debug.debug("WARNING: no status found for file transfer "+fileTransferID, 1);
    return "Status: "+STATUS_ERROR;
  }

  public String getStatus(String fileTransferID) throws Exception{
    TransferInfo [] ids = null;
    String status = null;
    for(Iterator it=s3ServiceEventListeners.keySet().iterator(); it.hasNext();){
      ids = (TransferInfo []) it.next();
      for(int i=0; i<ids.length; ++i){
        if(getTransferID(ids[i]).equals(fileTransferID)){
          status = ((MyS3ServiceEventListener) s3ServiceEventListeners.get(ids)).getStatus();
        }
      }
      if(status!=null){
        break;
      }
    }
    if(status==null){
      status = SSSFileTransfer.STATUS_DONE;
    }
    Debug.debug("Status of file transfer "+fileTransferID+": "+status, 1);
    return status;
  }

  public int getInternalStatus(String id, String ftStatus)
      throws Exception{
    Debug.debug("Mapping status "+ftStatus, 2);
    int ret = -1;
    if(ftStatus==null || ftStatus.equals("")){
      ret = FileTransfer.STATUS_ERROR;
    }
    else if(ftStatus==null || ftStatus.equalsIgnoreCase(STATUS_ERROR)){
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

  public long getFileBytes(GlobusURL _globusUrl) throws Exception{
    GlobusURL globusUrl = fixUrl(_globusUrl);
    if(globusUrl.getURL().endsWith("/")){
      if(S3FOX_DIRECTORY_MODE){
        globusUrl = new GlobusURL(globusUrl.getURL().replaceFirst("/$", "")+S3FOX_DIRECTORY_SUFFIX);
      }
    }
    S3Bucket bucket = getBucket(getBucketName(globusUrl), false);
    S3Object [] objects = s3Service.listObjects(bucket, globusUrl.getPath(), "/");
    if(objects==null || objects.length==0){
      Debug.debug("WARNING: object not found: "+globusUrl.getPath(), 1);
      return -1;
    }
    if(objects==null || objects.length>1){
      Debug.debug("WARNING: object ambiguous: "+globusUrl.getPath(), 1);
      return -1;
    }
    if(!globusUrl.getPath().equals(objects[0].getKey())){
      Debug.debug("WARNING: object not found: "+globusUrl.getPath(), 1);
      return -1;
    }
    Debug.debug("Size of object: "+objects[0]+"-->"+objects[0].getContentLength(), 3);
    return objects[0].getContentLength();
  }

  public long getBytesTransferred(String fileTransferID) throws Exception{
    TransferInfo [] ids = null;
    long ret = 0;
    MyS3ServiceEventListener s3Listener = null;
    for(Iterator it=s3ServiceEventListeners.keySet().iterator(); it.hasNext();){
      ids = (TransferInfo []) it.next();
      for(int i=0; i<ids.length; ++i){
        if(getTransferID(ids[i]).equals(fileTransferID)){
          s3Listener = ((MyS3ServiceEventListener) s3ServiceEventListeners.get(ids));
          if(s3Listener.getThreadWatcher().isBytesTransferredInfoAvailable()){
            ret = s3Listener.getThreadWatcher().getBytesTransferred();
            break;
          }
        }
      }
    }
    return ret;
  }

  public int getPercentComplete(String fileTransferID) throws Exception{
    TransferInfo [] ids = null;
    long transferredBytes = 0;
    long totalBytes = 0;
    MyS3ServiceEventListener s3Listener = null;
    for(Iterator it=s3ServiceEventListeners.keySet().iterator(); it.hasNext();){
      ids = (TransferInfo []) it.next();
      for(int i=0; i<ids.length; ++i){
        if(getTransferID(ids[i]).equals(fileTransferID)){
          s3Listener = ((MyS3ServiceEventListener) s3ServiceEventListeners.get(ids));
          if(s3Listener.getThreadWatcher().isBytesTransferredInfoAvailable()){
            transferredBytes = s3Listener.getThreadWatcher().getBytesTransferred();
            totalBytes = s3Listener.getThreadWatcher().getBytesTotal();
            break;
          }
        }
      }
    }
    return totalBytes==0 ? 0 : (int) (100*transferredBytes/totalBytes);
  }

  public void cancel(String fileTransferID) throws Exception{
    TransferInfo [] ids = null;
    MyS3ServiceEventListener s3Listener = null;
    for(Iterator it=s3ServiceEventListeners.keySet().iterator(); it.hasNext();){
      ids = (TransferInfo []) it.next();
      for(int i=0; i<ids.length; ++i){
        if(getTransferID(ids[i]).equals(fileTransferID)){
          s3Listener = ((MyS3ServiceEventListener) s3ServiceEventListeners.get(ids));
          s3Listener.getThreadWatcher().cancelTask();
          return;
        }
      }
    }
    Debug.debug("WARNING: transfer with id "+fileTransferID+" not found. Cannot cancel.", 1);
  }

  public void finalize(String fileTransferID) throws Exception{
    TransferInfo [] ids = null;
    String status = null;
    boolean allDone = true;
    for(Iterator it=s3ServiceEventListeners.keySet().iterator(); it.hasNext();){
      ids = (TransferInfo []) it.next();
      allDone = true;
      for(int i=0; i<ids.length; ++i){
        if(getTransferID(ids[i]).equals(fileTransferID)){
          status = ((MyS3ServiceEventListener) s3ServiceEventListeners.get(ids)).getStatus();
        }
        allDone = allDone && status!=null && (status.equals(SSSFileTransfer.STATUS_DONE) ||
            status.equals(SSSFileTransfer.STATUS_FAILED));
      }
      if(status!=null && ids!=null && allDone){
        Debug.debug("All done, removing listener for "+MyUtil.arrayToString(ids), 3);
        s3ServiceEventListeners.remove(ids);
        // Write cache information
        File destinationFile;
        for(int i=0; i<ids.length; ++i){
          destinationFile = new File(ids[i].getDestination().getPath());
          try{
            fileCacheMgr.writeCacheInfo(destinationFile);
          }
          catch(Exception e){
            logFile.addMessage("Could not write cache information for "+destinationFile);
          }
        }
      }
      if(status!=null){
        break;
      }
    }
  }

  public void deleteFiles(GlobusURL [] destUrls) throws Exception{
    GlobusURL globusUrl = null;
    for(int i=0; i<destUrls.length; ++i){
      globusUrl = fixUrl(destUrls[i]);
      // TODO: should we continue if one delete fails? - if so, do this in other plugins as well...
      deleteFileOrDir(globusUrl);
    }
    refreshMyBuckets();
  }
  
  private void deleteFileOrDir(GlobusURL globusUrl) throws Exception{
    String objectName;
    S3Bucket bucket;
    String deleteBucketName = null;
    // We cannot delete /
    if(globusUrl.getURL().matches("^"+PLUGIN_NAME+":/+$")){
      throw new IOException("Cannot delete top level.");
    }
    if(globusUrl.getURL().matches("^"+PLUGIN_NAME+":/+[^/]+/*$")){
      // If we are deleting a URL of the form sss://bucket, get bucket name directly
      objectName = null;
      deleteBucketName = globusUrl.getURL().replaceFirst("^"+PLUGIN_NAME+":/+([^/]+)/*$", "$1");
      bucket = getBucket(deleteBucketName, false);
      Debug.debug("Deleting bucket "+bucket.getName(), 1);
    }
    else{
      Debug.debug("Deleting file or directory "+globusUrl.getURL(), 1);
      // In case of a directory, get rid of the trailing /.
      objectName = globusUrl.getPath().replaceFirst("(.*)/$", "$1");
      bucket = getBucket(getBucketName(globusUrl), false);
    }
    // If we are deleting a URL of the form sss://bucket, delete the bucket
    if(deleteBucketName!=null){
      s3Service.deleteBucket(deleteBucketName);
    }
    // Otherwise, delete the objects from the directory
    else{
      S3Object [] objects = s3Service.listObjects(bucket, objectName, null);
      if(objects==null || objects.length==0){
        String error = "WARNING: object not found: "+objectName+". Backing out.";
        Debug.debug(error, 1);
        return;
      }
      if(objects.length>1){
        String error = "WARNING: directory not empty: "+objectName+": "+MyUtil.arrayToString(objects);
        Debug.debug(error, 1);
        //return;
      }
      for(int j=0; j<objects.length; ++j){
        Debug.debug("---> "+objects[j].getKey(), 2);
        if(objects[j].getKey().startsWith(objectName)){
          Debug.debug("Deleting", 2);
          s3Service.deleteObject(bucket, objects[j].getKey());
        }
      }
    }
  }

  /**
   * Quick and dirty method to just get a file - bypassing
   * caching, queueing and monitoring. Notice, that it does NOT
   * start a separate thread.
   */
  public void getFile(GlobusURL _globusUrl, File downloadDirOrFile,
      final StatusBar statusBar) throws Exception{
    
    final GlobusURL globusUrl = fixUrl(_globusUrl);
    
    if(globusUrl.getURL().endsWith("/")){
      throw new IOException("ERROR: cannot download a directory. ");
    }
    Debug.debug("Get "+globusUrl.getURL(), 3);
    Debug.debug("Getting "+globusUrl.getURL(), 3);
    if(statusBar!=null){
      ResThread t = new ResThread(){
        public void run(){
          statusBar.setLabel("Getting "+globusUrl.getURL());
        }
      };
      SwingUtilities.invokeLater(t);
    }
    File downloadFile = null;
    String fileName = globusUrl.getPath().replaceFirst(".*/([^/]+)", "$1");
    if(downloadDirOrFile.isDirectory()){
      downloadFile = new File(downloadDirOrFile.getAbsolutePath(), fileName);
    }
    else{
      downloadFile = downloadDirOrFile;
    }
    final File dlFile = downloadFile;

    final S3Bucket bucket = getBucket(getBucketName(globusUrl), false);
    S3Object [] objects = s3Service.listObjects(bucket, globusUrl.getPath(), null);
    if(objects==null || objects.length==0){
      String error = "WARNING: object not found: "+globusUrl.getPath()+". Backing out.";
      Debug.debug(error, 1);
      return;
    }
    if(objects==null || objects.length>1){
      String error = "WARNING: object ambiguous: "+globusUrl.getPath()+". Backing out.";
      Debug.debug(error, 1);
      return;
    }
    final String objectKey = objects[0].getKey();
    final TransferInfo transfer = new TransferInfo(globusUrl,
        new GlobusURL("file:///"+downloadDirOrFile.getAbsolutePath()));
    fileTransfers.add(transfer);
    Debug.debug("Downloading "+globusUrl.getURL()+"->"+downloadFile.getAbsolutePath(), 3);
    ResThread t = new ResThread(){
      public void run(){
        try{
          S3Object s3Object = s3Service.getObject(bucket, objectKey);
          InputStream in = s3Object.getDataInputStream();
          byte[] buf = new byte[1024];
          int len;
          OutputStream out = new FileOutputStream(dlFile); 
          while((len = in.read(buf))>0 && fileTransfers.contains(transfer)){
            out.write(buf, 0, len);
          }
          in.close();
          out.close();
        }
        catch(Exception e){
          this.setException(e);
          e.printStackTrace();
        }
      }
    };
    t.start();
    if(!MyUtil.myWaitForThread(t, PLUGIN_NAME, COPY_TIMEOUT, "getFile", new Boolean(true))){
      throw new IOException("Download taking too long (>"+COPY_TIMEOUT+" ms). Cancelling.");
    }
    if(t.getException()!=null){
      if(statusBar!=null){
        statusBar.setLabel("Download failed");
      }
      throw t.getException();
    }
    // if we didn't get an exception, the file got downloaded
    if(statusBar!=null){
      statusBar.setLabel("Download done");
    }
    Debug.debug(globusUrl.getURL()+" downloaded.", 2);
  }

  private void put(GlobusURL _fileUrl, final InputStream is, final StatusBar statusBar)
      throws Exception{
    
    GlobusURL fileUrl = _fileUrl;
    
    if(fileUrl.getURL().matches("^"+PLUGIN_NAME+":/+[^/]+/*$")){
      String bucketName = fileUrl.getURL().replaceFirst("^"+PLUGIN_NAME+":/+([^/]+)/*$", "$1");
      getBucket(bucketName, true);
      return;
    }
    
    if(S3FOX_DIRECTORY_MODE && fileUrl.getPath().endsWith("/")){
      String urlStr = fileUrl.getURL();
      urlStr = urlStr.substring(0, urlStr.length()-1)+S3FOX_DIRECTORY_SUFFIX;
      fileUrl = new GlobusURL(urlStr);
    }
    
    final GlobusURL globusFileUrl = fileUrl;
    
    (new ResThread(){
      public void run(){
        if(statusBar!=null){
          statusBar.setLabel("Uploading "+globusFileUrl.getURL());
        }
      }
    }).run();               
 
    final S3Bucket bucket = getBucket(getBucketName(globusFileUrl), true);
    
    ResThread t = new ResThread(){
      public void run(){
        // Notice: abortTransfer will not work here; there's no stream writing to interrupt...
        try{
          String path = null;
          if(globusFileUrl.getPath()==null){
            path = "/";
          }
          else{
            path = globusFileUrl.getPath();
          }
          Debug.debug("Uploading object to "+path, 2);
          S3Object isObject = new S3Object(bucket, path);
          if(globusFileUrl.getURL().endsWith("/")){
            isObject.setContentType(Mimetypes.MIMETYPE_JETS3T_DIRECTORY);
          }
          isObject.setDataInputStream(is);
          isObject.setContentLength(is.available());
          s3Service.putObject(bucket, isObject);
        }
        catch(Exception e){
          this.setException(e);
        }
      }
    };
    t.start();
    if(!MyUtil.myWaitForThread(t, PLUGIN_NAME, COPY_TIMEOUT, "putFile")){
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

  public void abortTransfer(String id) throws IOException{
    fileTransfers.remove(id);
  }

  /**
   * Quick and dirty method to just get a file - bypassing
   * caching, queueing and monitoring. Notice, that it does NOT
   * start a separate thread.
   */
  public void putFile(final File file, final GlobusURL globusFileUrl, final StatusBar statusBar)
  throws Exception {
    String fileName = file.getName();
    GlobusURL uploadUrl = null;
    if(globusFileUrl.getURL().endsWith("/")){
      uploadUrl = new GlobusURL(globusFileUrl.getURL()+fileName);
    }
    else{
      uploadUrl = globusFileUrl;
    }
    uploadUrl = fixUrl(uploadUrl);
    final GlobusURL upUrl = uploadUrl;
    Debug.debug("put "+file.getAbsolutePath()+" --> "+uploadUrl.getURL(), 3);

    FileInputStream fileIS = new FileInputStream(file);
    put(upUrl, fileIS, statusBar);
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

  public void write(GlobusURL _globusUrl, String text) throws Exception{
    final GlobusURL globusUrl = fixUrl(_globusUrl);
    ByteArrayInputStream textIS = new ByteArrayInputStream(text.getBytes());
    put(globusUrl, textIS, GridPilot.getClassMgr().getStatusBar());
  }
  
  // In order to use URL.getHost(), the URL needs strictly to be in the form sss://bucket/some/file
  private GlobusURL fixUrl(GlobusURL globusUrl) throws MalformedURLException{
    return new GlobusURL(globusUrl.getURL().replaceFirst("^"+PLUGIN_NAME+":/+", PLUGIN_NAME+"://"));
  }
  
  private GlobusURL[] fixUrls(GlobusURL[] _globusUrls) throws MalformedURLException{
    GlobusURL [] globusUrls = new GlobusURL[_globusUrls.length];
    for(int i=0; i<globusUrls.length; ++i){
      globusUrls[i] = fixUrl(_globusUrls[i]);
    }
    return globusUrls;
  }
  
  private String getBucketName(GlobusURL globusUrl) throws MalformedURLException{
    return fixUrl(globusUrl).getHost();
  }

  private Vector list(GlobusURL globusUrl, String filter, boolean recurse, StatusBar statusBar)
      throws Exception{
    
    globusUrl = fixUrl(globusUrl);
    
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
    
    Debug.debug("Filtering with "+filter, 3);
    
    // The URLs are of the form sss://bucket/some/file/name
    // getHost() --> bucket, getPath() --> some/file/name
    
    S3Bucket bucket = null;
    
    // If no bucket is given we list buckets
    if(globusUrl.getURL().matches("^"+PLUGIN_NAME+":/+$")){
      Debug.debug("Top level: "+globusUrl.getURL()+" listing buckets", 2);
      return listBuckets(globusUrl, filter, statusBar);
    }
    // If a bucket is given we list its content
    else{
      bucket = getBucket(getBucketName(globusUrl), false);
      Debug.debug("Listing bucket content of "+globusUrl.getURL()+"<--"+globusUrl.getHost(), 2);
      return listBucketContent(bucket, globusUrl, filter, recurse, statusBar);
    }
  }
  
  private Vector listBuckets(GlobusURL globusUrl, String filter, StatusBar statusBar) throws S3ServiceException {
    refreshMyBuckets();
    Vector resVec = new Vector();
    int directories = 0;
    int files = 0;
    String fName = null;
    String type = null;
    S3Bucket myBucket = null;
    for(Iterator it=myBuckets.keySet().iterator(); it.hasNext();){
      myBucket = myBuckets.get(it.next());
      Debug.debug("myBucket: " + myBucket.getName(), 3);
      type = Mimetypes.MIMETYPE_JETS3T_DIRECTORY;
      fName = myBucket.getName();
      fName = fName.replaceFirst("^"+globusUrl.getPath(), "");
      if(type==null ||
          type!=null && type.equals(Mimetypes.MIMETYPE_JETS3T_DIRECTORY) ||
          fName.endsWith("/") ||
          /*this is the convention of the S3 Organizer Firefox plugin (S3Fox)*/
          S3FOX_DIRECTORY_MODE && fName.endsWith(S3FOX_DIRECTORY_SUFFIX)){
        if(S3FOX_DIRECTORY_MODE && fName.endsWith(S3FOX_DIRECTORY_SUFFIX)){
          fName = fName.substring(0, fName.length()-9);
        }
        if(!fName.endsWith("/")){
          fName += "/";
        }
      }
      if(!MyUtil.filterMatches(fName, filter)){
        continue;
      }
      if(fName.endsWith("/")){
        ++directories;
      }
      else{
        ++files;
      }
      resVec.add(fName + " " + "0"/*bytes*/);
    }
    if(statusBar!=null){
      statusBar.setLabel(directories+" directories");
    }
    return resVec;
  }

  private Vector listBucketContent(S3Bucket bucket, GlobusURL globusUrl, String filter,
      boolean recurse, StatusBar statusBar) throws S3ServiceException{
    Vector resVec = new Vector();
    String path = globusUrl.getPath()==null?"":globusUrl.getPath();
    Debug.debug("Listing path "+path, 2);
    s3Objects = s3Service.listObjects(bucket, path, recurse?null:(S3FOX_DIRECTORY_MODE?"/":""));
    Debug.debug("Number of objects: "+s3Objects.length, 3);
    //retrieveObjectsDetails(existingObjects, bucket);
    int directories = 0;
    int files = 0;
    String fName = null;
    String type = null;
    S3Object objectDetailsOnly = null;
    for(int i=0; i<s3Objects.length; i++){
      // TODO: find out why this doesn't work.
      //type = existingObjects[i].getContentType();
      //Debug.debug("Listing "+existingObjects[i].getKey()+" : "+type, 3);
      objectDetailsOnly = s3Service.getObjectDetails(bucket, s3Objects[i].getKey());
      Debug.debug("S3Object, details only: " + objectDetailsOnly, 3);
      type = objectDetailsOnly.getContentType();
      fName = s3Objects[i].getKey();
      fName = fName.replaceFirst("^"+path, "");
      if(type==null ||
          type!=null && type.equals(Mimetypes.MIMETYPE_JETS3T_DIRECTORY) ||
          fName.endsWith("/") ||
          /*this is the convention of the S3 Organizer Firefox plugin (S3Fox)*/
          S3FOX_DIRECTORY_MODE && fName.endsWith(S3FOX_DIRECTORY_SUFFIX)){
        if(S3FOX_DIRECTORY_MODE && fName.endsWith(S3FOX_DIRECTORY_SUFFIX)){
          fName = fName.substring(0, fName.length()-9);
        }
        if(!fName.endsWith("/")){
          fName += "/";
        }
      }
      if(!MyUtil.filterMatches(fName, filter)){
        continue;
      }
      if(fName.endsWith("/")){
        ++directories;
      }
      else{
        ++files;
      }
      resVec.add(fName + " " + s3Objects[i].getContentLength()/*bytes*/);
    }
    if(statusBar!=null){
      statusBar.setLabel(directories+" directories, "+files+" files");
    }
    return resVec;
  }
  
  private void uploadObjects(File[] uploadFiles, GlobusURL[] destinations,
      StatusBar statusBar, final S3Bucket bucket, TransferInfo [] ids){
    
    FileComparer fc = new FileComparer(s3Service.getJetS3tProperties());
    
    // Build map of files proposed for upload.
    //filesForUploadMap = fc.buildFileMap(uploadFiles, true);
    filesForUploadMap = new HashMap<String, File>();
    for(int i=0; i<destinations.length; ++i){
      filesForUploadMap.put(destinations[i].getPath(), uploadFiles[i]);
    }
    if(Debug.DEBUG_LEVEL>2){
      String key;
      for(Iterator<String>it=filesForUploadMap.keySet().iterator(); it.hasNext();){
        key = it.next();
        Debug.debug("File for upload: "+filesForUploadMap.get(key)+"-->"+key,3);
      }
    }
                
    // Build map of objects already existing in target S3 bucket with keys
    // matching the proposed upload keys.
    List objectsWithExistingKeys = new ArrayList();
    for(int i=0; i<s3Objects.length; i++) {
      if(filesForUploadMap.keySet().contains(s3Objects[i].getKey())){
        Debug.debug("Object already exists: "+s3Objects[i].getKey(), 3);
        objectsWithExistingKeys.add(s3Objects[i]);
      }
    }
    s3Objects = (S3Object[]) objectsWithExistingKeys.toArray(
        new S3Object[objectsWithExistingKeys.size()]);
    s3ExistingObjectsMap = fc.populateS3ObjectMap("", s3Objects);
    if(s3Objects.length>0){
      // Retrieve details of potential clashes.
      final S3Object[] clashingObjects = s3Objects;
      (new Thread(){
        public void run(){
          retrieveObjectsDetails(clashingObjects, bucket);
        }
      }).start();
    }
    else{
      compareAndCopy(filesForUploadMap, s3ExistingObjectsMap, true,
         statusBar, null, bucket, ids);
    }
  }

  private void downloadObjects(S3Object [] s3Objects, StatusBar statusBar,
      final S3Bucket bucket, final TransferInfo [] ids, File downloadDirectory) throws IOException{
    
    Debug.debug("Preparing for download in "+downloadDirectory.getAbsolutePath(), 3);
    
    FileComparer fc = new FileComparer(s3Service.getJetS3tProperties());

    // Build map of existing local files.
    Map filesInDownloadDirectoryMap = fc.buildFileMap(downloadDirectory, null, true);
    filesAlreadyInDownloadDirectoryMap = new HashMap();
    
    // Build map of S3 Objects being downloaded. 
    s3DownloadObjectsMap = fc.populateS3ObjectMap("", s3Objects);

    // Identify objects that may clash with existing files, or may be directories,
    // and retrieve details for these.
    ArrayList potentialClashingObjects = new ArrayList();
    Set existingFilesObjectKeys = filesInDownloadDirectoryMap.keySet();
    Iterator objectsIter = s3DownloadObjectsMap.entrySet().iterator();
    while(objectsIter.hasNext()){
      Map.Entry entry = (Map.Entry) objectsIter.next();
      String objectKey = (String) entry.getKey();
      S3Object object = (S3Object) entry.getValue();
      
      if(object.getContentLength()==0 || existingFilesObjectKeys.contains(objectKey)){
        potentialClashingObjects.add(object);
      }
      if(existingFilesObjectKeys.contains(objectKey)){
        filesAlreadyInDownloadDirectoryMap.put(
            objectKey, filesInDownloadDirectoryMap.get(objectKey));
      }
    }
    
    if(filesAlreadyInDownloadDirectoryMap.size()>0/*potentialClashingObjects.size()>0*/){
      logFile.addInfo("WARNING: overwriting "+
          MyUtil.arrayToString(filesAlreadyInDownloadDirectoryMap.keySet().toArray())+" in "+downloadDirectory);
    }
    /*if(potentialClashingObjects.size()>0){
      // Retrieve details of potential clashes.
      final S3Object[] clashingObjects = (S3Object[])
          potentialClashingObjects.toArray(new S3Object[potentialClashingObjects.size()]);
      (new Thread(){
        public void run(){
          retrieveObjectsDetails(clashingObjects, bucket);
        }
      }).start();
    }
    else{*/
      Debug.debug("Comparing remote and local files", 3);
      compareAndCopy(filesAlreadyInDownloadDirectoryMap, s3DownloadObjectsMap,
         false, statusBar, downloadDirectory, bucket, ids);
    //}
  }
  
  private void performFilesUpload(FileComparerResults comparisonResults,
      Map uploadingFilesMap, final StatusBar statusBar, final S3Bucket bucket, TransferInfo [] ids) throws Exception {
    // Determine which files to upload, prompting user whether to over-write existing files
    List fileKeysForUpload = new ArrayList();
    fileKeysForUpload.addAll(comparisonResults.onlyOnClientKeys);
    int newFiles = comparisonResults.onlyOnClientKeys.size();
    int unchangedFiles = comparisonResults.alreadySynchronisedKeys.size();
    int changedFiles = comparisonResults.updatedOnClientKeys.size() 
        + comparisonResults.updatedOnServerKeys.size();
      if(unchangedFiles>0 || changedFiles>0){
          logFile.addMessage("Files for upload clash with existing S3 objects");
          String message = "Of the " + uploadingFilesMap.size() 
              + " file(s) being uploaded:\n\n";        
          if(newFiles>0){
              message += newFiles + " file(s) are new.\n\n";
          }
          if(changedFiles>0){
              message += changedFiles + " file(s) have changed.\n\n";
          }
          if(unchangedFiles>0){
              message += unchangedFiles + " file(s) already exist and are unchanged.\n\n";
          }
          message += "Click \"OK\" to proceed or \"Cancel\" to cancel.";
          ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
          int response = confirmBox.getOkCancelConfirm("File(s) already exist!", message,
              GridPilot.ICONS_PATH + "ok.png", GridPilot.ICONS_PATH + "cancel.png");
          if(response!=1){
              return;
          }
          fileKeysForUpload.addAll(comparisonResults.updatedOnClientKeys);
          fileKeysForUpload.addAll(comparisonResults.updatedOnServerKeys);
          fileKeysForUpload.addAll(comparisonResults.alreadySynchronisedKeys);

      }

      if(fileKeysForUpload.size()==0){
        return;
      }
      final JProgressBar pb = statusBar.createJProgressBar(0, fileKeysForUpload.size());
      if(statusBar!=null){
        statusBar.setLabel("Prepared 0 of " + fileKeysForUpload.size() 
            + " file(s) for upload");
        statusBar.setProgressBar(pb);
      }
      // Populate S3Objects representing upload files with metadata etc.
      final S3Object[] objects = new S3Object[fileKeysForUpload.size()];
      int objectIndex = 0;
      for(Iterator iter = fileKeysForUpload.iterator(); iter.hasNext();){
        String fileKey = iter.next().toString();
        File file = (File) uploadingFilesMap.get(fileKey);
        objects[objectIndex++] = createS3Object(file, fileKey);
        statusBar.setProgressBarValue(pb, objectIndex+1);
        statusBar.setLabel("Prepared " + (objectIndex + 1) 
            + " of " + fileKeysForUpload.size() + " file(s) for upload");
      }
      statusBar.removeProgressBar(pb);
      statusBar.setLabel("");      
      S3ServiceEventListener s3Listener = new MyS3ServiceEventListener();
      final S3ServiceMulti s3ServiceMulti = new S3ServiceMulti(s3Service, s3Listener);
      s3ServiceEventListeners.put(ids, (MyS3ServiceEventListener) s3Listener);
      // Upload the files.
      Runnable r = new Runnable(){
        public void run(){
          Debug.debug("Putting object(s): "+MyUtil.arrayToString(objects), 2);
          s3ServiceMulti.putObjects(bucket, objects);
        }
      };
      r.run();
  }

  private S3Object createS3Object(File file, String fileKey) throws Exception {
    S3Object newObject = new S3Object(fileKey);
    if(file.isDirectory()){   
      newObject.setContentType(Mimetypes.MIMETYPE_JETS3T_DIRECTORY);
      /* This is to be compatible with the S3 Organizer Firefox plugin (S3Fox).
      The plugin does not use setContentType but instead the hack
      of appending "_$folder$" to the name in order to tag something
      as a directory. */
      if(S3FOX_DIRECTORY_MODE){
        newObject = new S3Object(fileKey+S3FOX_DIRECTORY_SUFFIX);
      }
    }
    else{     
      newObject.setContentType(Mimetypes.getInstance().getMimetype(file));
      // Do any necessary file pre-processing.
      File fileToUpload = prepareUploadFile(file, newObject);
      newObject.addMetadata(Constants.METADATA_JETS3T_LOCAL_FILE_DATE, 
          ServiceUtils.formatIso8601Date(new Date(file.lastModified())));
      newObject.setContentLength(fileToUpload.length());
      newObject.setDataInputFile(fileToUpload);
      // Compute the upload file's MD5 hash.
      newObject.setMd5Hash(ServiceUtils.computeMD5Hash(
          new FileInputStream(fileToUpload)));
      if(!fileToUpload.equals(file)){
        // Compute the MD5 hash of the *original* file, if upload file has been altered
        // through encryption or gzipping.
        newObject.addMetadata(
            S3Object.METADATA_HEADER_ORIGINAL_HASH_MD5,
            ServiceUtils.toBase64(ServiceUtils.computeMD5Hash(new FileInputStream(file))));
      }
    }
    if(!filesWorldReadable){
      newObject.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
    }
    return newObject;
  }

  private void performObjectsDownload(FileComparerResults comparisonResults,
      Map s3DownloadObjectsMap, File downloadDirectory, final S3Bucket bucket, TransferInfo [] transfers) throws Exception {        
    // Determine which files to download, prompting user whether to over-write existing files
    List objectKeysForDownload = new ArrayList();
    objectKeysForDownload.addAll(comparisonResults.onlyOnServerKeys);
    int newFiles = comparisonResults.onlyOnServerKeys.size();
    int unchangedFiles = comparisonResults.alreadySynchronisedKeys.size();
    int changedFiles = comparisonResults.updatedOnClientKeys.size() 
       + comparisonResults.updatedOnServerKeys.size();
    if(unchangedFiles>0 || changedFiles>0){
      logFile.addMessage("Files for download clash with existing local files");
      String message = "Of the " + (newFiles + unchangedFiles + changedFiles) 
         + " file(s) being downloaded:\n\n";
      if(newFiles>0){
        message += newFiles + " file(s) are new.\n\n";
      }
      if(changedFiles>0){
        message += changedFiles + " file(s) have changed.\n\n";
      }
      if(unchangedFiles>0){
        message += unchangedFiles + " file(s) already exist and are unchanged.\n\n";
      }
      message += "Click \"OK\" to proceed or \"Cancel\" to cancel.";
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
      int response = confirmBox.getOkCancelConfirm("File(s) already exist!", message,
          GridPilot.ICONS_PATH + "ok.png", GridPilot.ICONS_PATH + "cancel.png");
      if(response!=1){
        return;
      }
      objectKeysForDownload.addAll(comparisonResults.updatedOnClientKeys);
      objectKeysForDownload.addAll(comparisonResults.updatedOnServerKeys);
      objectKeysForDownload.addAll(comparisonResults.alreadySynchronisedKeys);    
    }
    Debug.debug("Downloading " + objectKeysForDownload.size() + " object(s) to "+downloadDirectory.getAbsolutePath(), 2);
    if(objectKeysForDownload.size()==0){
      return;
    }
    // Create array of objects for download.        
    S3Object[] objects = new S3Object[objectKeysForDownload.size()];
    int objectIndex = 0;
    for(Iterator iter = objectKeysForDownload.iterator(); iter.hasNext();){
      objects[objectIndex++] = (S3Object) s3DownloadObjectsMap.get(iter.next()); 
    }
    HashMap<String, File> downloadObjectsToFileMap = new HashMap<String, File>();
    ArrayList<DownloadPackage> downloadPackageList = new ArrayList<DownloadPackage>();
    DownloadPackage downloadPackage;
    // Setup files to write to, creating parent directories when necessary.
    for(int i=0; i<objects.length; i++){
      downloadPackage = createDownloadPackage(downloadDirectory, objects[i], downloadObjectsToFileMap);
      if(downloadPackage!=null){
        downloadPackageList.add(downloadPackage);            
      }
    }
    // Download the files.
    final DownloadPackage[] downloadPackagesArray = 
        downloadPackageList.toArray(new DownloadPackage[downloadPackageList.size()]);            
    S3ServiceEventListener s3Listener = new MyS3ServiceEventListener();
    s3ServiceEventListeners.put(transfers, (MyS3ServiceEventListener) s3Listener);
    final S3ServiceMulti s3ServiceMulti = new S3ServiceMulti(s3Service, s3Listener);
    Runnable r = new Runnable(){
      public void run(){
        try{
          s3ServiceMulti.downloadObjects(bucket, downloadPackagesArray);
        }
        catch(S3ServiceException e){
          e.printStackTrace();
          logFile.addMessage("ERROR: could not download object(s) "+Util.arrayToString(downloadPackagesArray), e);
          return;
        }
      }
    };
    r.run();
  }
  
  private DownloadPackage createDownloadPackage(File downloadDirectory, S3Object object, Map downloadObjectsToFileMap) throws S3ServiceException, InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeySpecException, UnsupportedEncodingException {
    File destFile = new File(downloadDirectory,
        /*use this to download misc/file.txt to dldir/misc/file.txt*/
        //object.getKey()
        /*use this to download misc/file.txt to dldir/file.txt*/
        object.getKey().replaceFirst("^.*/([^/]+)$", "$1")
        );
    try{
      long srcSize = object.getContentLength();
      Date srcLastModified = object.getLastModifiedDate();
      if(fileCacheMgr.checkCache(destFile, srcSize, srcLastModified)){
        return null;
      }
    }
    catch(Exception e){
      logFile.addMessage("WARNING: problem checking cache. ", e);
    }
    // Create directory corresponding to object, or parent directories of object.
    if(Mimetypes.MIMETYPE_JETS3T_DIRECTORY.equals(object.getContentType())) {
      destFile.mkdirs();
      // No further data to download for directories...
      return null;
    }
    else{
      if(destFile.getParentFile()!=null){
        destFile.getParentFile().mkdirs();
      }
    }
    downloadObjectsToFileMap.put(object.getKey(), destFile);
    boolean isZipped = false;
    EncryptionUtil encryptionUtil = null;
    if("gzip".equalsIgnoreCase(object.getContentEncoding())
        || object.containsMetadata(Constants.METADATA_JETS3T_COMPRESSED)){
      // Automatically inflate gzipped data.
      isZipped = true;
    }
    if(object.containsMetadata(Constants.METADATA_JETS3T_CRYPTO_ALGORITHM) ||
        object.containsMetadata(Constants.METADATA_JETS3T_ENCRYPTED_OBSOLETE)){
      Debug.debug("Decrypting encrypted data for object: " + object.getKey(), 2);
      // Prompt user for the password, if necessary.
      if(encryptionPassword==null || encryptionPassword.equalsIgnoreCase("")){
        throw new S3ServiceException(
            "One or more objects are encrypted. GridPilot cannot download encrypted "
            + "objects unless the encyption password is set in Preferences");
      }
      if(object.containsMetadata(Constants.METADATA_JETS3T_ENCRYPTED_OBSOLETE)) {
        // Item is encrypted with obsolete crypto.
        logFile.addMessage("WARNING: Object is encrypted with out-dated crypto version, please update it when possible: " 
            + object.getKey());
        encryptionUtil = EncryptionUtil.getObsoleteEncryptionUtil(encryptionPassword);                                            
      }
      else{
        String algorithm = (String) object.getMetadata(
            Constants.METADATA_JETS3T_CRYPTO_ALGORITHM);
        String version = (String) object.getMetadata(
            Constants.METADATA_JETS3T_CRYPTO_VERSION);
        if (version == null) {
            version = EncryptionUtil.DEFAULT_VERSION;
        }
        encryptionUtil = new EncryptionUtil(encryptionPassword, algorithm, version);                                            
      }                    
    }
    return new DownloadPackage(object, destFile, isZipped, encryptionUtil);
  }

  private void compareAndCopy(final Map localFilesMap, final Map s3ObjectsMap,
      final boolean upload, final StatusBar statusBar, File downloadDirectory, S3Bucket bucket,
      TransferInfo [] ids){
    final JProgressBar pb = statusBar.createJProgressBar(0, 100);
    try{
      // Compare objects being downloaded and existing local files.
      final String statusText =
          "Comparing " + s3ObjectsMap.size() + " object" + (s3ObjectsMap.size() > 1 ? "s" : "") +
          " in S3 with " + localFilesMap.size() + " local file" + (localFilesMap.size() > 1 ? "s" : "");
      if(statusBar!=null){
        statusBar.setLabel(statusText);
        statusBar.setProgressBar(pb);
      }
      
      // Calculate total files size.
      File[] files = (File[]) localFilesMap.values().toArray(new File[localFilesMap.size()]);
      final long filesSizeTotal[] = new long[1];
      for(int i = 0; i < files.length; i++){
        filesSizeTotal[0] += files[i].length();
      }
      
      // Monitor generation of MD5 hash, and provide feedback via the progress bar. 
      final long hashedBytesTotal[] = new long[1];
      hashedBytesTotal[0] = 0;
      final BytesProgressWatcher hashWatcher = new BytesProgressWatcher(filesSizeTotal[0]) {
        public void updateBytesTransferred(long transferredBytes) {
          hashedBytesTotal[0] += transferredBytes;
          final int percentage =  (int) (100 * hashedBytesTotal[0] / filesSizeTotal[0]);
          statusBar.setProgressBarValue(pb, percentage);
        }
      };
      
      FileComparer fc = new FileComparer(s3Service.getJetS3tProperties());
      
      FileComparerResults comparisonResults = 
        fc.buildDiscrepancyLists(localFilesMap, s3ObjectsMap, hashWatcher);
      
      statusBar.removeProgressBar(pb);
      statusBar.setLabel("");
      
      if(upload){
        performFilesUpload(comparisonResults, localFilesMap, statusBar, bucket, ids);
      }
      else{
        performObjectsDownload(comparisonResults, s3ObjectsMap, downloadDirectory, bucket, ids);                
      }
    }
    catch(RuntimeException e){
      throw e;
    }
    catch(Exception e){
      statusBar.removeProgressBar(pb); 
      String message = "Unable to download objects";
      logFile.addMessage(message, e);
      statusBar.setLabel(message);
    }
  }
  
  /**
   * Prepares to upload files to S3 
   * @param originalFile
   * @param newObject
   * @return
   * @throws Exception
   */
  private File prepareUploadFile(final File originalFile, final S3Object newObject) throws Exception {        
      if(!compressUploads && !encryptUploads){
        // No file pre-processing required.
        return originalFile;
      }
      
      String actionText = "";
      
      // File must be pre-processed. Process data from original file 
      // and write it to a temporary one ready for upload.
      final File tempUploadFile = File.createTempFile("GridPilot-",".tmp");
      tempUploadFile.deleteOnExit();
      
      OutputStream outputStream = null;
      InputStream inputStream = null;
      
      try{
        outputStream = new BufferedOutputStream(new FileOutputStream(tempUploadFile));
        inputStream = new BufferedInputStream(new FileInputStream(originalFile));

        String contentEncoding = null;
        if(compressUploads){
          inputStream = new GZipDeflatingInputStream(inputStream);
          contentEncoding = "gzip";
          newObject.addMetadata(Constants.METADATA_JETS3T_COMPRESSED, "gzip"); 
          actionText += "Compressing";                
        } 
        if(encryptUploads){
          String algorithm = Jets3tProperties.getInstance(Constants.JETS3T_PROPERTIES_FILENAME)
          .getStringProperty("crypto.algorithm", "PBEWithMD5AndDES");                
          EncryptionUtil encryptionUtil = new EncryptionUtil(
              encryptionPassword, algorithm, EncryptionUtil.DEFAULT_VERSION);
          inputStream = encryptionUtil.encrypt(inputStream);
          contentEncoding = null;
          newObject.setContentType(Mimetypes.MIMETYPE_OCTET_STREAM);
          newObject.addMetadata(Constants.METADATA_JETS3T_CRYPTO_ALGORITHM, 
              encryptionUtil.getAlgorithm()); 
          newObject.addMetadata(Constants.METADATA_JETS3T_CRYPTO_VERSION, 
              EncryptionUtil.DEFAULT_VERSION); 
          actionText += (actionText.length() == 0? "Encrypting" : " and encrypting");                
        }
        if(contentEncoding!=null){
          newObject.addMetadata("Content-Encoding", contentEncoding);
        }

        Debug.debug("Re-writing file data for '" + originalFile + "' to temporary file '" 
            + tempUploadFile.getAbsolutePath() + "': " + actionText, 2);
        
        byte[] buffer = new byte[4096];
        int c = -1;
        while((c = inputStream.read(buffer))>=0){
            outputStream.write(buffer, 0, c);
        }
      }
      finally {
        if(inputStream!=null){
          inputStream.close();
        }
        if(outputStream!=null){
            outputStream.close();
        }      
      }
      
      return tempUploadFile;
  }

  private void retrieveObjectsDetails(final S3Object[] candidateObjects, final S3Bucket bucket) {
    // Identify which of the candidate objects have incomplete metadata.
    ArrayList s3ObjectsIncompleteList = new ArrayList();
    for(int i=0; i<candidateObjects.length; i++){
      if(!candidateObjects[i].isMetadataComplete()){
        s3ObjectsIncompleteList.add(candidateObjects[i]);
      }
    }
    
    Debug.debug("Of " + candidateObjects.length + " object candidates for HEAD requests "
        + s3ObjectsIncompleteList.size() + " are incomplete, performing requests for these only", 1);
    
    final S3Object[] incompleteObjects = (S3Object[]) s3ObjectsIncompleteList.toArray(
        new S3Object[s3ObjectsIncompleteList.size()]);        
    ResThread t = (new ResThread(){
      public void run(){
        //S3ServiceEventListener s3Listener = new MyS3ServiceEventListener();
        //S3ServiceMulti s3ServiceMulti = new S3ServiceMulti(s3Service, s3Listener);
        s3ServiceMulti.getObjectsHeads(bucket, incompleteObjects);
      };
    });
    t.start();
    //Util.waitForThread(t, "SSSFileTransfer", 20000, "retrieveObjectsDetails", false);
  }

  /**
   * Implementation method for the CredentialsProvider interface.
   * <p>
   * Based on sample code:  
   * <a href="http://svn.apache.org/viewvc/jakarta/commons/proper/httpclient/trunk/src/examples/InteractiveAuthenticationExample.java?view=markup">InteractiveAuthenticationExample</a> 
   * 
   */
  public Credentials getCredentials(AuthScheme authscheme, String host, int port, boolean proxy) throws CredentialsNotAvailableException {
    if(authscheme==null){
      return null;
    }
    try{
      Credentials credentials = null;  
      if(authscheme instanceof NTLMScheme){
        String [] up = GridPilot.userPwd("Authentication Required.\n\n"+ 
            "<html>Host <b>" + host + ":" + port + "</b> requires Windows authentication</html>",
            new String [] {"User", "Password", "Host", "Domain"},
            new String [] {"", "", host, ""});
        credentials = new NTCredentials(up[0], up[1], up[2], up[3]);
      }
      else if(authscheme instanceof RFC2617Scheme){
          String [] up = GridPilot.userPwd("Authentication Required.\n\n"+ 
              "<html><center>Host <b>" + host + ":" + port + "</b>" 
              + " requires authentication for the realm:<br><b>" + authscheme.getRealm() + "</b></center></html>",
              new String [] {"User", "Password"},
              new String [] {"", ""});
          credentials = new UsernamePasswordCredentials(up[0], up[1]);
      }
      else{
          throw new CredentialsNotAvailableException("Unsupported authentication scheme: " +
              authscheme.getSchemeName());
      }
      return credentials;     
    }
    catch(Exception e){
      e.printStackTrace();
      throw new CredentialsNotAvailableException(e.getMessage(), e);
    }
  }

  public void getFile(GlobusURL arg0, File arg1) throws Exception {
    getFile(arg0, arg1, null);
  }

  public Vector list(GlobusURL arg0, String arg1) throws Exception {
    return list(arg0, arg1, false, null);
  }

  public Vector find(GlobusURL arg0, String arg1) throws Exception {
    return list(arg0, arg1, true, null);
  }

  public void putFile(File arg0, GlobusURL arg1) throws Exception {
    putFile(arg0, arg1, null);
  }

  public String getName() {
    return PLUGIN_NAME;
  }

}
