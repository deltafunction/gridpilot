package gridpilot.ftplugins.sss;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.swing.JOptionPane;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;

import org.globus.ftp.exception.ServerException;
import org.globus.util.GlobusURL;
import org.jets3t.service.Constants;
import org.jets3t.service.Jets3tProperties;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.io.BytesTransferredWatcher;
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

import gridpilot.ConfirmBox;
import gridpilot.Debug;
import gridpilot.FileTransfer;
import gridpilot.GridPilot;
import gridpilot.LogFile;
import gridpilot.StatusBar;
import gridpilot.Util;

/**
 * Portions of code in this class taken from org.jets3t.samples and org.jets3t.apps.cockpit
 * @author fjob
 *
 */

public class SSSFileTransfer implements FileTransfer{
  
  private AWSCredentials awsCredentials = null;
  private S3Service s3Service = null;
  private LogFile logFile = null;
  private boolean filesWorldReadable = false;
  private boolean compressUploads = false;
  private boolean encryptUploads = false;
  private String encryptionPassword = null;
  private Map filesAlreadyInDownloadDirectoryMap = null;
  private Map s3DownloadObjectsMap = null;
  private Map filesForUploadMap = null;
  private Map s3ExistingObjectsMap = null;
  private HashMap s3ServiceEventListeners = null;
  private HashMap fileTransfers = null;
  private HashMap myBuckets = null;
  private S3Object[] existingObjects = new S3Object [] {};
  
  // Load the AWS credentials. 3s does not support X509 credentials..
  public SSSFileTransfer(){
    logFile = GridPilot.getClassMgr().getLogFile();
    s3ServiceEventListeners = new HashMap();
    fileTransfers = new HashMap();
    String accessKey = GridPilot.getClassMgr().getConfigFile().getValue("SSS",
       "AWS access key id");
    String secretKey = GridPilot.getClassMgr().getConfigFile().getValue("SSS",
       "AWS secret access key");
    String readableStr = GridPilot.getClassMgr().getConfigFile().getValue("SSS",
       "World readable files");
    filesWorldReadable = readableStr!=null &&
       (readableStr.equalsIgnoreCase("yes") || readableStr.equalsIgnoreCase("true"));
    String compressUploadsStr = GridPilot.getClassMgr().getConfigFile().getValue("SSS",
       "Compress uploads");
    compressUploads = compressUploadsStr!=null &&
       (compressUploadsStr.equalsIgnoreCase("yes") || compressUploadsStr.equalsIgnoreCase("true"));
    encryptionPassword = GridPilot.getClassMgr().getConfigFile().getValue("SSS",
       "Encryption password");
    encryptUploads = encryptionPassword!=null && !encryptionPassword.equalsIgnoreCase("");
    awsCredentials = new AWSCredentials(accessKey, secretKey);
  }
  
  private S3Bucket getBucket(String bucketName) throws Exception{
    if(myBuckets.containsKey(bucketName)){
      return (S3Bucket) myBuckets.get(bucketName);
    }
    s3Service = new RestS3Service(awsCredentials);
    S3Bucket [] myBucketsArr = s3Service.listAllBuckets();
    Debug.debug("Number of buckets in S3? " + myBucketsArr.length, 1);
    // Check if the bucket specified in the config files exists, if not, try to create it
    S3Bucket bucket = null;
    for(int i=0; i<myBucketsArr.length; ++i){
      if(myBucketsArr[i].getName().equals(bucketName)){
        bucket = myBucketsArr[i];
        myBuckets.put(bucketName, bucket);
        break;
      }
    }
    return bucket;
  }

  public boolean checkURLs(GlobusURL[] srcUrls, GlobusURL[] destUrls)
      throws Exception{
    return (srcUrls.length==destUrls.length && (
        srcUrls[0].getProtocol().equalsIgnoreCase("sss") &&
           destUrls[0].getProtocol().equalsIgnoreCase("file") ||
        srcUrls[0].getProtocol().equalsIgnoreCase("file") &&
           destUrls[0].getProtocol().equalsIgnoreCase("sss")
          ));
  }
 

  /**
   * The URLs are of the form sss://<bucket name>/some/file/name
   */
  public String[] startCopyFiles(GlobusURL[] srcUrls, GlobusURL[] destUrls)
      throws Exception{

    //prepareForFilesUpload(File[] uploadFiles, StatusBar statusBar, final S3Bucket bucket);
    
    //prepareForObjectsDownload(S3Object [] s3Objects, StatusBar statusBar, final S3Bucket bucket)
        
    // TODO Auto-generated method stub
    return null;
  }

  public String getUserInfo() throws Exception{
    // TODO Auto-generated method stub
    return null;
  }

  public String getFullStatus(String fileTransferID) throws Exception{
    // TODO Auto-generated method stub
    return null;
  }

  public String getStatus(String fileTransferID) throws Exception{
    // TODO Auto-generated method stub
    return null;
  }

  public int getInternalStatus(String fileTransferID,String status)
      throws Exception{
    // TODO Auto-generated method stub
    return 0;
  }

  public long getFileBytes(GlobusURL url) throws Exception{
    // TODO Auto-generated method stub
    return 0;
  }

  public long getBytesTransferred(String fileTransferID) throws Exception{
    // TODO Auto-generated method stub
    return 0;
  }

  public int getPercentComplete(String fileTransferID) throws Exception{
    // TODO Auto-generated method stub
    return 0;
  }

  public void cancel(String fileTransferID) throws Exception{
    // TODO Auto-generated method stub

  }

  public void finalize(String fileTransferID) throws Exception{
    // TODO Auto-generated method stub

  }

  public void deleteFiles(GlobusURL[] destUrls) throws Exception{
    // TODO Auto-generated method stub

  }

  public void getFile(GlobusURL globusUrl,File downloadDirOrFile,
      StatusBar statusBar) throws Exception{
    // TODO Auto-generated method stub

  }

  public void putFile(File file,GlobusURL globusFileUrl,StatusBar statusBar)
      throws Exception{
    // TODO Auto-generated method stub

  }

  public Vector list(GlobusURL globusUrl, String filter, StatusBar statusBar)
      throws Exception{
    
    // The URLs are of the form sss://bucket/some/file/name
    // getHost() --> bucket, getPath() --> some/file/name
    S3Bucket bucket = getBucket(globusUrl.getHost());
    if(bucket==null){
      String error ="WARNING: bucket not found: "+globusUrl.getHost();
      Debug.debug(error, 1);
      return new Vector();
    }
    
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

    Vector resVec = new Vector();
    existingObjects = s3Service.listObjects(bucket, globusUrl.getPath(), null);
    int directories = 0;
    int files = 0;
    for(int o = 0; o<existingObjects.length; o++){
      if(!existingObjects[o].getKey().matches(filter)){
        continue;
      }
      if(existingObjects[o].getContentType().equals(Mimetypes.MIMETYPE_JETS3T_DIRECTORY) ||
          existingObjects[o].getKey().endsWith("/")){
        ++directories;
      }
      else{
        ++files;
      }
      resVec.add(existingObjects[o].getKey() + " " + existingObjects[o].getContentLength()/*bytes*/);
    }
    if(statusBar!=null){
      statusBar.setLabel(directories+" directories, "+files+" files");
    }
    return resVec;
  }

  public void abortTransfer(String id) throws ServerException,IOException{
    // TODO Auto-generated method stub

  }

  public void write(GlobusURL globusUrl,String text) throws Exception{
    // TODO Auto-generated method stub

  }
  
  private void prepareForFilesUpload(File[] uploadFiles, StatusBar statusBar, final S3Bucket bucket){
    // Build map of files proposed for upload.
    filesForUploadMap = FileComparer.buildFileMap(uploadFiles);
                
    // Build map of objects already existing in target S3 bucket with keys
    // matching the proposed upload keys.
    List objectsWithExistingKeys = new ArrayList();
    for (int i = 0; i<existingObjects.length; i++) {
        if(filesForUploadMap.keySet().contains(existingObjects[i].getKey())){
            objectsWithExistingKeys.add(existingObjects[i]);
        }
    }
    existingObjects = (S3Object[]) objectsWithExistingKeys.toArray(
        new S3Object[objectsWithExistingKeys.size()]);
    s3ExistingObjectsMap = FileComparer.populateS3ObjectMap("", existingObjects);
    if(existingObjects.length>0){
        // Retrieve details of potential clashes.
        final S3Object[] clashingObjects = existingObjects;
        (new Thread() {
            public void run() {
                retrieveObjectsDetails(clashingObjects, bucket);
            }
        }).start();
    }
    else{
        compareRemoteAndLocalFiles(filesForUploadMap, s3ExistingObjectsMap, true,
            statusBar, null, bucket);
    }
  }

  private void prepareForObjectsDownload(S3Object [] s3Objects, StatusBar statusBar,
      final S3Bucket bucket) throws IOException{
    File downloadDirectory = Util.getDownloadDir(JOptionPane.getRootFrame());

    // Build map of existing local files.
    Map filesInDownloadDirectoryMap = FileComparer.buildFileMap(downloadDirectory, null);
    filesAlreadyInDownloadDirectoryMap = new HashMap();
    
    // Build map of S3 Objects being downloaded. 
    s3DownloadObjectsMap = FileComparer.populateS3ObjectMap("", s3Objects);

    // Identify objects that may clash with existing files, or may be directories,
    // and retrieve details for these.
    ArrayList potentialClashingObjects = new ArrayList();
    Set existingFilesObjectKeys = filesInDownloadDirectoryMap.keySet();
    Iterator objectsIter = s3DownloadObjectsMap.entrySet().iterator();
    while (objectsIter.hasNext()){
      Map.Entry entry = (Map.Entry) objectsIter.next();
      String objectKey = (String) entry.getKey();
      S3Object object = (S3Object) entry.getValue();
      
      if (object.getContentLength()==0 || existingFilesObjectKeys.contains(objectKey)){
        potentialClashingObjects.add(object);
      }
      if (existingFilesObjectKeys.contains(objectKey)){
        filesAlreadyInDownloadDirectoryMap.put(
            objectKey, filesInDownloadDirectoryMap.get(objectKey));
      }
    }
    
    if(potentialClashingObjects.size()>0){
      logFile.addInfo("WARNING: overwriting files "+
          Util.arrayToString(filesInDownloadDirectoryMap.keySet().toArray()));
    }
    if(potentialClashingObjects.size()>0){
      // Retrieve details of potential clashes.
      final S3Object[] clashingObjects = (S3Object[])
          potentialClashingObjects.toArray(new S3Object[potentialClashingObjects.size()]);
      (new Thread() {
          public void run() {
              retrieveObjectsDetails(clashingObjects, bucket);
          }
      }).start();
    }
    else{
      compareRemoteAndLocalFiles(filesAlreadyInDownloadDirectoryMap, s3DownloadObjectsMap,
          false, statusBar, downloadDirectory, bucket);
    }
  }
  
  private void performFilesUpload(FileComparerResults comparisonResults,
      Map uploadingFilesMap, StatusBar statusBar, final S3Bucket bucket) throws Exception {
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
          
          int response = confirmBox.getConfirm("Files already exist!", message,
              new Object[] {"OK", "Cancel"}, null, null, false);
          
          if(response!=1){
              return;
          }
          
          fileKeysForUpload.addAll(comparisonResults.updatedOnClientKeys);
          fileKeysForUpload.addAll(comparisonResults.updatedOnServerKeys);
          fileKeysForUpload.addAll(comparisonResults.alreadySynchronisedKeys);

      }

      if (fileKeysForUpload.size()==0){
          return;
      }
      
      final JProgressBar pb = new JProgressBar();
      if(statusBar!=null){
        statusBar.setLabel("Prepared 0 of " + fileKeysForUpload.size() 
            + " file(s) for upload");
        pb.setMaximum(fileKeysForUpload.size());
        statusBar.setProgressBar(pb);
      }
      
      // Populate S3Objects representing upload files with metadata etc.
      final S3Object[] objects = new S3Object[fileKeysForUpload.size()];
      int objectIndex = 0;
      for(Iterator iter = fileKeysForUpload.iterator(); iter.hasNext();) {
          String fileKey = iter.next().toString();
          File file = (File) uploadingFilesMap.get(fileKey);
          
          S3Object newObject = new S3Object(fileKey);
          
          if(!filesWorldReadable){
              newObject.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
          }
          
          if(file.isDirectory()){
              newObject.setContentType(Mimetypes.MIMETYPE_JETS3T_DIRECTORY);
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
              
              if (!fileToUpload.equals(file)) {
                  // Compute the MD5 hash of the *original* file, if upload file has been altered
                  // through encryption or gzipping.
                  newObject.addMetadata(
                      S3Object.METADATA_HEADER_ORIGINAL_HASH_MD5,
                      ServiceUtils.toBase64(ServiceUtils.computeMD5Hash(new FileInputStream(file))));
              }
              statusBar.setLabel("Prepared " + (objectIndex + 1) 
                  + " of " + fileKeysForUpload.size() + " file(s) for upload");
              pb.setMaximum((objectIndex + 1));
          }
          objects[objectIndex++] = newObject;
      }
      statusBar.removeProgressBar(pb);
      
      S3ServiceEventListener s3Listener = new MyS3ServiceEventListener();
      final S3ServiceMulti s3ServiceMulti = new S3ServiceMulti(s3Service, s3Listener);
      
      // Upload the files.
      Runnable r = new Runnable() {
        public void run() {
          s3ServiceMulti.putObjects(bucket, objects);
        }
      };
      SwingUtilities.invokeLater(r);
  }

  private void performObjectsDownload(FileComparerResults comparisonResults,
      Map s3DownloadObjectsMap, File downloadDirectory, final S3Bucket bucket) throws Exception {        
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
      
      int response = confirmBox.getConfirm("Files already exist!", message,
          new Object[] {"OK", "Cancel"}, null, null, false);
      
      if(response!=1){
          return;
      }
      
      objectKeysForDownload.addAll(comparisonResults.updatedOnClientKeys);
      objectKeysForDownload.addAll(comparisonResults.updatedOnServerKeys);
      objectKeysForDownload.addAll(comparisonResults.alreadySynchronisedKeys);
    
    }

    Debug.debug("Downloading " + objectKeysForDownload.size() + " objects", 2);
    if(objectKeysForDownload.size()==0){
        return;
    }
                
    // Create array of objects for download.        
    S3Object[] objects = new S3Object[objectKeysForDownload.size()];
    int objectIndex = 0;
    for (Iterator iter = objectKeysForDownload.iterator(); iter.hasNext();) {
        objects[objectIndex++] = (S3Object) s3DownloadObjectsMap.get(iter.next()); 
    }
                
    HashMap downloadObjectsToFileMap = new HashMap();
    ArrayList downloadPackageList = new ArrayList();

    // Setup files to write to, creating parent directories when necessary.
    for(int i=0; i<objects.length; i++){
      File file = new File(downloadDirectory, objects[i].getKey());
      
      // Create directory corresponding to object, or parent directories of object.
      if(Mimetypes.MIMETYPE_JETS3T_DIRECTORY.equals(objects[i].getContentType())) {
        file.mkdirs();
        // No further data to download for directories...
        continue;
      }
      else{
        if(file.getParentFile()!=null){
          file.getParentFile().mkdirs();
        }
      }
      
      downloadObjectsToFileMap.put(objects[i].getKey(), file);

      boolean isZipped = false;
      EncryptionUtil encryptionUtil = null;
      
      if("gzip".equalsIgnoreCase(objects[i].getContentEncoding())
          || objects[i].containsMetadata(Constants.METADATA_JETS3T_COMPRESSED)){
        // Automatically inflate gzipped data.
        isZipped = true;
      }
      if(objects[i].containsMetadata(Constants.METADATA_JETS3T_CRYPTO_ALGORITHM) ||
          objects[i].containsMetadata(Constants.METADATA_JETS3T_ENCRYPTED_OBSOLETE)){
        Debug.debug("Decrypting encrypted data for object: " + objects[i].getKey(), 2);
        
        // Prompt user for the password, if necessary.
        if(encryptionPassword==null || encryptionPassword.equalsIgnoreCase("")){
          throw new S3ServiceException(
              "One or more objects are encrypted. Cockpit cannot download encrypted "
              + "objects unless the encyption password is set in Preferences");
        }

        if(objects[i].containsMetadata(Constants.METADATA_JETS3T_ENCRYPTED_OBSOLETE)) {
          // Item is encrypted with obsolete crypto.
          logFile.addMessage("WARNING: Object is encrypted with out-dated crypto version, please update it when possible: " 
              + objects[i].getKey());
          encryptionUtil = EncryptionUtil.getObsoleteEncryptionUtil(encryptionPassword);                                            
        }
        else{
          String algorithm = (String) objects[i].getMetadata(
              Constants.METADATA_JETS3T_CRYPTO_ALGORITHM);
          String version = (String) objects[i].getMetadata(
              Constants.METADATA_JETS3T_CRYPTO_VERSION);
          if (version == null) {
              version = EncryptionUtil.DEFAULT_VERSION;
          }
          encryptionUtil = new EncryptionUtil(encryptionPassword, algorithm, version);                                            
        }                    
      }
      
      downloadPackageList.add(new DownloadPackage(
          objects[i], file, isZipped, encryptionUtil));            
    }
    
    // Download the files.
    final DownloadPackage[] downloadPackagesArray = (DownloadPackage[])
        downloadPackageList.toArray(new DownloadPackage[downloadPackageList.size()]);            
    S3ServiceEventListener s3Listener = new MyS3ServiceEventListener();
    final S3ServiceMulti s3ServiceMulti = new S3ServiceMulti(s3Service, s3Listener);
    Runnable r = new Runnable(){
      public void run(){
        s3ServiceMulti.downloadObjects(bucket, downloadPackagesArray);
      }
    };
    SwingUtilities.invokeLater(r);
  }
  
  private void compareRemoteAndLocalFiles(final Map localFilesMap, final Map s3ObjectsMap,
      final boolean upload, StatusBar statusBar, File downloadDirectory, S3Bucket bucket){
    final JProgressBar pb = new JProgressBar();
    try{
      // Compare objects being downloaded and existing local files.
      final String statusText =
          "Comparing " + s3ObjectsMap.size() + " object" + (s3ObjectsMap.size() > 1 ? "s" : "") +
          " in S3 with " + localFilesMap.size() + " local file" + (localFilesMap.size() > 1 ? "s" : "");
      if(statusBar!=null){
        statusBar.setLabel(statusText);
        pb.setMaximum(100);
        statusBar.setProgressBar(pb);
      }
      
      // Calculate total files size.
      File[] files = (File[]) localFilesMap.values().toArray(new File[localFilesMap.size()]);
      final long filesSizeTotal[] = new long[1];
      for (int i = 0; i < files.length; i++) {
          filesSizeTotal[0] += files[i].length();
      }
      
      // Monitor generation of MD5 hash, and provide feedback via the progress bar. 
      final long hashedBytesTotal[] = new long[1];
      hashedBytesTotal[0] = 0;
      final BytesTransferredWatcher hashWatcher = new BytesTransferredWatcher() {
          public void bytesTransferredUpdate(long transferredBytes) {
              hashedBytesTotal[0] += transferredBytes;
              final int percentage = 
                  (int) (100 * hashedBytesTotal[0] / filesSizeTotal[0]);
              SwingUtilities.invokeLater(new Runnable(){
                  public void run(){
                    pb.setValue(percentage);
                  }
              });
          }
      };
      
      FileComparerResults comparisonResults = 
          FileComparer.buildDiscrepancyLists(localFilesMap, s3ObjectsMap, hashWatcher);
      
      statusBar.removeProgressBar(pb); 
      
      if(upload){
          performFilesUpload(comparisonResults, localFilesMap, statusBar, bucket);
      }
      else{
          performObjectsDownload(comparisonResults, s3ObjectsMap, downloadDirectory, bucket);                
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
      final File tempUploadFile = File.createTempFile("JetS3tCockpit",".tmp");
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
    for (int i = 0; i < candidateObjects.length; i++) {
        if (!candidateObjects[i].isMetadataComplete()) {
            s3ObjectsIncompleteList.add(candidateObjects[i]);
        }
    }
    
    Debug.debug("Of " + candidateObjects.length + " object candidates for HEAD requests "
        + s3ObjectsIncompleteList.size() + " are incomplete, performing requests for these only", 1);
    
    final S3Object[] incompleteObjects = (S3Object[]) s3ObjectsIncompleteList
        .toArray(new S3Object[s3ObjectsIncompleteList.size()]);        
    (new Thread() {
        public void run() {
          S3ServiceEventListener s3Listener = new MyS3ServiceEventListener();
          S3ServiceMulti s3ServiceMulti = new S3ServiceMulti(s3Service, s3Listener);
            s3ServiceMulti.getObjectsHeads(bucket, incompleteObjects);
        };
    }).start();
  }


}
