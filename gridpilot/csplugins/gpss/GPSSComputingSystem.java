package gridpilot.csplugins.gpss;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import javax.naming.TimeLimitExceededException;
import javax.swing.Timer;

import jonelo.jacksum.JacksumAPI;
import jonelo.jacksum.algorithm.AbstractChecksum;

import org.globus.ftp.exception.FTPException;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;

import gridpilot.ComputingSystem;
import gridpilot.ConfigFile;
import gridpilot.DBPluginMgr;
import gridpilot.DBRecord;
import gridpilot.DBResult;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobInfo;
import gridpilot.LocalStaticShellMgr;
import gridpilot.LogFile;
import gridpilot.PullJobsDaemon;
import gridpilot.TransferControl;
import gridpilot.TransferInfo;
import gridpilot.Util;
import gridpilot.ftplugins.gsiftp.GSIFTPFileTransfer;

public class GPSSComputingSystem implements ComputingSystem{

  private String error = "";
  private LogFile logFile = null;
  private String [] localRuntimeDBs = null;
  private String remoteDB = null;
  private String csName;
  private HashSet finalRuntimesLocal = null;
  private String user = null;
  private String remoteDir = null;
  private GSIFTPFileTransfer gsiftpFileTransfer = null;
  private Vector allowedSubjects = null;
  
  // RTEs are refreshed from entries written by pull nodes in remote database
  // every RTE_SYNC_DELAY milliseconds.
  private static int RTE_SYNC_DELAY = 60000;
  // Wait max 60 seconds for all input files to be uploaded
  private static int MAX_UPLOAD_WAIT = 60000;
  // Time to wait for stdout/stderr to be uploaded
  private static int STDOUT_WAIT = 20000;
  
  private Timer timerSyncRTEs = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      Debug.debug("Syncing RTEs", 2);
      cleanupRuntimeEnvironments();
      setupRuntimeEnvironments();
    }
  });

  public GPSSComputingSystem(String _csName){
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    finalRuntimesLocal = new HashSet();
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    localRuntimeDBs = configFile.getValues(csName, "runtime databases");
    remoteDB = configFile.getValue(csName, "remote database");
    remoteDir = configFile.getValue(csName, "remote directory");
    timerSyncRTEs.setDelay(RTE_SYNC_DELAY);
    // Set user
    try{
      Debug.debug("getting credential", 3);
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
      
      // Append hash of the user subject to the remote directory name
      AbstractChecksum checksum = null;
      try{
        checksum = JacksumAPI.getChecksumInstance("cksum");
      }
      catch(NoSuchAlgorithmException e){
        e.printStackTrace();
      }
      checksum.update(user.getBytes());
      String dir = checksum.getFormattedValue();
      if(!remoteDir.endsWith("/")){
        remoteDir += "/";
      }
      remoteDir = remoteDir + dir + "/";
      // Create the directory
      mkRemoteDir(remoteDir);
      // Set up list of trusted subjects
      String subjects = configFile.getValue(csName, "Allowed subjects");
      String [] subjectsArray = Util.splitUrls(subjects);
      allowedSubjects = new Vector();
      for(int i=0; i<subjectsArray.length; ++i){
        if(subjectsArray[i].matches("/.*=.*/.*")){
          // assume this is a subject
          allowedSubjects.add(subjectsArray[i]);
        }
        else{
          // assume this is a URL
          try{
            File tmpFile = File.createTempFile("GridPilot-", "");
            Collections.addAll(allowedSubjects, Util.readURL(subjectsArray[i],tmpFile));
            tmpFile.delete();
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    catch(Exception ioe){
      error = "ERROR during initialization of GPSS plugin\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
  }

  /**
   * Copies jobDefinition plus the associated dataset to the (remote) database,
   * where it will be picked up by pull clients. Sets csState to 'ready'.
   * Sets finalStdOut and finalStdErr and any local output files 
   * to files in the remote gridftp directory.
   */
  public boolean submit(JobInfo job){
    DBRecord jobDefinition = null;
    DBRecord dataset = null;
    String datasetID = null;
    String datasetName = null;
    DBRecord transformation = null;
    String transformationName = null;
    String transformationVersion = null;
    String transformationID = null;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    // First, read the jobDefinition, dataset and transformation.
    try{
      jobDefinition = dbPluginMgr.getJobDefinition(job.getJobDefId());
      datasetID = dbPluginMgr.getJobDefDatasetID(job.getJobDefId());
      dataset = dbPluginMgr.getDataset(datasetID);
      datasetName = (String) dataset.getValue(Util.getNameField(dbPluginMgr.getDBName(), "dataset"));
      transformationName = dbPluginMgr.getDatasetTransformationName(datasetID);
      transformationVersion = dbPluginMgr.getDatasetTransformationVersion(datasetID);
      transformationID = dbPluginMgr.getTransformationID(transformationName, transformationVersion);
      transformation = dbPluginMgr.getTransformation(transformationID);
      // Hmm, 7 database lookups. Perhaps we should reconsider this...
    }
    catch(Exception e){
      error = "ERROR: could not read jobDefinition, dataset or transformation. "+e.getMessage();
      return false;
    }
    try{
      // Create the transformation if necessary
      String remoteTransformationID = remoteMgr.getTransformationID(transformationName, transformationVersion);
      if(remoteTransformationID==null || remoteTransformationID.equals("-1") ||
          remoteTransformationID.equals("-1")){
        remoteMgr.createTrans(transformation.fields, transformation.values);
      }
      // Tag it for deletion
      tagDeleteTransformation(remoteMgr, transformationName, transformationVersion);
      // Create the dataset if necessary
      String remoteDatasetID = null;
      try{
        remoteDatasetID = remoteMgr.getDatasetID(datasetName);
      }
      catch(Exception ee){
      }
      if(remoteDatasetID==null || remoteDatasetID.equals("-1") || remoteDatasetID.equals("")){
        remoteMgr.createDataset("dataset", dataset.fields, dataset.values);
      }
      // Tag it for deletion
      tagDeleteDataset(remoteMgr, datasetName);
      // Modify and write the jobDefinition
      jobDefinition.setValue("csStatus", PullJobsDaemon.STATUS_READY);
      try{
        jobDefinition = updateURLs(jobDefinition);
        // TODO: look into this...
        job.setOutputs((String) jobDefinition.getValue("stdoutDest"),
            (String) jobDefinition.getValue("stderrDest"));
      }
      catch(Exception ee){
        error = "WARNING: could not update input/output file URLs of job";
        logFile.addMessage(error, ee);
      }
      remoteMgr.createJobDef(jobDefinition.fields, jobDefinition.values);
    }
    catch(Exception e){
      error = "ERROR: could not write to 'remote' database. "+e.getMessage();
      return false;
    }
    return true;
  }
  
  private String getRemoteJobdefID(JobInfo job) throws IOException{
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    String localJobDefID = job.getJobDefId();
    if(localJobDefID==null || Integer.parseInt(localJobDefID)<0){
      throw new IOException("Could not get local jobDefinition ID");
    }
    String name = job.getName();
    String localDatasetID = dbPluginMgr.getJobDefDatasetID(localJobDefID);
    String dsName = dbPluginMgr.getDatasetName(localDatasetID);
    String remoteNameField = Util.getNameField(remoteMgr.getDBName(), "jobDefinition");
    String remoteIdField = Util.getIdentifierField(remoteMgr.getDBName(), "jobDefinition");
    String [] remoteJobDefDSRef = Util.getJobDefDatasetReference(remoteMgr.getDBName());
    String providerInfo = dbPluginMgr.getJobDefValue(localJobDefID, "providerInfo");
    // The remote DB will be a MySQL DB
    DBResult remoteJobDefinitions = remoteMgr.select(
        "SELECT "+remoteIdField+" FROM jobDefinition WHERE "+remoteNameField+" = '"+name+
        "' AND "+remoteJobDefDSRef[1]+" = '"+dsName+"' AND csStatus != '' AND (" +
        "userInfo = '"+user+"'" + 
        (providerInfo!=null&&!providerInfo.equals("null")?
            " OR providerInfo = '"+providerInfo+"')":")"), remoteIdField, false);
    if(remoteJobDefinitions.values.length>1){
      error = "ERROR: more than one jobDefinition with the same name!";
      throw new IOException(error);
    }
    if(remoteJobDefinitions.values.length<1){
      error = "ERROR: no jobDefinition found with the name "+name;
      throw new IOException(error);
    }
    return (String) remoteJobDefinitions.getValue(0, remoteIdField);
  }
  
  private String getRemoteDir(String name){
    AbstractChecksum checksum;
    try{
      checksum = JacksumAPI.getChecksumInstance("cksum");
    }
    catch(NoSuchAlgorithmException e){
      e.printStackTrace();
      return null;
    }
    checksum.update(name.getBytes());
    String dir = checksum.getFormattedValue();
    Debug.debug("Using directory name from cksum of jobDefinition name: "+dir, 2);
    if(!remoteDir.endsWith("/")){
      remoteDir += "/";
    }
    return remoteDir+dir+"/";
  }
  
  private void mkRemoteDir(String url) throws IOException, FTPException{
    if(!url.endsWith("/")){
      throw new IOException("Directory URL: "+url+" does not end with a slash.");
    }
    GlobusURL globusUrl= new GlobusURL(url);
    if(gsiftpFileTransfer==null){
      gsiftpFileTransfer = new GSIFTPFileTransfer();
    }
    // First, check if directory already exists.
    try{
      gsiftpFileTransfer.list(globusUrl, null, null);
      return;
    }
    catch(Exception e){
    }
    // If not, create it.
    Debug.debug("Creating directory "+globusUrl.getURL(), 2);
    gsiftpFileTransfer.write(globusUrl, "");
  }
  
  /**
   * Deletes a gsiftp URL after deleting all files in it.
   * Will not work if the URL has any sub-directories.
   * @param url
   * @throws Exception
   */
  private void deleteRemoteDir(String url) throws Exception{
    if(!url.endsWith("/")){
      throw new IOException("Directory URL: "+url+" does not end with a slash.");
    }
    GlobusURL globusUrl= new GlobusURL(url);
    if(gsiftpFileTransfer==null){
      gsiftpFileTransfer = new GSIFTPFileTransfer();
    }
    // First, check if directory exists.
    GlobusURL [] fileURLs = null;
    String fileName = null;
    String [] entryArr = null;
    String line;
    try{
      Vector files = gsiftpFileTransfer.list(globusUrl, null, null);
      fileURLs = new GlobusURL [files.size()];
      for(int i=0; i<fileURLs.length; ++i){
        line = (String) files.get(i);
        entryArr = Util.split(line);
        fileName = entryArr[entryArr.length-1];
        fileURLs[i] = new GlobusURL(url+fileName);
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    // If it does, delete the files then the directory.
    gsiftpFileTransfer.deleteFiles(fileURLs);
    gsiftpFileTransfer.deleteFile(globusUrl);
  }
  
  /**
   * Updates the URLs of local input/output files and stdout/stderr to be in the
   * defined remoted directory. Also sets 'userInfo'. This is to be able
   * to identify 'our' jobDefinitions. Also sets status to 'defined'.
   * @param job definition
   * @return The updated job definition
   * @throws Exception
   */
  private DBRecord updateURLs(DBRecord jobDefinition) throws Exception{
    String nameField = Util.getNameField(remoteDB, "jobDefinition");
    // TODO: consider moving ALL input files to remote directory.
    // TransferControl should be able to handle third-party transfers.
    //String replacePattern = "^[a-z][a-z]+:.+/([^/]+)"
    String rDir = getRemoteDir((String) jobDefinition.getValue(nameField));
    String replacePattern = "^file:.+/([^/]+)";
    String [] inputFileNames = Util.splitUrls((String) jobDefinition.getValue("inputFileNames"));
    String newInputFileName = null;
    for(int i=0; i<inputFileNames.length; ++i){
      if(!Util.urlIsRemote(inputFileNames[i])){
        newInputFileName = inputFileNames[i].replaceFirst(replacePattern, rDir+"$1");
        inputFileNames[i] = newInputFileName;
      }
    }
    String [] outFileMapping = Util.splitUrls((String) jobDefinition.getValue("outFileMapping"));
    for(int i=0; i<outFileMapping.length; ++i){
      if((i+1)%2==0 && !Util.urlIsRemote(outFileMapping[i])){
        outFileMapping[i] = outFileMapping[i].replaceFirst(replacePattern, rDir+"$1");
      }
    }
    String stdoutDest = (String) jobDefinition.getValue("stdoutDest");
    String stderrDest = (String) jobDefinition.getValue("stderrDest");
    stdoutDest = stdoutDest.replaceFirst(replacePattern, rDir+"$1");
    stderrDest = stderrDest.replaceFirst(replacePattern, rDir+"$1");
    jobDefinition.setValue("inputFileNames", Util.arrayToString(inputFileNames));
    jobDefinition.setValue("outFileMapping", Util.arrayToString(outFileMapping));
    jobDefinition.setValue("stdoutDest", stdoutDest);
    jobDefinition.setValue("stderrDest", stderrDest);
    jobDefinition.setValue("userInfo", user);
    jobDefinition.setValue("providerInfo", user);
    jobDefinition.setValue("status", "defined");
    Debug.debug("Updated URLs:" +Util.arrayToString(inputFileNames)+" :: "+
        Util.arrayToString(outFileMapping)+" :: "+stdoutDest+" :: "+stderrDest, 2);
    return jobDefinition;
  }

  /**
   * Uploads local input files to the defined remote directory.
   * @param job definition
   * @return The updated job definition
   * @throws Exception
   */
  private void uploadLocalInputFiles(DBRecord jobDefinition, String rDir) throws Exception{
    // TODO: consider moving ALL input files to remote directory.
    // TransferControl should be able to handle third-party transfers.
    //String replacePattern = "^[a-z][a-z]+:.+/([^/]+)"
    String replacePattern = "^file:.+/([^/]+)";
    String [] inputFileNames = Util.splitUrls((String) jobDefinition.getValue("inputFileNames"));
    String newInputFileName = null;
    TransferInfo transfer = null;
    Vector transferVector = new Vector();
    // Start transfers
    for(int i=0; i<inputFileNames.length; ++i){
      if(inputFileNames[i].startsWith("file:")){
        newInputFileName = inputFileNames[i].replaceFirst(replacePattern, rDir+"$1");
        transfer = new TransferInfo(
            new GlobusURL(inputFileNames[i].replaceFirst("file:/+", "file:////")),
            new GlobusURL(newInputFileName));
        transferVector.add(transfer);
      }
    }
    GridPilot.getClassMgr().getTransferControl().queue(transferVector);
    // Wait for transfers to finish
    boolean transfersDone = false;
    int sleepT = 3000;
    int waitT = 0;
    while(!transfersDone && waitT*sleepT<MAX_UPLOAD_WAIT){
      transfersDone = true;
      for(Iterator itt=transferVector.iterator(); itt.hasNext();){
        transfer = (TransferInfo) itt.next();
        if(TransferControl.isRunning(transfer)){
          transfersDone = false;
          break;
        }
      }
      if(transfersDone){
        break;
      }
      Thread.sleep(sleepT);
      ++ waitT;
    }
    if(!true){
      TransferControl.cancel(transferVector);
      throw new TimeLimitExceededException("Upload took too long, aborting.");
    }
  }

  /**
   * Tags a transformation record, copied to a remote database only to be
   * able to run a job, for deletion once the job has finished.
   * If the transformation happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void tagDeleteTransformation(DBPluginMgr remoteMgr, String transName, String transVersion){
    String transformationID = remoteMgr.getTransformationID(transName, transVersion);
    String comment = (String) remoteMgr.getTransformation(transformationID).getValue("comment");
    if(comment!=null && comment.startsWith("volatile:")){
      if(!comment.matches("volatile::'"+user+"'.*") &&
          !comment.matches("volatile::.*::'"+user+"'.*")){
        comment = comment+"::'"+user+"'";
      }
    }
    else{
      comment = "volatile::'"+user+"'";
    }
    remoteMgr.updateTransformation(transformationID, new String [] {"comment"}, new String [] {comment});
  }

  /**
   * Deletes transformation tagged for deletion.
   * If a transformation happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void deleteTaggedTransformation(DBPluginMgr remoteMgr, String transformationID){
    String comment = (String) remoteMgr.getTransformation(transformationID).getValue("comment");
    if(comment!=null && comment.startsWith("volatile:")){
      if(!comment.matches("volatile::'"+user+"'")){
        remoteMgr.deleteTransformation(transformationID);
      }
      else{
        comment = comment.replaceFirst("::'"+user+"'", "");
        remoteMgr.updateTransformation(transformationID, new String [] {"comment"}, new String [] {comment});
      }
    }
  }

  /**
   * Tags a dataset record, copied to a remote database only to be
   * able to run a job, for deletion once the job has finished.
   * If the dataset happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void tagDeleteDataset(DBPluginMgr remoteMgr, String datasetName){
    String datasetID = remoteMgr.getDatasetID(datasetName);
    String metaData = (String) remoteMgr.getDataset(datasetID).getValue("metaData");
    if(metaData!=null && metaData.startsWith("volatile:")){
      if(!metaData.matches("volatile::'"+user+"'.*") &&
          !metaData.matches("volatile::.*::'"+user+"'.*")){
        metaData = metaData+"::'"+user+"'";
      }
    }
    else{
      metaData = "volatile::'"+user+"'";
    }
    remoteMgr.updateDataset(datasetID, new String [] {"metaData"}, new String [] {metaData});
  }

  /**
   * Deletes transformation tagged for deletion.
   * If a transformation happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void deleteTaggedDataset(DBPluginMgr remoteMgr, String datasetID){
    String metaData = (String) remoteMgr.getDataset(datasetID).getValue("metaData");
    if(metaData!=null && metaData.startsWith("volatile:")){
      if(!metaData.matches("volatile::'"+user+"'")){
        remoteMgr.deleteDataset(datasetID, false);
      }
      else{
        metaData = metaData.replaceFirst("::'"+user+"'", "");
        remoteMgr.updateDataset(datasetID, new String [] {"metaData"}, new String [] {metaData});
      }
    }
  }

  /**
   * Sets csState to 'requestKill' in remote jobDefinition record.
   */
  public boolean killJobs(Vector jobs){
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    boolean ok = true;
    Enumeration en = jobs.elements();
    JobInfo job = null;
    String remoteID= null;
    while(en.hasMoreElements()){
      job = (JobInfo) en.nextElement();
      try{
        remoteID = getRemoteJobdefID(job);
      }
      catch(IOException e){
        logFile.addMessage("WARNING: could not kill "+job, e);
        e.printStackTrace();
      }
      remoteMgr.updateJobDefinition(remoteID,
          new String [] {"csState"}, new String [] {PullJobsDaemon.STATUS_REQUESTED_KILLED});
    }
    return ok;
  }

  /**
   * Checks if a job has remote input/output files or stdout/stderr.
   * @param job
   * @return
   */
  private boolean hasLocalFiles(JobInfo job){
    boolean hasLocal = false;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    DBRecord jobDefinition = dbPluginMgr.getJobDefinition(job.getJobDefId());
    try{
      String [] inputFileNames = Util.splitUrls((String) jobDefinition.getValue("inputFileNames"));
      String [] outFileMapping = Util.splitUrls((String) jobDefinition.getValue("outFileMapping"));
      String stdoutDest = (String) jobDefinition.getValue("stdoutDest");
      String stderrDest = (String) jobDefinition.getValue("stderrDest");
      for(int i=0; i<inputFileNames.length; ++i){
        if(!Util.urlIsRemote(inputFileNames[i])){
          hasLocal = true;
          break;
        }
      }
      for(int i=0; i<outFileMapping.length; ++i){
        if((i+1)%2==0 && !Util.urlIsRemote(outFileMapping[i])){
          if(!Util.urlIsRemote(outFileMapping[i])){
            hasLocal = true;
            break;
          }
        }
      }
      if(!Util.urlIsRemote(stdoutDest)){
        hasLocal = true;
      }
      if(!Util.urlIsRemote(stderrDest)){
        hasLocal = true;
      }
    }
    catch(Exception e){
      error = "ERROR: problem pre-processing job. ";
      logFile.addMessage(error, e);
      return false;
    }
    return hasLocal;
  }
  
  /**
   * If any input or output files or finalStdout, finalStderr are local:
   * - Creates temporary directory on the configured gridftp server.
   * - Uploads any local input files and sets inputFiles accordingly.
   */
  public boolean preProcess(JobInfo job){
    // IF the job has local input or output files, create the remote temporary directory
    // and upload input files.
    if(hasLocalFiles(job)){
      String rDir = null;
      try{
        rDir = getRemoteDir(job.getName());
        if(!rDir.endsWith("/")){
          rDir += "/";
        }
        mkRemoteDir(rDir);
        DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
        DBRecord jobDefinition = dbPluginMgr.getJobDefinition(job.getJobDefId());
        uploadLocalInputFiles(jobDefinition, rDir);
      }
      catch(Exception e){
        error = "ERROR: could not create directory or upload remote files. "+e.getMessage();
        logFile.addMessage("could not create directory for remote output files", e);
        return false;
      }
    }
    return true;
  }

  /**
   * Clean up on remote gridftp server. Remove the remote jobDefinition.
   */
  public void clearOutputMapping(JobInfo job){
    // IF the job has local input or output files, delete the remote equivalents
    // and then the remote temporary directory.
    if(hasLocalFiles(job)){
      String rDir = null;
      try{
        rDir = getRemoteDir(job.getName());
        if(!rDir.endsWith("/")){
          rDir += "/";
        }
        deleteRemoteDir(rDir);
      }
      catch(Exception e){
        error = "ERROR: could not delete directory or remote files. ";
        logFile.addMessage(error, e);
      }
    }
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());

    // Delete files that may have been copied to final destination.
    // Files starting with file: are considered to locally available, accessed
    // with shellMgr
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getJobDefId());
    String fileName;
    Vector remoteFiles = new Vector();
    for(int i=0; i<outputFileNames.length; ++i){
      fileName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(), outputFileNames[i]);
      if(!Util.urlIsRemote(fileName)){
        LocalStaticShellMgr.deleteFile(fileName);
      }
      else{
        remoteFiles.add(fileName);
      }
    }
    String [] remoteFilesArr = new String [remoteFiles.size()];
    for(int i=0; i<remoteFilesArr.length; ++i){
      remoteFilesArr[i] = (String) remoteFiles.get(i);
    }
    try{
      TransferControl.deleteFiles(remoteFilesArr);
    }
    catch(Exception e){
      error = "WARNING: could not delete output files. "+e.getMessage();
      Debug.debug(error, 3);
    }
    
    // Delete stdout/stderr that may have been copied to final destination
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        TransferControl.deleteFiles(new String [] {finalStdOut});
      }
      catch(Exception e){
        error = "WARNING: could not delete "+finalStdOut+". "+e.getMessage();
        Debug.debug(error, 2);
      }
      catch(Throwable e){
        error = "WARNING: could not delete "+finalStdOut+". "+e.getMessage();
        Debug.debug(error, 2);
      }
    }
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      try{
        TransferControl.deleteFiles(new String [] {finalStdErr});
      }
      catch(Exception e){
        error = "WARNING: could not delete "+finalStdErr+". "+e.getMessage();
        Debug.debug(error, 2);
      }
      catch(Throwable e){
        error = "WARNING: could not delete "+finalStdErr+". "+e.getMessage();
        Debug.debug(error, 2);
      }
    }
    String remoteID = null;
    try{
      remoteID = getRemoteJobdefID(job);
    }
    catch(IOException e){
      e.printStackTrace();
      error = "ERROR: could not get remote jobDefinition ID";
      logFile.addMessage(error, e);
      return;
    }
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    Debug.debug("Deleting remote job definition "+remoteID, 2);
    remoteMgr.deleteJobDefinition(remoteID, false);
  }

  /**
   * Moves job.StdOut and job.StdErr to final destination specified in the DB. <p>
   * job.StdOut and job.StdErr are then set to these final values. <p>
   * @return <code>true</code> if the move went ok, <code>false</code> otherwise.
   */
  private boolean copyToFinalDest(JobInfo job){
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    
    // Output files
    // Try copying file(s) to output destination
    String jobDefID = job.getJobDefId();
    String remoteID = null;
    try{
      remoteID = getRemoteJobdefID(job);
    }
    catch(IOException e){
      e.printStackTrace();
      error = "ERROR: could not get remote jobDefinition ID";
      logFile.addMessage(error, e);
      return false;
    }
    String [] outputNames = dbPluginMgr.getOutputFiles(jobDefID);
    String remoteName = null;
    String origDest = null;
    TransferInfo transfer = null;
    Vector transferVector = new Vector();
    boolean ok = true;
    for(int i=0; i<outputNames.length; ++i){
      try{
        remoteName = remoteMgr.getJobDefOutRemoteName(remoteID, outputNames[i]);
        origDest = dbPluginMgr.getJobDefOutRemoteName(jobDefID, outputNames[i]);
        if(!remoteName.equals(origDest)/*origDest.startsWith("file:")*/){
          transfer = new TransferInfo(
              new GlobusURL(remoteName),
              new GlobusURL(origDest.replaceFirst("file:/+", "file:////")));
          transferVector.add(transfer);
        }
      }
      catch(Exception e){
        job.setJobStatus("Error");
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
        error = "Exception during copying of output file(s) for job : " + job.getName() + "\n" +
        "\tCommand\t: " + remoteName + ": -> " + origDest +"\n" +
        "\tException\t: " + e.getMessage();
        logFile.addMessage(error, e);
        ok = false;
      }
    }
    if(!ok){
      return ok;
    }
    
    // Stdout/stderr
    String origFinalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String origFinalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    String remoteFinalStdOut = remoteMgr.getStdOutFinalDest(remoteID);
    String remoteFinalStdErr = remoteMgr.getStdErrFinalDest(remoteID);
    
    /**
     * move temp StdOut -> finalStdOut
     */
    if(origFinalStdOut!=null && origFinalStdOut.trim().length()>0 &&
        remoteFinalStdOut!=null && remoteFinalStdOut.trim().length()>0 &&
        !origFinalStdOut.equals(remoteFinalStdOut) && !Util.urlIsRemote(origFinalStdOut)){
      try{
        TransferControl.download(remoteFinalStdOut, new File(origFinalStdOut),
            GridPilot.getClassMgr().getGlobalFrame().getContentPane());
      }
      catch(Exception e){
        e.printStackTrace();
        error = "Exception during copying of output file(s) for job : " + job.getName() + "\n" +
        "\tCommand\t: " + remoteFinalStdOut + ": -> " + origFinalStdOut +"\n" +
        "\tException\t: " + e.getMessage();
        logFile.addMessage(error, e);
        ok = false;
      }      
      job.setStdOut(origFinalStdOut); 
    }
    else{
      ok = false;
    }

    /**
     * move temp StdErr -> finalStdErr
     */
    if(origFinalStdErr!=null && origFinalStdErr.trim().length()>0 &&
        remoteFinalStdErr!=null && remoteFinalStdErr.trim().length()>0 &&
        !origFinalStdErr.equals(remoteFinalStdErr) && !Util.urlIsRemote(origFinalStdErr)){
      try{
        TransferControl.download(remoteFinalStdErr, new File(origFinalStdErr),
            GridPilot.getClassMgr().getGlobalFrame().getContentPane());
      }
      catch(Exception e){
        e.printStackTrace();
        error = "Exception during copying of output file(s) for job : " + job.getName() + "\n" +
        "\tCommand\t: " + remoteFinalStdErr + ": -> " + origFinalStdErr +"\n" +
        "\tException\t: " + e.getMessage();
        logFile.addMessage(error, e);
        ok = false;
      }      
      job.setStdErr(origFinalStdErr); 
    }
    else{
      ok = false;
    }

    return ok;
    
  }

  /**
   * - Downloads any files specified as local in the original jobDefinition
   *   from the remote temporary location.
   * - Deletes the remote temporary location.
   * - Cleans up temporary transformation and dataset if any such was written.
   * - Deletes remote jobDefinition.
   */
  public boolean postProcess(JobInfo job){
    boolean ok = true;
    if(hasLocalFiles(job)){
      String rDir = null;
      try{
        rDir = getRemoteDir(job.getName());
        if(!rDir.endsWith("/")){
          rDir += "/";
        }
        // Download files - not necessary, already done by updateStatus
        //copyToFinalDest(job);
        // Delete remote directory
        deleteRemoteDir(rDir);
      }
      catch(Exception e){
        ok = false;
        error = "ERROR: could not delete directory or remote files. ";
        logFile.addMessage(error, e);
      }
    }
    // Clean up temporary transformation, dataset and jobDefinition
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    String remoteID = null;
    try{
      remoteID = getRemoteJobdefID(job);
    }
    catch(IOException e){
      e.printStackTrace();
      error = "ERROR: could not get remote jobDefinition ID";
      logFile.addMessage(error, e);
      return false;
    }
    String datasetID = remoteMgr.getJobDefDatasetID(remoteID);
    String transformationName = remoteMgr.getDatasetTransformationName(datasetID);
    String transformationVersion = remoteMgr.getDatasetTransformationVersion(datasetID);
    String transformationID = remoteMgr.getTransformationID(transformationName, transformationVersion);
    try{
      deleteTaggedDataset(remoteMgr, datasetID);
      deleteTaggedTransformation(remoteMgr, transformationID);
    }
    catch(Exception e){
      ok = false;
      error = "Failed cleaning up remote dataset or transformation";
      logFile.addMessage(error, e);
    }
    try{
      remoteMgr.deleteJobDefinition(remoteID, false);
    }
    catch(Exception e){
      ok = false;
      error = "Failed cleaning up remote jobDefinition";
      logFile.addMessage(error, e);
    }
    return ok;
  }

  /**
   * Returns csStatus of remote jobDefinition record.
   */
  public String getFullStatus(JobInfo job){
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    String remoteID = null;
    try{
      remoteID = getRemoteJobdefID(job);
    }
    catch(IOException e){
      e.printStackTrace();
      error = "ERROR: could not get remote jobDefinition ID";
      logFile.addMessage(error, e);
      return null;
    }
    return remoteMgr.getJobDefValue(remoteID, "csStatus");
  }

  /**
   * If job is running: deletes stdout and stderr on gridftp server,
   * sets csStatus to 'requestStdout', waits for files to reappear,
   * then resets csStatus to its previous value.
   * If job is done, just reads finalStdout and finalStderr.
   */
  public String[] getCurrentOutputs(JobInfo job) throws IOException {
    return getCurrentOutputs(job, false);
  }
  
  /**
   * jobDone should be set true only when the job is done.
   * This is in order to allow local validation to be done.
   * copyToFinalDest is usually called by postProcess, but
   * here we don't keep a local run dir and thus no local
   * cache (only a tmp file), so we call copyToFinalDest
   * here instead.
   */
  public String[] getCurrentOutputs(JobInfo job, boolean jobDone)
      throws IOException{
    String status = getFullStatus(job);
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    String remoteID = null;
    try{
      remoteID = getRemoteJobdefID(job);
    }
    catch(IOException e){
      e.printStackTrace();
      error = "ERROR: could not get remote jobDefinition ID";
      logFile.addMessage(error, e);
      return null;
    }
    
    String [] res = new String[2];
    
    File tmpStdout = File.createTempFile(/*prefix*/"GridPilot-stdout", /*suffix*/"");
    File tmpStdErr = File.createTempFile(/*prefix*/"GridPilot-stderr", /*suffix*/"");
    int sleepT = 5000;
    int sleepN = 0;
    try{
      if(status.equals(PullJobsDaemon.STATUS_EXECUTED) || status.equals(PullJobsDaemon.STATUS_FAILED)){
        if(jobDone){
          copyToFinalDest(job);
        }
        String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
        String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
        // stdout
        if(finalStdOut.startsWith("file:")){
          res[0] = LocalStaticShellMgr.readFile(finalStdOut);
        }
        else if(Util.urlIsRemote(finalStdOut)){
          try{
            gsiftpFileTransfer.getFile(new GlobusURL(finalStdOut), tmpStdout.getParentFile(),
                GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar, null);
          }
          catch(Exception e){
            e.printStackTrace();
            throw new IOException("ERROR: could not download stdout. "+e.getMessage());
          }
          res[0] = LocalStaticShellMgr.readFile(tmpStdout.getCanonicalPath());
        }
        else{
          throw new IOException("Cannot access local files on remote system");
        }
        // stderr
        if(finalStdErr.startsWith("file:")){
          res[0] = LocalStaticShellMgr.readFile(finalStdErr);
        }
        else if(Util.urlIsRemote(finalStdErr)){
          boolean ok = true;
          try{
            gsiftpFileTransfer.getFile(new GlobusURL(finalStdErr), tmpStdErr.getParentFile(),
                GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar, null);
          }
          catch(Exception e){
            ok = false;
            e.printStackTrace();
            throw new IOException("ERROR: could not download stderr. "+e.getMessage());
          }
          if(ok){
            res[1] = LocalStaticShellMgr.readFile(tmpStdErr.getCanonicalPath());
          }
        }
        else{
          throw new IOException("Cannot access local files on remote system");
        }
      }
      else if(status.equals(PullJobsDaemon.STATUS_SUBMITTED)){
        String origFinalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
        String origFinalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
        String remoteFinalStdOut = remoteMgr.getStdOutFinalDest(remoteID);
        String remoteFinalStdErr = remoteMgr.getStdErrFinalDest(remoteID);      
        String oldStatus = remoteMgr.getJobDefValue(remoteID, "csStatus");
        String dlStdout = null;
        String dlStderr = null;
        if(Util.urlIsRemote(origFinalStdOut)){
          dlStdout = origFinalStdOut;
          dlStderr = origFinalStdErr;
        }
        else{
          dlStdout = remoteFinalStdOut;
          dlStderr = remoteFinalStdErr;
        }
        // Final stdout/stderr remote, delete from remote dir
        TransferControl.deleteFiles(new GlobusURL [] {new GlobusURL(dlStdout),
            new GlobusURL(dlStderr)});
        // Request new ones
        remoteMgr.setJobDefsField(new String [] {remoteID}, "csStatus", PullJobsDaemon.STATUS_REQUESTED_STDOUT);
        // Re-download
        Thread.sleep(sleepT);
        while(sleepN*sleepT<STDOUT_WAIT){
          ++sleepN;
          try{
            TransferControl.download(dlStdout, tmpStdout,
                GridPilot.getClassMgr().getGlobalFrame().getContentPane());
            TransferControl.download(dlStderr, tmpStdErr,
                GridPilot.getClassMgr().getGlobalFrame().getContentPane());
            break;
          }
          catch(Exception e){
            Debug.debug("Waiting for stdout...", 2);
          }
          // Wait
          Thread.sleep(sleepT);
        }
        remoteMgr.setJobDefsField(new String [] {remoteID}, "csStatus", oldStatus);
      }
    }
    catch(Exception ee){
      ee.printStackTrace();
      throw new IOException(ee.getMessage());
    }
    finally{
      try{
        if(!LocalStaticShellMgr.deleteFile(tmpStdout.getCanonicalPath()) ||
            !LocalStaticShellMgr.deleteFile(tmpStdErr.getCanonicalPath())){
           error = "WARNING: could not delete stdout or stderr temp file.";
           logFile.addMessage(error);
         }
      }
      catch(Exception eee){
        eee.printStackTrace();
      }
    }
    return res;
  }

  /**
   * Finds requested jobs, checks if provider is allowed to run them;
   * if so, sets permissions on input files accordingly.
   */
  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i)
      updateStatus((JobInfo) jobs.get(i));
  }

  private void updateStatus(JobInfo job){
    String remoteID = null;
    try{
      remoteID = getRemoteJobdefID(job);
    }
    catch (IOException e){
      e.printStackTrace();
    }
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    String csStatus = remoteMgr.getJobDefValue(remoteID, "csStatus");
    String remoteStatus = remoteMgr.getJobDefValue(remoteID, "status");
    Debug.debug("Updating status of job "+job.getName()+" : "+job.getJobStatus()+" : "+csStatus+" : "+remoteStatus, 2);
    if(csStatus==null || csStatus.equals("")){
      logFile.addMessage("ERROR: no csStatus for remote job "+remoteID+"--->"+job);
      return;
    }
    // TODO: only take action if the status has changed
    if(csStatus.startsWith(PullJobsDaemon.STATUS_REQUESTED)){
      String dn = remoteMgr.getJobDefValue(remoteID, "providerInfo");
      if(checkProvider(dn)){
        String rDir = null;
        rDir = getRemoteDir(job.getName());
        if(setJobDirPermission(rDir, dn)){
          Debug.debug("Setting job "+remoteID+" to prepared", 2);
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
          // This is only so we can identify this job later (match it to a local GPSS job). I.e. remote jobs are uniquely identified
          // by name+userInfo before being picked up and by name+providerInfo after being picked up
          dbPluginMgr.updateJobDefinition(job.getJobDefId(), new String [] {"providerInfo"}, new String [] {dn});
          remoteMgr.updateJobDefinition(remoteID, new String [] {"csStatus"}, new String [] {PullJobsDaemon.STATUS_PREPARED});
          job.setJobStatus("Assigned");
          job.setInternalStatus(ComputingSystem.STATUS_WAIT);
        }
        else{
          // back out if we cannot set permissions
          remoteMgr.updateJobDefinition(remoteID, new String [] {"csStatus"}, new String [] {PullJobsDaemon.STATUS_FAILED});
          job.setJobStatus(DBPluginMgr.getStatusName(DBPluginMgr.FAILED));
          job.setInternalStatus(ComputingSystem.STATUS_FAILED);
        }
      }
      else{
        // set job back to 'ready' if provider is not allowed to run it
        Debug.debug("Not allowing provider to run job "+remoteID+". Setting it back to 'Defined'.", 1);
        remoteMgr.updateJobDefinition(remoteID, new String [] {"csStatus"}, new String [] {PullJobsDaemon.STATUS_READY});
        job.setJobStatus(DBPluginMgr.getStatusName(DBPluginMgr.DEFINED));
        job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      }
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_EXECUTED)){
      String rDir = null;
      try{
        rDir = getRemoteDir(job.getName());
        Debug.debug("Checking uploaded files in "+rDir, 3);
        if(checkUploadedFiles(new GlobusURL(rDir), job)){
          job.setInternalStatus(ComputingSystem.STATUS_DONE);
        }
        else{
          job.setInternalStatus(ComputingSystem.STATUS_FAILED);
        }
        job.setJobStatus(csStatus);
        getCurrentOutputs(job, true);
      }
      catch(Exception e){
        error = "WARNING: could check uploaded files in directory "+rDir;
        logFile.addMessage(error, e);
        job.setJobStatus(csStatus);
        job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      }
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_PREPARED)){
      job.setJobStatus(csStatus);
      job.setInternalStatus(ComputingSystem.STATUS_WAIT);
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_DOWNLOADING)){
      job.setJobStatus(csStatus);
      job.setInternalStatus(ComputingSystem.STATUS_WAIT);
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_REQUESTED_KILLED)){
      job.setJobStatus(csStatus);
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_FAILED) ||
        remoteStatus.matches("(?i).*failed.*")){
      job.setJobStatus(csStatus);
      job.setInternalStatus(ComputingSystem.STATUS_FAILED);
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_SUBMITTED)){
      job.setJobStatus(csStatus);
      job.setInternalStatus(ComputingSystem.STATUS_WAIT);
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_RUNNING)){
      job.setJobStatus(csStatus);
      job.setInternalStatus(ComputingSystem.STATUS_RUNNING);
    }
    else if(csStatus.startsWith(PullJobsDaemon.STATUS_READY)){
      if(job.getJobStatus()!=null && job.getJobStatus().equals(PullJobsDaemon.STATUS_REQUESTED_KILLED)){
         job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      }
      job.setJobStatus(csStatus);
    }
    else{
      error = "WARNING: unrecognized status "+csStatus;
      Debug.debug(error, 1);
    }
  }
  
  /**
   * Check that the owner of the certificate with this DN is
   * allowed to run my jobs.
   */
  private boolean checkProvider(String dn){
    dn = dn.replaceFirst("^\"(.*)\"$", "$1");
    String subject = null;
    for(Iterator it=allowedSubjects.iterator(); it.hasNext();){
      subject = (String) it.next();
      subject = subject.replaceFirst("^\"(.*)\"$", "$1");
      Debug.debug("Matching provider DN "+dn+" with "+subject, 3);
      if(dn.equals(subject)){
        Debug.debug("Match!", 2);
        return true;
      }
    }
    return false;
  }

  /**
   * Sets this gridftp directory to be read/writable only by me and the
   * owner of the certificate of the given DN.
   * NOTICE: we assume that this is a GACL controlled directory.
   * @throws IOException 
   * @throws FTPException 
   */
  private boolean setJobDirPermission(String rDir, String dn){
    File tmpFile = null;
    try{
      tmpFile = File.createTempFile("GridPilot-", "");
      String [] gaclLines = null;
      String gaclEntries =
        "<entry><person><dn>"+dn+"</dn></person>\n"+
        "<allow>\n" +
        "<read/><list/><write/><admin/>\n"+
        "</allow>\n"+
        "</entry>\n"+
        "<entry><person><dn>"+user+"</dn></person>\n"+
        "<allow>\n" +
        "<read/><list/><write/><admin/>\n"+
        "</allow>\n"+
        "</entry>\n";
      try{
        gaclLines = Util.readURL(rDir+".gacl", tmpFile);
        if(gaclLines==null || gaclLines.equals("")){
          throw new IOException("empty or non-existing file");
        }
      }
      catch(Exception e){
        Debug.debug("Failed reading .gacl file, trying to write fresh one. "+e.getMessage(), 3);
      }
      if(gaclLines==null || gaclLines.length==0){
        String gaclString = "<?xml version=\"1.0\"?>" +
              "<gacl version=\"0.0.1\">"+gaclEntries+
              "</gacl>";
        LocalStaticShellMgr.writeFile(tmpFile.getCanonicalPath(), gaclString, false);
      }
      else if(!gaclLines[gaclLines.length-1].matches("(?i).*</gacl>\\S*")){
        throw new IOException(".gacl file not in GACL format: "+gaclLines[gaclLines.length-1]);
      }
      else{
        gaclLines[gaclLines.length-1] =
          gaclLines[gaclLines.length-1].replaceFirst("(?i)</gacl>",
              gaclEntries+"</gacl>");
        LocalStaticShellMgr.writeFile(tmpFile.getCanonicalPath(), Util.arrayToString(gaclLines, "\n"), false);
      }
      TransferControl.upload(tmpFile, rDir+".gacl",
          GridPilot.getClassMgr().getGlobalFrame().getContentPane());
    }
    catch(Exception e){
      error = "WARNING: could not set r/w permission for "+
      dn+" on directory "+rDir;
      logFile.addMessage(error);
      return false;
    }
    finally{
      tmpFile.delete();
    }
    return true;
  }
  
  private boolean checkUploadedFiles(GlobusURL dir, JobInfo job){
    try{
      
      DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
      String remoteID = null;
      try{
        remoteID = getRemoteJobdefID(job);
      }
      catch(IOException e){
        e.printStackTrace();
        error = "ERROR: could not get remote jobDefinition ID";
        logFile.addMessage(error, e);
        return false;
      }
      remoteMgr.getJobDefValue(remoteID, "csStatus");

      DBRecord jobDefinition = remoteMgr.getJobDefinition(remoteID);
      String [] outFileMapping = Util.splitUrls((String) jobDefinition.getValue("outFileMapping"));
      String [] checkFiles = new String [outFileMapping.length/2+2];
      int j = 0;
      for(int i=0; i<outFileMapping.length; ++i){
        if((i+1)%2==0){
          checkFiles[j] = outFileMapping[i];
          ++j;
        }
      }
      checkFiles[j] = (String) jobDefinition.getValue("stdoutDest");
      ++j;
      checkFiles[j] = (String) jobDefinition.getValue("stderrDest");
      Vector fileVector = null;
      String urlDir = null;
      String oldUrlDir = null;
      boolean ok = false;
      for(int i=0; i<checkFiles.length; ++i){
        ok = false;
        // don't list the same dir two times after each other
        urlDir = checkFiles[i].replaceFirst("(.*/)[^/]+", "$1");
        if(oldUrlDir==null || !urlDir.equals(oldUrlDir)){
          Debug.debug("Checking dir "+urlDir, 2);
          fileVector = gsiftpFileTransfer.list(new GlobusURL(urlDir), null, null);
          oldUrlDir = urlDir;
        }
        if(fileVector==null || fileVector.size()<1){
          throw new IOException("File "+checkFiles[i]+" not found.");
        }
        else{
          String file = checkFiles[i].replaceFirst(".*/([^/]+)", "$1");
          String line = null;
          for(Iterator it=fileVector.iterator(); it.hasNext();){
            line = (String) it.next();
            Debug.debug("Comparing: "+line+" <-> "+file, 3);
            if(line.matches("^"+file+"\\s+.*")){
              ok = true;
              break;
            }
          }
        }
        if(!ok){
          logFile.addMessage("Uploaded file "+checkFiles[i]+" not present.");
          return false;
        }
      }
    }
    catch(Exception e){
      logFile.addMessage("Checking uploaded file failed.", e);
      return false;
    }
    return true;
  }

  public String[] getScripts(JobInfo job){
    // Nothing to do: we cannot get the scripts produced by the
    // remote GridPilot. - we could introduce another csStatus trigger
    // to have the remote GridPilot call its getScripts and upload the
    // scripts to the remote gridftp directory. But I don't really think
    // it's worth it...
    return null;
  }

  public String getUserInfo(String csName){
    return user;
  }

  public void setupRuntimeEnvironments(String csName){
    setupRuntimeEnvironments();
  }
  
  /**
   * Simply copies over runtime environment records from the defined
   * "remote database" to the defined "runtime databases".
   */
  public void setupRuntimeEnvironments(){
    String certificate = null;
    DBResult rtes = null;
    DBPluginMgr remoteDBMgr = null;
    DBPluginMgr localDBMgr = null;
    DBRecord rte = null;
    try{
      remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    }
    catch(Exception e){
      error = "Could not load remote runtime DB "+remoteDB+"."+e.getMessage();
      Debug.debug(error, 1);
      return;
    }
    rtes = remoteDBMgr.getRuntimeEnvironments();
    // Write records in the 'local' runtime DBs,
    // clearing the certificate.
    for(int ii=0; ii<localRuntimeDBs.length; ++ii){
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(localRuntimeDBs[ii]);
      }
      catch(Exception e){
        error = "Could not load local runtime DB "+localRuntimeDBs[ii]+"."+e.getMessage();
        Debug.debug(error, 1);
      }
      for(int i=0; i<rtes.values.length; ++i){
        rte = rtes.getRow(i);
        // Only copy records put there by pull clients.
        certificate = (String) rte.getValue("certificate");
        if(certificate==null || certificate.equals("")){
          continue;
        }
        try{
          // Clear the certificate,
          // in order to avoid confusion and have the record delete on exit.
          rte.setValue("certificate", "");
          localDBMgr.createRuntimeEnv(rte.fields, rte.values);
        }
        catch(Exception e1){
          e1.printStackTrace();
          error = "WARNING: could not create runtime environment " +
          rtes.getRow(i).getValue("name")+" in database "+localDBMgr.getDBName();
          Debug.debug(error, 1);
          continue;
        }
        if(ii==0){
          try{
            finalRuntimesLocal.add(localRuntimeDBs[ii]);
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
  }

  public void exit(){
    cleanupRuntimeEnvironments();
  }
  
  /**
   * Clean up runtime environment records copied from "remote database".
   */
  public void cleanupRuntimeEnvironments(){
    String runtimeName = null;
    String certificate = null;
    String id = "-1";
    boolean ok = true;
    DBPluginMgr localDBMgr = null;
    for(int i=0; i<localRuntimeDBs.length; ++i){
      localDBMgr = null;
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
            localRuntimeDBs[i]);
      }
      catch(Exception e){
        error = "Could not load local runtime DB "+localRuntimeDBs[i]+"."+e.getMessage();
        Debug.debug(error, 1);
      }
      if(localDBMgr!=null){
        for(Iterator it=finalRuntimesLocal.iterator(); it.hasNext();){
          ok = true;
          runtimeName = (String) it.next();
          id = localDBMgr.getRuntimeEnvironmentID(runtimeName, csName);
          if(!id.equals("-1")){
            // Don't delete records with a non-empty certificate.
            // These were put there by pull clients.
            certificate = (String) localDBMgr.getRuntimeEnvironment(id).getValue("certificate");
            if(certificate!=null && !certificate.equals("")){
              continue;
            }
            ok = localDBMgr.deleteRuntimeEnvironment(id);
          }
          else{
            ok = false;
          }
          if(!ok){
            error = "WARNING: could not delete runtime environment " +
            runtimeName+" from database "+localDBMgr.getDBName();
            Debug.debug(error, 1);
          }
        }
      }
    }
  }

  public String getError(String csName){
    return error;
  }

}
