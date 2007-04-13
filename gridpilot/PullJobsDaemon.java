package gridpilot;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.Timer;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;

public class PullJobsDaemon{
  
  private DBPluginMgr dbPluginMgr = null;
  private String csName = null;
  private StatusBar statusBar = null;
  private LogFile logFile = null;
  private String userInfo = null;
  private int maxPullRun = 1;
  private String idField = null;
  private String cacheDir = null;
  // map of JobInfo -> (Vector of TransferInfos)
  private HashMap runningTransfers = new HashMap();
  
  private static int WAIT_TRIES = 20;
  private static int WAIT_SLEEP = 10000;
  private static boolean CLEANUP_CACHE_ON_EXIT = true;
  
  private static String STATUS_READY = "ready";
  private static String STATUS_REQUESTED = "requested";
  private static String STATUS_DOWNLOADING = "downloading";
  private static String STATUS_SUBMITTED = "submitted";
  private static String STATUS_REQUESTED_KILLED = "requestKill";
  private static String STATUS_REQUESTED_STDOUT = "requestStdout";
  private static String STATUS_FAILED = "failed";
  private static String STATUS_EXECUTED = "executed";
  
  private Timer timerPull = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      pullJob();
    }
  });

  private Timer timerRun = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      runJob();
    }
  });

  private Timer timerFinish = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      finishJob();
    }
  });

  public PullJobsDaemon(String dbName, String _csName, StatusBar _statusBar){
    csName = _csName;
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    statusBar = _statusBar;
    logFile = GridPilot.getClassMgr().getLogFile();
    idField = Util.getIdentifierField(dbPluginMgr.getDBName(), "jobDefinition");
    // The grid certificate subject, used by the pull job manager to encrypt files
    userInfo = getUserInfo();
    String maxPullRunString = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Max pulled running jobs");
    if(maxPullRunString!=null){
      try{
        maxPullRun = Integer.parseInt(maxPullRunString);
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
    }
    cacheDir = GridPilot.getClassMgr().getConfigFile().getValue(csName, "pull cache directory");
    if(cacheDir!=null){
      try{
        File tmpFile = File.createTempFile(/*prefix*/"GridPilot-pull-cache", /*suffix*/"");
        String tmpDir = tmpFile.getAbsolutePath();
        tmpFile.delete();
        LocalStaticShellMgr.mkdirs(tmpDir);
        if(CLEANUP_CACHE_ON_EXIT){
          // hack to have the diretory deleted on exit
          GridPilot.tmpConfFile.put(tmpDir, new File(tmpDir));
        }
        cacheDir = tmpDir;
      }
      catch(IOException e){
        e.printStackTrace();
      }
    }
    else{
      cacheDir = Util.clearTildeLocally(Util.clearFile(cacheDir));
    }
  }
  
  public String getUserInfo(){
    String user = null;
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
    }
    catch(Exception ioe){
      String error = "Exception during getUserInfo\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    if(user==null){
      logFile.addMessage("ERROR: Grid user null, cannot initiate job pulling");
    }
    return user;
  }

  
  /**
   * Finds eligible jobs, chooses one, requests it, waits for it to
   * be available, then starts downloading input files.
   * @return the number of started jobs (0 or 1).
   */
  public boolean pullJob(){
    
    boolean timerFinishWasRunning = timerFinish.isRunning();
    
    // If the finish timer was not running, start it
    // and give some time to finish any done jobs.
    if(!timerFinishWasRunning){
      timerFinish.start();
      for(int i=0; i<7; ++i){
        if(checkFreeResources()){
          break;
        }
        Debug.debug("No free resources on "+csName+". waiting...", 2);
        try{
          Thread.sleep(5000);
        }
        catch(InterruptedException e){
          return false;
        }
      }
      timerFinish.stop();
    }
    
    if(!checkFreeResources()){
      Debug.debug("No free resources on "+csName+". exiting.", 2);
      return false;
    }

    DBRecord [] candidates = findEligibleJobs();
    Vector okJobs = new Vector();
    for(int i=0; i<candidates.length; ++i){
      if(checkRequirements(candidates[i])){
        okJobs.add(candidates[i]);
      }
    }
    DBRecord [] okCandidates = new DBRecord [okJobs.size()];
    for(int i=0; i<okCandidates.length; ++i){
      okCandidates[i] = (DBRecord) okJobs.get(i);
    }
    okCandidates = rankJobs(okCandidates);
    for(int i=0; i<okCandidates.length; ++i){
      Debug.debug("Requesting job  "+okCandidates[i].getValue(
          Util.getNameField(dbPluginMgr.getDBName(), "jobDefinition")), 2);
      try{
        // Request the job
        statusBar.setLabel("Requesting job");
        if(requestJob(okCandidates[i])){
          // Wait for it to be ready on the server
          if(waitForJob(okCandidates[i])){
            // Start downloading input files
            String jobDefID = (String) okCandidates[i].getValue(idField);
            JobInfo job = new JobInfo(
                jobDefID,
                dbPluginMgr.getJobDefName(jobDefID),
                csName,
                dbPluginMgr.getDBName()
                );
            try{
              startDownloadInputs(job);
            }
            catch(Exception e){
              throw e;
            }
            try{
              dbPluginMgr.updateJobDefinition(jobDefID, new String [] {"csStatus"},
                  new String [] {STATUS_DOWNLOADING});
             }
            catch(Exception e){
              logFile.addMessage("WARNING: could not set csStatus to downloading for job "+jobDefID, e);
            }
            return true;
          }
        }
        Debug.debug("Failed requesting job, forgetting "+okCandidates[i], 2);
        unRequestJob(okCandidates[i]);
      }
      catch(Exception e){
        logFile.addMessage("Failed requesting job "+i, e);
        try{
          unRequestJob(okCandidates[i]);
        }
        catch(Exception ee){
          ee.printStackTrace();
        }
      }
    }
    return false;
  }
  
  /**
   * Called when either the spinner valuer is changed or combo box "sec/min" is changed
   */
  public void setDelay(int delay){
      timerPull.setDelay(delay);
      timerRun.setDelay(delay/3);
      timerFinish.setDelay(delay/3);
  }

  /**
   * Scans the job database for runnable jobs
   * @return array of job records
   */
  private DBRecord [] findEligibleJobs(){
    String [] allFields = null;
    try{
      allFields = dbPluginMgr.getFieldnames("jobDefinition");
    }
    catch(Exception e){
      Debug.debug("Skipping DB "+dbPluginMgr.getDBName(), 1);
      e.printStackTrace();
      return null;
    }
    // We only reqest jobs that are "Defined"
    String [] statusList = new String [] {DBPluginMgr.getStatusName(DBPluginMgr.DEFINED)};
    DBResult allJobDefinitions = dbPluginMgr.getJobDefinitions("-1", allFields, statusList);
    Vector eligibleJobs = new Vector(); //DBRecords
    for(int i=0; i<allJobDefinitions.values.length; ++i){
      DBRecord jobRecord = allJobDefinitions.getRow(i);
      if(checkRequirements(jobRecord)){
        eligibleJobs.add(jobRecord);
      }
    }
    DBRecord [] ret = new DBRecord[eligibleJobs.size()];
    for(int i=0; i<ret.length; ++i){
      ret[i] = (DBRecord) eligibleJobs.get(i);
    }
    return ret;
  }
  
  private boolean checkRequirements(DBRecord jobRecord){
    Debug.debug("Checking job  "+jobRecord.getValue(
        Util.getNameField(dbPluginMgr.getDBName(), "jobDefinition")), 2);
    // We only consider jobs that have been explicitly submitted to GPSS
    String csName = jobRecord.getValue("computingSystem").toString();
    if(csName!=null || !csName.equalsIgnoreCase("gpss")){
      return false;
    }
    String userInfo = (String) jobRecord.getValue("userInfo");
    // If userInfo is set, the job is already taken.
    if(userInfo!=null && !userInfo.equals("")){
      //user = GridPilot.getClassMgr().getCSPluginMgr().getUserInfo(csName);
      return false;
    }
    // We only consider jobs that have csStatus ready or ready:n, n<maxRetries
    String status = (String) jobRecord.getValue("csStatus");
    if(status==null || !status.startsWith(STATUS_READY)){
      return false;
    }
    if(jobRecord!=null){
      int retries = 0;
      int index = status.indexOf(":");
      if(index>0){
        String retriesString = status.substring(index+1);
        retries = Integer.parseInt(retriesString);
        if(retries>GridPilot.maxPullRerun){
          return false;
        }
      }
    }
    //TODO: extend jobDefinition schema according to 3.1 of KnowARC virtualization proposal (T1.5)
    // and include corresponding checks. E.g. of allowedVOs and runtimeEnvironments.
    return true;
  }

  private DBRecord [] rankJobs(DBRecord [] jobs){
    // TODO
    return jobs;
  }

  /**
   * Sets the status of the job to 'requested' and writes the certificate
   * subject in the 'providerInfo' field of the jobDefinition on the remote
   * database.
   * @param jobRecord
   * @return
   */
  private boolean requestJob(DBRecord jobRecord){
    String jobDefID = (String) jobRecord.getValue(idField);
    boolean ok = false;
    try{
      ok = dbPluginMgr.updateJobDefinition(jobDefID, new String [] {"csStatus", "providerInfo"},
          new String [] {STATUS_REQUESTED, userInfo});
     }
    catch(Exception e){
      logFile.addMessage("ERROR: could not request job "+jobDefID, e);
      ok = false;
    }
    return ok;
  }

  /**
   * Sets the status of the job to 'ready:n' and clears the certificate
   * subject in the 'providerInfo' field of the jobDefinition on the remote
   * database.
   * @param jobRecord
   * @return true if successful, false otherwise.
   */
  private boolean unRequestJob(DBRecord jobRecord){
    boolean ok = false;
    String jobDefID = "-1";
    try{
      jobDefID = (String) jobRecord.getValue(idField);
      String status = (String) jobRecord.getValue("csStatus");
      int retries = 0;
      int index = status.indexOf(":");
      if(index>0){
        String retriesString = status.substring(index+1);
        retries = Integer.parseInt(retriesString);
        ++retries;
      }
      ok = dbPluginMgr.updateJobDefinition(jobDefID, new String [] {"csStatus", "providerInfo"},
          new String [] {STATUS_READY+":"+retries, ""});
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not unrequest job "+jobDefID, e);
      ok = false;
    }
    return ok;
  }

  /**
   * Sets the status of the job to 'failed:n' in the remote
   * database.
   * @param jobRecord
   * @return true if successful, false otherwise.
   */
  /*private void failUploadJob(JobInfo job){
    String jobDefID = job.getJobDefId();
    try{
      String csStatus = (String) dbPluginMgr.getJobDefinition(jobDefID).getValue("csStatus");
      int retries = 0;
      int index = csStatus.indexOf(":");
      if(index>0){
        String retriesString = csStatus.substring(index+1);
        retries = Integer.parseInt(retriesString);
        ++retries;
      }
      // After 3 attempts to upload output files we give up
      if(retries>2){
        dbPluginMgr.updateJobDefinition(jobDefID, new String [] {"csStatus"},
            new String [] {STATUS_FAILED+": could not upload outputs of job."});
        // Now tag the job as failed internally and
        // have JobStatusUpdateControl set the DB status
        job.setInternalStatus(ComputingSystem.STATUS_FAILED);
      }
      else{
        dbPluginMgr.updateJobDefinition(jobDefID, new String [] {"csStatus"},
            new String [] {STATUS_EXECUTED+":"+retries});
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not tag job as failed; "+job, e);
    }
  }*/

  private boolean checkFreeResources(){
    int runningJobs = -1;
    int preparingJobs = -1;
    try{
      runningJobs = GridPilot.getClassMgr().getShellMgr(csName).getJobsNumber();
    }
    catch(Exception e){
      logFile.addMessage("Error: Could not get running jobs.", e);
      e.printStackTrace();
      return false;
    }
    try{
      preparingJobs = runningTransfers.size();
    }
    catch(Exception e){
      logFile.addMessage("Error: Could not get running jobs.", e);
      e.printStackTrace();
      return false;
    }
    return runningJobs>-1 && preparingJobs>-1 && runningJobs+preparingJobs<maxPullRun;
  }

  /**
   * Waits until the status of the job has been set to 'Prepared'.
   * @param jobRecord DB record
   * @return false if this is not achieved before a timeout
   * of WAIT_FOR_JOB_READY milliseconds. true otherwise.
   */
  private boolean waitForJob(DBRecord jobRecord){
    String jobDefID = null;
    String status = null;
    for(int i=0; i<WAIT_TRIES; ++i){
      try{
        jobDefID = (String) jobRecord.getValue(idField);
        status = dbPluginMgr.getJobDefValue(jobDefID, "csStatus");
        if(status.equalsIgnoreCase("Prepared")){
          Debug.debug("Job "+jobDefID+" prepared",  2);
          return true;
        }
        Debug.debug("Waiting for job to be prepared...",  2);
        statusBar.setLabel("Waiting for requested job...");
        Thread.sleep(WAIT_SLEEP);
      }
      catch(Exception e){
        e.printStackTrace();
        break;
      }
    }
    return false;
  }

  /**
   * Start downloading all non-local files to session directory, so the
   * job can be run directly by the Fork plugin. Update the
   * downloadFiles of the JobInfo correspondingly.
   * @param job description
   */
  private void startDownloadInputs(JobInfo job) throws Exception{
    
    statusBar.setLabel("Downloading input files for pulled job...");
    
    String transID = dbPluginMgr.getJobDefTransformationID(job.getJobDefId());
    Debug.debug("Getting input files for transformation " + transID, 2);
    String [] transInputFiles = dbPluginMgr.getTransformationInputs(transID);
    Debug.debug("Getting input files for job " + job.getName(), 2);
    String [] jobInputFiles = dbPluginMgr.getJobDefInputFiles(job.getJobDefId());
    String [] inputFiles = new String [transInputFiles.length+jobInputFiles.length];
    for(int i=0; i<transInputFiles.length; ++i){
      inputFiles[i] = transInputFiles[i];
    }
    for(int i=0; i<jobInputFiles.length; ++i){
      inputFiles[i+transInputFiles.length] = jobInputFiles[i];
    }
    Vector downloadVector = new Vector();
    Vector transferVector = new Vector();
    GlobusURL srcUrl = null;
    File destFile = null;
    String destFileName = null;
    GlobusURL destUrl = null;
    int lastSlash = -1;
    String fileName = null;
    for(int i=0; i<inputFiles.length; ++i){
      // Get the remote input file URLs.
      if(Util.urlIsRemote(inputFiles[i])){
        try{
          srcUrl = new GlobusURL(inputFiles[i]);
          lastSlash = inputFiles[i].lastIndexOf("/");
          fileName = inputFiles[i].substring(lastSlash + 1);
          destFile = File.createTempFile(
              (new File(cacheDir, fileName)).getCanonicalPath(), "");
          destFileName = destFile.getCanonicalPath();
          downloadVector.add(destFileName/*inputFiles[i]*/);
          destFileName = destFileName.replaceFirst("^/", "");
          destFileName = "file:////"+destFileName;
          destUrl = new GlobusURL(destFileName);
          destFile.delete();
          TransferInfo transfer = new TransferInfo(srcUrl, destUrl);
          transferVector.add(transfer);
        }
        catch(Exception e){
          logFile.addMessage("ERROR: could not get input file "+inputFiles[i]+
              ".", e);
          throw e;
        }
      }
    }
    // Queue the transfers
    if(transferVector.size()>0){
      try{
        GridPilot.getClassMgr().getTransferControl().queue(transferVector);
      }
      catch(Exception e){
        logFile.addMessage("ERROR: could not queue input file transfers.", e);
        throw e;
      }
      String [] downloadFiles = new String[downloadVector.size()];
      for(int i=0; i<downloadVector.size(); ++i){
        if(downloadVector.get(i)!=null){
          downloadFiles[i] = (String) downloadVector.get(i);
        }
      }
      // This is to have the CS copy these files over with the shell
      // and ignore JobDefinition.inputFiles.
      // TODO: consider having other CSs (LSF) than FORK take heed of this.
      job.setDownloadFiles(downloadFiles);
    }
    runningTransfers.put(job, transferVector);
  }
  
  /**
   * Finds job ready to be run and starts it.
   * @return true if successful, false otherwise.
   */
  private void runJob(){
    boolean ok = true;
    JobInfo job = null;
    Vector toSubmitJobs = null;
    String jobDefID = null;
    for(Iterator it=runningTransfers.keySet().iterator(); it.hasNext();){
      toSubmitJobs = new Vector();
      job = (JobInfo) it.next();
      Vector transfers = (Vector) runningTransfers.get(job);
      ok = true;
      for(Iterator itt=transfers.iterator(); itt.hasNext();){
        TransferInfo transfer = (TransferInfo) itt.next();
        if(TransferControl.isRunning(transfer)){
          ok = false;
          break;
        }
      }
      if(ok){
        statusBar.setLabel("Submitting pulled job");
        try{
          // Submitting implies the creation of a new JobInfo object, now representing
          // a running job and a record on the job monitor. This can be accessed with
          // JobMgr.getJob().
          jobDefID = (String) job.getValue(idField);
          toSubmitJobs.add(dbPluginMgr.getJobDefinition(jobDefID));
          GridPilot.getClassMgr().getSubmissionControl().submitJobDefinitions(toSubmitJobs,
              csName, dbPluginMgr);
          dbPluginMgr.updateJobDefinition((String) job.getValue(idField), new String [] {"csStatus"},
              new String [] {STATUS_SUBMITTED});
          break;
        }
        catch(Exception e){
          logFile.addMessage("ERROR: failed starting job "+job, e);
          // Running the job failed, flag it as failed - i.e. don't try to run it again.
          dbPluginMgr.updateJobDefinition((String) job.getValue(idField), new String [] {"csStatus"},
              new String [] {STATUS_FAILED+": "+"failed starting job. "+e.getMessage()});
        }
      }
    }
    if(ok && job!=null && jobDefID!=null){
      // Carry over the setDownloadFiles.
      JobMgr.getJob(jobDefID).setDownloadFiles(job.getDownloadFiles());
      runningTransfers.remove(job);
    }
  }

  /**
   * Detects a finished job and calls finishJob(JobInfo).
   * @return true if successful, false otherwise.
   */
  private boolean finishJob(){
    
    JobInfo [] doneJobs = findDoneJobs();
    for(int i=0; i<doneJobs.length; ++i){
      try{
        if(finishJob(doneJobs[i])){
          return true;
        }
      }
      catch(Exception e){
        logFile.addMessage("ERROR: could not finish job "+doneJobs[i], e);
        e.printStackTrace();
      }
    }
    return false;
  }
  
  /**
   * Checks the status of jobs as set by the CS plugin.
   * Checks the csStatus field of the jobDefinition DB record:
   * if it is 'requestKill' the job is killed and set to failed,
   * if it is 'requestStdout' the stdout/stderris is written in the run directory.
   * @return array of finished jobs
   */
  private JobInfo [] findDoneJobs(){
    Vector allRunningJobs = GridPilot.getClassMgr().getSubmittedJobs();
    Vector doneJobs = new Vector(); //DBRecords
    Enumeration en = allRunningJobs.elements();
    JobInfo job = null;
    DBRecord jobRecord = null;
    while(en.hasMoreElements()){
      job = (JobInfo) en.nextElement();
      String jobDefID = job.getJobDefId();
      jobRecord = dbPluginMgr.getJobDefinition(jobDefID);
      if(!job.getCSName().equalsIgnoreCase("gpss") ||
          !jobRecord.getValue("providerInfo").equals(userInfo)){
        continue;
      }
      // Add jobs that have been set as done by JobStatusUpdateControl,
      // i.e. normal GridPilot running.
      if(job.getInternalStatus()==ComputingSystem.STATUS_DONE ||
         job.getInternalStatus()==ComputingSystem.STATUS_FAILED /*||
         job.getInternalStatus()==ComputingSystem.STATUS_ERROR*/){
        doneJobs.add(job);
      }
      if(jobRecord.getValue("csStatus").equals(STATUS_REQUESTED_KILLED)){
        Vector killJobs = new Vector();
        // Temporarily change the CS name from GPSS to the local one,
        // so the killing is actually done.
        job.setCSName(csName);
        killJobs.add(job);
        try{
          GridPilot.getClassMgr().getCSPluginMgr().killJobs(killJobs);
        }
        catch(Exception e){
          e.printStackTrace();
        }
        job.setCSName("GPSS");
        // Don't wait for any confirmation, just assume the job has been killed
        dbPluginMgr.updateJobDefinition((String) job.getValue(idField), new String [] {"csStatus"},
            new String [] {STATUS_FAILED+": "+"job killed. "});
      }
      else if(jobRecord.getValue("csStatus").equals(STATUS_REQUESTED_STDOUT)){
        // Simply copy stdout/stderr to their final destinations.
        // These will be on a gridftp server (if they were not originally, GPSS
        // will have modified them temporarily to be).
        uploadStdoutErr(job);
      }
    }
    JobInfo [] ret = new JobInfo[doneJobs.size()];
    for(int i=0; i<ret.length; ++i){
      ret[i] = (JobInfo) doneJobs.get(i);
    }
    return ret;
  }
  

  /**
   * copy temp stdout -> finalStdout, temp stderr -> finalStdErr
   */
  private boolean uploadStdoutErr(JobInfo job){
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    boolean ok = false;
    ShellMgr shellMgr = null;
    try {
      shellMgr = GridPilot.getClassMgr().getShellMgr(csName);
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      if(Util.copyOutputFile(job.getStdOut(), finalStdOut, shellMgr, "", logFile)){
        ok = true;
      }
    }
    ok = false;   
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      if(Util.copyOutputFile(job.getStdErr(), finalStdErr, shellMgr, "", logFile)){
        ok = true;
      }
    }
    return ok;
  }
  
  /**
   * Uploads output files and set the job status as 'executed' or 'failed'.
   * @param jobRecord record
   * @return true if successful, false otherwise.
   */
  private boolean finishJob(JobInfo job){
    statusBar.setLabel("Pulled job done");
    try{
      if(!dbPluginMgr.updateJobDefinition(job.getJobDefId(), new String [] {"csStatus"},
          new String [] {STATUS_EXECUTED})){
        throw new Exception();
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not update status of job "+job , e);
      return false;
    }
    return true;
  }
  
  public void startPulling(){
    Debug.debug("Starting job pulling", 2);
    timerPull.start();
    timerFinish.start();
    timerRun.start();
  }

  public void stopPulling(){
    Debug.debug("Stopping job pulling", 2);
    timerPull.stop();
    timerFinish.stop();
    timerRun.stop();
  }
  
  public void exit(){
    try{
      // Cancel all transfers
      TransferControl.cancel(new Vector(runningTransfers.values()));
      if(CLEANUP_CACHE_ON_EXIT){
        // Delete everything in the cache directory
        String [] files = LocalStaticShellMgr.listFiles(cacheDir);
        for(int i=0; i<files.length; ++i){
          if(LocalStaticShellMgr.isDirectory(files[i])){
            LocalStaticShellMgr.deleteDir(new File(files[i]));
          }
          else{
            LocalStaticShellMgr.deleteFile(files[i]);
          }
        }
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }      
  }

}
