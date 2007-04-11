package gridpilot;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.util.Vector;

import javax.swing.Timer;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSCredential;

public class PullJobsDaemon{
  
  private DBPluginMgr dbPluginMgr = null;
  private CSPluginMgr csPluginMgr = null;
  private String csName = null;
  private StatusBar statusBar = null;
  private LogFile logFile = null;
  private String userInfo = null;
  private int maxPullRun = 1;
  private String idField = null;
  private String cacheDir = null;
  
  private static int WAIT_TRIES = 20;
  private static int WAIT_SLEEP = 10000;
  
  private Timer timerPull = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e){
      pullJob();
    }
  });

  private Timer timerFinish = new Timer(0, new ActionListener (){
    public void actionPerformed(ActionEvent e){
      finishJob();
    }
  });

  public PullJobsDaemon(String dbName, String _csName, StatusBar _statusBar){
    csName = _csName;
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    csPluginMgr = GridPilot.getClassMgr().getCSPluginMgr();
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
        // hack to have the diretory deleted on exit
        GridPilot.tmpConfFile.put(tmpDir, new File(tmpDir));
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
   * be available, then starts it.
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
        if(requestJob(okCandidates[i])){
          if(waitForJob(okCandidates[i])){
            String jobDefID = (String) okCandidates[i].getValue(idField);
            JobInfo job = new JobInfo(
                jobDefID,
                dbPluginMgr.getJobDefName(jobDefID),
                csName,
                dbPluginMgr.getDBName()
                );
            if(downloadInputs(job)!=null){
              return runJob(okCandidates[i]);
            }
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
  }

  /**
   * Scans the job database for runnable jobs
   * @return array of job records
   */
  private DBRecord [] findEligibleJobs(){
    String [] allFields = null;
    try{
      allFields = dbPluginMgr.getFieldnames("jobDefinition");//dbPluginMgr.getDBDefFields("jobDefinition");
    }
    catch(Exception e){
      Debug.debug("Skipping DB "+dbPluginMgr.getDBName(), 1);
      e.printStackTrace();
      return null;
    }
    // Well, we only reqest jobs that are "Defined"
    String [] statusList = new String [] {"Defined"};
    DBResult allJobDefinitions = dbPluginMgr.getJobDefinitions("-1", allFields, statusList);
    Vector eligibleJobs = new Vector(); //DBRecords
    for(int i=0; i<allJobDefinitions.values.length; ++i){
      DBRecord jobRecord = new DBRecord(allJobDefinitions.fields,
          allJobDefinitions.values[i]);
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
    // We only consider jobs that have csStatus unset or Ready:n, n<maxRetries
    String status = (String) jobRecord.getValue("csStatus");
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
          new String [] {"requested", userInfo});
     }
    catch(Exception e){
      logFile.addMessage("ERROR: could not request job "+jobDefID, e);
      ok = false;
    }
    return ok;
  }

  /**
   * Sets the status of the job to 'Ready:n' and clears the certificate
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
          new String [] {"Ready:"+retries, ""});
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not unrequest job "+jobDefID, e);
      ok = false;
    }
    return ok;
  }

  private boolean checkFreeResources(){
    int runningJobs = -1;
    try{
      runningJobs = GridPilot.getClassMgr().getShellMgr(csName).getJobsNumber();
    }
    catch(Exception e){
      logFile.addMessage("Error: Could not get running jobs.", e);
      e.printStackTrace();
      return false;
    }
    return runningJobs>-1 && runningJobs<maxPullRun;
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
   * Download all non-local files to session directory, so the
   * job can be run directly by the Fork plugin. Update the
   * inputFiles of the JobInfo correspondingly.
   * @param job description
   * @return updated job description
   */
  private JobInfo downloadInputs(JobInfo job) throws Exception{
    
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
    GlobusURL srcUrl = null;
    GlobusURL destUrl = null;
    int lastSlash = -1;
    String fileName = null;
    for(int i=0; i<inputFiles.length; ++i){
      // Get the remote input file URLs.
      if(!inputFiles[i].matches("^file:/*[^/]+.*") &&
          inputFiles[i].matches("^[a-z]+:/*[^/]+.*")){
        try{
          srcUrl = new GlobusURL(inputFiles[i]);
          lastSlash = inputFiles[i].lastIndexOf("/");
          fileName = inputFiles[i].substring(lastSlash + 1);
          destUrl = new GlobusURL((new File(cacheDir, fileName)).getCanonicalPath());
          TransferInfo transfer = new TransferInfo(srcUrl, destUrl);
          downloadVector.add(inputFiles[i]);
        }
        catch(Exception ioe){
          logFile.addMessage("WARNING: GridPilot could not get input file "+inputFiles[i]+
              ".", ioe);
          ioe.printStackTrace();
        }
      }
    }
    String [] downloadFiles = new String[downloadVector.size()];
    for(int i=0; i<downloadVector.size(); ++i){
      if(downloadVector.get(i)!=null){
        downloadFiles[i] = (String) downloadVector.get(i);
      }
    }
    job.setDownloadFiles(downloadFiles);
    return job;
  }
  
  private boolean uploadOutputs(DBRecord jobRecord){
    // TODO
    return false;
  }
  
  private boolean runJob(DBRecord jobRecord){
    
    String jobDefID = (String) jobRecord.getValue(idField);
    JobInfo job = new JobInfo(
        jobDefID,
        dbPluginMgr.getJobDefName(jobDefID),
        csName,
        dbPluginMgr.getDBName()
        );

    
    
    
    // TODO
    return false;
  }

  /**
   * Detects a finished job and calls finishJob(JobInfo).
   * @return true if successful, false otherwise.
   */
  private boolean finishJob(){
    // TODO
    return false;
  }
  
  /**
   * Uploads output files and set the job status as 'executed'.
   * @param jobRecord record
   * @return true if successful, false otherwise.
   */
  private boolean finishJob(DBRecord jobRecord){
    try{
      if(!uploadOutputs(jobRecord)){
        return false;
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not upload outputs of job "+jobRecord , e);
      return false;
    }
    try{
      String jobDefID = (String) jobRecord.getValue(idField);
      if(dbPluginMgr.updateJobDefinition(jobDefID, new String [] {"csStatus"},
          new String [] {"executed"})){
        return false;
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not upload outputs of job "+jobRecord , e);
      return false;
    }
    return true;
  }
  
  public void startPulling(){
    Debug.debug("Starting job pulling", 2);
    timerPull.start();
    timerFinish.start();
  }

  public void stopPulling(){
    Debug.debug("Stopping job pulling", 2);
    timerPull.stop();
    timerFinish.stop();
  }

}
