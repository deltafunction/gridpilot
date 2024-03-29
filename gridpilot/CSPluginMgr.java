package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.MyLinkedHashSet;
import gridfactory.common.ResThread;
import gridfactory.common.Shell;
import gridfactory.common.StatusBar;
import gridfactory.common.jobrun.VirtualMachine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeoutException;

import org.safehaus.uuid.UUIDGenerator;

/**
 * The purpose of this class is to protect GridPilot from plugin errors, exceptions, abnormal behaviour. <p>
 *
 * The class <code>CSPluginMgr</code> defines the same methods as <code>ComputingSystem</code>,
 * and some others. <br>
 * For all methods from <code>ComputingSystem</code>, this class chooses the right plug-in,
 * calls it, catching all exceptions, and controls the duration of the function.<br>
 * For all methods from plug-in, an attribute "'method name' timeout"  - in seconds, is defined
 * in the configuration file. If one of them is not defined in this file,
 * "default timeout" is used, and if this one is not defined either,
 * <code>defaultTimeOut</code> is used. <p>
 * If the timeout delay is elapsed before the end of a method, the user is asked to
 * interrupt the plug-in (if <code>askBeforeInterrupt==true</code>)
 */

public class CSPluginMgr implements MyComputingSystem{
  
  private ConfigFile configFile;

  private MyLogFile logFile;

  /** Timeout in ms for <code>submit</code> method */
  private long submissionTimeOut;
  /** Timeout in ms for <code>updateStatus</code> method */
  private long updateTimeOut;
  /** Timeout in ms for <code>exit</code> method */
  private long exitTimeOut;
  /** Timeout in ms for <code>killJob</code> method */
  private long killTimeOut;
  /** Timeout in ms for <code>getCurrentOutputs</code> method */
  public long currentOutputTimeOut;
  /** Timeout in ms for <code>cleanup</code> method */
  private long clearTimeOut;
  /** Timeout in ms for <code>getFullStatus</code> method */
  private long fullStatusTimeOut;
  /** Timeout in ms for <code>getUserInfoTimeOut</code> method */
  private long getUserInfoTimeOut;
  /** Timeout in ms for <code>copyFileTimeOut</code> method */
  public long copyFileTimeOut;
  /** Timeout in ms for <code>setupRuntimeEnvironments</code> method */
  private long setupTimeOut;
  /** Timeout in ms used if neither the specific timeout nor "default timeout" is
   * defined in configFile */
  private long defaultTimeOut = 60L;

  private String [] csNames;
  private HashMap<String, Object> cs;
  private int threadI;
  private String [] enabledCSs;
  private HashMap<String, Integer> notUpdatableJobs = new HashMap<String, Integer>();

  private Integer MAX_UPDATE_ATTEMPTS = 30;

  public CSPluginMgr() throws Throwable{
    init();
  }
  /**
   * Constructs a <code>CSPluginMgr</code>. <p>
   * Looks after plug-in names and class in configFile, load them, and read timeout values.
   * @throws Throwable if <ul>
   * <li>There is no ComputingSystem specified in configuration file
   * <li>One of theses computing system hasn't a class name defined
   * <li>An Exception occurs when GridPilot tries to load these classes (e.g. if
   * the constructor with one parameter (String) is not defined)
   * </ul>
   */
  public void init() throws Throwable{

    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    csNames = GridPilot.CS_NAMES;
    cs = new HashMap<String, Object>(csNames.length);
    
    try{
      loadClasses();
    }
    catch(Throwable e){
      Debug.debug("Error loading class "+e.getMessage(), 1);
      e.printStackTrace();
    }
    loadValues();
  }

  /**
   * Loads all plug-ins whose names are given by csNames.
   * @throws Throwable if an exception or an error occurs during plug-in loading.
   */
  public void loadClasses() throws Throwable{

    for(int i=0; i<csNames.length; ++i){
      
      if(!MyUtil.checkCSEnabled(csNames[i])){
        continue;
      }
      
      try{
        GridPilot.splashShow("Connecting to "+csNames[i]+"...");
      }
      catch(Exception e){
        // if we cannot show text on splash, just silently ignore
      }

      // Arguments and class name for <ComputingSystemName>ComputingSystem
      String csClass = configFile.getValue(csNames[i], "class");
      if(csClass==null){
        throw new Exception("Cannot load classes for system " + csNames[i] + " : \n"+
                            configFile.getMissingMessage(csNames[i], "class"));
      }

      Class<?> [] csArgsType = {String.class};
      Object [] csArgs = {csNames[i]};
      
      Debug.debug("class: "+csClass, 3);
      Debug.debug("argument types: "+MyUtil.arrayToString(csArgsType), 3);
      Debug.debug("arguments: "+MyUtil.arrayToString(csArgs), 3);
      try{
        cs.put(csNames[i], MyUtil.loadClass(csClass, csArgsType, csArgs, logFile));
        MyComputingSystem myCs = (MyComputingSystem) cs.get(csNames[i]);
        Debug.debug("Discovering RTEs for "+myCs.getClass().getCanonicalName(), 2);
        myCs.setupRuntimeEnvironments(csNames[i]);
      }
      catch(Throwable e){
        logFile.addMessage("ERROR: plugin " + csNames[i] + "(" + csClass + ") not loaded. ", e);
        e.printStackTrace();
      }
    }
    enabledCSs = cs.keySet().toArray(new String [cs.size()]);
  }
  
  /**
   * Get a list of the enabled computing systems.
   * @return a list of the names of the successfully loaded computing systems
   */
  public String [] getEnabledCSNames(){
    return enabledCSs;
  }

  void disconnect(){
    for(int i=0; i<csNames.length ; ++i){
      Shell shellMgr = null;
      try{
        shellMgr = GridPilot.getClassMgr().getShell(csNames[i]);
      }
      catch(Exception e){
        continue;
      }
      if(shellMgr instanceof MySecureShell){
        ((MySecureShell) shellMgr).exit();
      }
    }
  }

  /**
   * Reads timeout values in configuration file.
   */
  public void loadValues(){

    String tmp;

    // default timeout
    tmp = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "default timeout");
    if(tmp!=null){
      try{
        defaultTimeOut = new Long(tmp).intValue();
      }
      catch(NumberFormatException nfa){
        logFile.addMessage("value of default timeout (" + tmp +") is not an integer");
      }
    }

    /**
     * method timeouts
     */
    String timeOutNames [] = {"submit", "updateStatus", "exit", "killJob", "getCurrentOutputs",
      "cleanup", "getFullStatus", "getUserInfo", "copyFile"};
    long values [] = new long[timeOutNames.length];

    for(int i=0; i<timeOutNames.length; ++i){

      tmp = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, timeOutNames[i] + " timeout");
      if(tmp!=null){
        try{
          values[i] = new Long(tmp).longValue();
        }
        catch(NumberFormatException nfa){
          logFile.addMessage("value of " + timeOutNames[i] + " timeout (" + tmp +") is not an integer");
          values[i] = defaultTimeOut * 1000L;
        }
      }
      else{
        values[i] = defaultTimeOut * 1000L;
        Debug.debug(configFile.getMissingMessage(GridPilot.TOP_CONFIG_SECTION, timeOutNames[i] + " timeout"), 3);
      }
    }

    submissionTimeOut = values[0] * 1000L;
    updateTimeOut = values[1] * 1000L;
    exitTimeOut = values[2] * 1000L;
    killTimeOut = values[3] * 1000L;
    currentOutputTimeOut = values[4] * 1000L;
    clearTimeOut  = values[5] * 1000L;
    fullStatusTimeOut = values[6] * 1000L;
    getUserInfoTimeOut = values[7] * 1000L;
    copyFileTimeOut = values[8] * 1000L;

  }

  /**
   * Runs this job on the computing system specified by job.
   * Returns one of MyComputingSystem.RUN_*
   */
  public int run(final MyJobInfo job){
    
    // first check if the runtime environments are present
    try{
      checkRTEs(job);
    }
    catch(Exception e){
      //MessagePane.showMessage("Could not submit job. "+e.getMessage(), "Error");
      logFile.addMessage("Could not submit job. "+e.getMessage(), e);
      return MyComputingSystem.RUN_FAILED;
    }

    ResThread t = new ResThread(){
      int res = MyComputingSystem.RUN_FAILED;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(job.getCSName())).run(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + job.getCSName() +
                             " during job " + job.getName() + " submission", job, t);
          res = MyComputingSystem.RUN_FAILED;
        }
      }
      public int getIntRes(){
        return res;
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, job.getCSName(), submissionTimeOut, "run")){
      return t.getIntRes();
    }
    else{
      return MyComputingSystem.RUN_FAILED;
    }
  }

  private void checkRTEs(JobInfo job) throws IOException {
    
    String[] rtes = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName()
       ).getRuntimeEnvironments(job.getIdentifier());
    
    HashMap<String, String> rteApproximationMap = null;
    String csName = ((MyJobInfo)job).getCSName();
    if(csName!=null){
      rteApproximationMap = GridPilot.getClassMgr().getRteApproximationMap(csName);
    }
    
    String [] ids;
    boolean ok;
    DBResult allRtes;
    String provides;
    String name;
    String thisCsName;
    for(int i=0; i<rtes.length; ++i){
      // This is just a rough check: although all RTEs may be available
      //                             for a given CS, they may be available
      //                             on different OS'es.
      ids = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName()
         ).getRuntimeEnvironmentIDs(rtes[i], ((MyJobInfo) job).getCSName());
      if(ids==null){
        ok = false;
        allRtes = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName()).getRuntimeEnvironments();
        for(int j=0; j<allRtes.values.length; ++j){
          provides = (String) allRtes.getValue(j, "provides");
          name = (String) allRtes.getValue(j, "name");
          thisCsName = (String) allRtes.getValue(j, "computingSystem");
          if(csName.equalsIgnoreCase(thisCsName) && MyUtil.checkOS(rtes[i], name, provides==null?null:MyUtil.split(provides))){
            ok = true;
            if(rteApproximationMap!=null){
              rteApproximationMap.put(rtes[i], name);
            }
            break;
          }
        }
        if(!ok){
          throw new IOException("Runtime environment "+rtes[i]+" not found on "+((MyJobInfo) job).getCSName()+
              " in DB "+((MyJobInfo) job).getDBName());
        }
      }
    }
  }
  
  /**
   * Update the status of this job on the computing system specified by job.ComputingSystem
   * @see MyComputingSystem#updateStatus(Vector)
   */
  public void updateStatus(final Vector<JobInfo> jobs){
    final String csName = ((MyJobInfo) jobs.get(0)).getCSName();
    if(csName==null || csName.equals("")){
      return;
    }
    ResThread t = new ResThread(){
      public void run(){
        try{
          ((MyComputingSystem) cs.get(csName)).updateStatus(jobs);
        }
        catch(Throwable t){
          handleUpdateException(jobs, t);
        }
      }
    };

    t.start();

    MyUtil.myWaitForThread(t, csName, updateTimeOut, "updateStatus");
  }


  protected void handleUpdateException(Vector<JobInfo> jobs, Throwable t) {
    JobInfo job;
    Integer oldValue;
    for(Iterator<JobInfo> it=jobs.iterator(); it.hasNext();){
      job = it.next();
      if(!notUpdatableJobs.containsKey(job.getIdentifier())){
        notUpdatableJobs.put(job.getIdentifier(), new Integer(0));
      }
      else{
        oldValue = notUpdatableJobs.get(job.getIdentifier());
        notUpdatableJobs.put(job.getIdentifier(), oldValue+1);
        Debug.debug("Increased update error count for job "+job.getIdentifier()+" / "+job.getName()+" to "+
            notUpdatableJobs.get(job.getIdentifier()), 2);
      }
      if(notUpdatableJobs.get(job.getIdentifier())>MAX_UPDATE_ATTEMPTS ){
        String csName = ((MyJobInfo) jobs.get(0)).getCSName();
        logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
            " from plugin " + csName +
            " during updateStatus. Tried "+MAX_UPDATE_ATTEMPTS+" times. Setting job as failed.", t);
        job.setStatus(JobInfo.STATUS_FAILED);
        notUpdatableJobs.remove(job.getIdentifier());
      }
    }
  }
  
  /**
   * Kills these jobs
   * @see MyComputingSystem#killJobs(Vector)
   */
  public boolean killJobs(final Set<JobInfo> jobs){
    
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar();
    statusBar.setLabel("Killing job(s)...");
    statusBar.animateProgressBar();
    
    HashMap<String, MyLinkedHashSet<JobInfo>> csJobs = new HashMap<String, MyLinkedHashSet<JobInfo>>();
    String csName = null;
    MyJobInfo job;
    for(Iterator<JobInfo> it=jobs.iterator(); it.hasNext();){
      job = (MyJobInfo) it.next();
      csName = job.getCSName();
      if(!csJobs.keySet().contains(csName)){
        csJobs.put(csName, new MyLinkedHashSet<JobInfo>());
      }
      csJobs.get(csName).add(job);
    }
    
    final HashMap<String, MyLinkedHashSet<JobInfo>> csJobsFinal = csJobs;
    
    ResThread [] threads = new ResThread[csJobs.size()];
    for(threadI=0; threadI<threads.length; ++threadI){
 
      Debug.debug("Killing thread "+threads.length+":"+threadI, 3);
      
      threads[threadI] = new ResThread(){
        public void run(){
          try{
            String csName = (String) csJobsFinal.keySet().toArray()[threadI];
            ((MyComputingSystem) cs.get(csName)).killJobs(csJobsFinal.get(csName));
          }
          catch(Exception t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " during kill job ", t);
          }
        }
      };
    }

    for(int i=0; i<threads.length; ++i){
      threadI = i;
      threads[threadI].start();
      try{
        Thread.sleep(2000);
      }
      catch(InterruptedException e){
      }
    }

    // When killing jobs from several CSs, we wait the timeout of
    // the first one for the first jobs and then just move with
    // timeout 0 to kill the others
    MyUtil.myWaitForThread(threads[0],(String) csJobsFinal.keySet().toArray()[0],
        killTimeOut, "killJobs");
    if(csNames.length>1){
      for(int i=1; i<threads.length; ++i){
        MyUtil.myWaitForThread(threads[i], (String) csJobsFinal.keySet().toArray()[i],
            0, "killJobs");
      }
    }
    statusBar.stopAnimation();
    statusBar.setLabel("Killing job(s) done.");
    return true;
  }

  public boolean cleanup(final JobInfo job) {
    final String csName = ((MyJobInfo) job).getCSName();
    if(csName==null || csName.equals("")){
      return false;
    }

    ResThread t = new ResThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(csName)).cleanup(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " cleanup", (MyJobInfo) job, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };

    t.start();
    
    if(MyUtil.myWaitForThread(t, csName, clearTimeOut, "cleanup")){
      return t.getBoolRes();
    }
    else{
      return false;
    }

  }

  /**
   * Calls exit of all plug-ins
   * @see MyComputingSystem#exit()
   */
  public void exit() {
    for(int i=0; i<csNames.length; ++i){
      if(!MyUtil.checkCSEnabled(csNames[i])){
        continue;
      }
      final int k = i;
      ResThread t = new ResThread(){
        public void run(){
          try{
            ((MyComputingSystem) cs.get(csNames[k])).cleanupRuntimeEnvironments(csNames[k]);
            ((MyComputingSystem) cs.get(csNames[k])).exit();
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               "Exception from plugin " + csNames[k] +
                               " during exit", t);
          }
        }
      };

      t.start();

      MyUtil.myWaitForThread(t, csNames[k], exitTimeOut, "exit");

    }
  }

  /**
   * Gets the full status of the specified job on its ComputingSystem
   * @see MyComputingSystem#getFullStatus(MyJobInfo)
   */
  public String getFullStatus(final JobInfo job) {
    final String csName = ((MyJobInfo) job).getCSName();
    if(csName==null || csName.equals("")){
      return "ERROR: no computing system";
    }
    boolean csFound = false;
    for(int i=0; i<csNames.length; ++i){
      if(!MyUtil.checkCSEnabled(csNames[i])){
        continue;
      }
      if(csNames[i].equalsIgnoreCase(csName)){
        csFound = true;
        break;
      }
    }
    if(!csFound){
      return "Computing system "+csName+" not found. Try loading it.";
    }

    ResThread t = new ResThread(){
      String res = null;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(csName)).getFullStatus(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " getFullStatus", (MyJobInfo) job, t);
          res = null;
        }
      }
      public String getStringRes(){
        return res;
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, csName, fullStatusTimeOut, "getFullStatus")){
      return t.getStringRes();
    }
    else{
      return "No response";
    }
  }

  public String[] getCurrentOutput(final JobInfo job){
    final String csName = ((MyJobInfo) job).getCSName();
    if(csName==null || csName.equals("")){
      return null;
    }

    ResThread t = new ResThread(){
      String [] res = new String[]{null,null};
      public void run(){
        // Try three times - the FS may be blocking because the files are being written...
        for(int i=0; i<3; ++i){
          try{
            res = ((MyComputingSystem) cs.get(csName)).getCurrentOutput(job);
            return;
          }
          catch(Exception t){
            try {
              Thread.sleep(3000);
            }
            catch(InterruptedException e){
              e.printStackTrace();
            }
            setException(t);
          }
          res = new String[]{null, null};
          logFile.addMessage((getException() instanceof Exception ? "Exception" : "Error") +
              " from plugin " + csName +
              " during job " + job.getName() + " getCurrentOutpus", (MyJobInfo) job, getException());
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, csName, currentOutputTimeOut, "getCurrentOutputs")){
      return t.getString2Res();
    }
    else{
      return new String [] {null, "No response"};
    }
  }

  /**
   * Gets the scripts of the specified job on its ComputingSystem.
   * @see MyComputingSystem#getScripts(JobInfo)
   */
  public String[] getScripts(final JobInfo job) {
    final String csName = ((MyJobInfo) job).getCSName();
    if(csName==null || csName.equals("")){
      return null;
    }

    ResThread t = new ResThread(){
      String [] res = new String[]{null,null};
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(csName)).getScripts(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " getScripts", (MyJobInfo) job, t);
          res = new String[]{null, null};
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, csName, currentOutputTimeOut, "getScripts")){
      return t.getString2Res();
    }
    else{
      return new String [] {null, "No response"};
    }
  }

  public Shell getShell(final JobInfo job) {
    final String csName = ((MyJobInfo) job).getCSName();
    if(csName==null || csName.equals("")){
      return null;
    }

    MyResThread t = new MyResThread(){
      Shell res = null;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(csName)).getShell(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " getScripts", (MyJobInfo) job, t);
        }
      }
      public Shell getShellRes(){
        return res;
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, csName, currentOutputTimeOut, "getShell")){
      return t.getShellRes();
    }
    else{
      return null;
    }
  }

  /**
   * @see MyComputingSystem#getUserInfo()
   */
  public String getUserInfo(final String csName) {
    if(csName==null || csName.equals("")){
      return null;
    }

    ResThread t = new ResThread(){
      String res = null;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(csName)).getUserInfo(csName);
        }
        catch(Throwable t){
          /*logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " for getUserInfo", t);*/
          Debug.debug("Could not get user info. "+csName+"-->"+((MyComputingSystem) cs.get(csName))+t.getMessage(), 1);
          t.printStackTrace();
          res = null;
        }
      }
      public String getStringRes(){
        return res;
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, csName, getUserInfoTimeOut, "getUserInfo")){
      return t.getStringRes();
    }
    else{
      return "No response";
    }
  }

  public String getError(final String csName) {
    if(csName==null || csName.equals("")){
      return null;
    }

    ResThread t = new ResThread(){
      String res = null;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(csName)).getError();
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " for getError", t);
          res = null;
        }
      }
      public String getStringRes(){
        return res;
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, csName, defaultTimeOut*1000L, "getError")){
      return t.getStringRes();
    }
    else{
      return "No response";
    }
  }
  
  public void setCSUserInfo(final MyJobInfo job) {
    if(job.getCSName()==null || job.getCSName().equals("")){
      return;
    }

    ResThread t = new ResThread(){
      public void run(){
        try{
          ((MyComputingSystem) cs.get(job.getCSName())).setCSUserInfo(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + job.getCSName() +
                             " for getError", t);
        }
      }
    };

    t.start();

    if(MyUtil.myWaitForThread(t, job.getCSName(), defaultTimeOut*1000L, "setCSUserInfo", false)){
      return;
    }
    else{
      logFile.addMessage("WARNING: could not set userInfo from computing system "+job.getCSName());
    }
  }


  /**
   * @see MyComputingSystem#postProcess(MyJobInfo)
   */
  public boolean postProcess(final JobInfo job){

    ResThread t = new ResThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(((MyJobInfo) job).getCSName())).postProcess(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + ((MyJobInfo) job).getCSName() +
                             " during postProcessing", t);
          res = false;
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };

    t.start();
    
    long outNum = job.getOutputFileNames()==null?0L:job.getOutputFileNames().length;

    if(MyUtil.myWaitForThread(t, ((MyJobInfo) job).getCSName(), (outNum+2L)*copyFileTimeOut, "postProcessing")){
      if(t.getBoolRes()){
        // Register the new file if the db is a file catalog
        try{
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
          String[] outputFiles = dbPluginMgr.getOutputFiles(job.getIdentifier());
          if(outputFiles.length>0 && dbPluginMgr.isFileCatalog()){
            String datasetID = dbPluginMgr.getJobDefDatasetID(job.getIdentifier());
            String datasetName = dbPluginMgr.getDatasetName(datasetID);
            String remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(),
                outputFiles[0]);
            String [] nameArray = MyUtil.split(remoteName, "\\\\");
            nameArray = MyUtil.split(nameArray[nameArray.length-1], "/");
            String lfn = nameArray[nameArray.length-1];
            String size = dbPluginMgr.getFileBytes(datasetName, job.getIdentifier());
            String checksum = dbPluginMgr.getFileChecksum(datasetName, job.getIdentifier());
            String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
            String message = "Registering UUID "+uuid.toString()+" and LFN "+lfn+
               " for new location "+remoteName;
            GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar().setLabel(message);
            Debug.debug(message, 2);
            dbPluginMgr.registerFileLocation(datasetID, datasetName,
                uuid, lfn, remoteName, size, checksum, false);
            message = "Registration done";
            GridPilot.getClassMgr().getGlobalFrame().getMonitoringPanel().getStatusBar().setLabel(message);
          }
        }
        catch(Exception e){
          String error = "Exception during postProcess of job " + job.getName()+ "\n" +
          "\tException\t: " + e.getMessage();
          logFile.addMessage(error, e);
          return false;
        }
        return true;
      }
      else{
        return false;
      }
    }
    else{
      return false;
    }    
  }
  
  /**
   * Attention: this method propagates exceptions of the preProcess() of the called computing system.
   * @throws Exception if the called preProcess() throws an exception 
   * @see MyComputingSystem#preProcess(MyJobInfo)
   */
  public boolean preProcess(final JobInfo job) throws Exception{

    ResThread t = new ResThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(((MyJobInfo) job).getCSName())).preProcess(job);
        }
        catch(Exception e){
          res = false;
          setException(e);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };

    t.start();
    boolean waitOk;
    if((waitOk=MyUtil.myWaitForThread(t, ((MyJobInfo) job).getCSName(), copyFileTimeOut, "preProcessing")) &&
        t.getBoolRes()){
      return true;
    }
    else{
      if(t.getException()!=null){
        Debug.debug("Detected exception - propagating to caller.", 1);
        throw t.getException();
      }
      if(!waitOk){
        throw new TimeoutException("Timed out waiting for preProcessing of job "+job.getName());
      }
      return false;
    }
  }
  
  public void setupRuntimeEnvironments(final String csName) throws Exception{
    if(csName==null || csName.equals("")){
      return;
    }
    ResThread t = new ResThread(){
      public void run(){
        if(!MyUtil.checkCSEnabled(csName)){
          return;
        }
        try{
          ((MyComputingSystem) cs.get(csName)).setupRuntimeEnvironments(csName);
        }
        catch(Exception e){
          setException(e);
        }
      }
    };

    t.start();

    if(!MyUtil.myWaitForThread(t, csName, setupTimeOut, "setupRuntimeEnvironments")){
      throw new Exception("Timed out setting up runtime environments.");
    }
    if(t.getException()!=null){
      throw t.getException();
    }
  }

  public void cleanupRuntimeEnvironments(final String csName){
    if(csName==null || csName.equals("")){
      return;
    }
    ResThread t = new ResThread(){
      public void run(){
        try{
          if(!MyUtil.checkCSEnabled(csName)){
            return;
          }
          ((MyComputingSystem) cs.get(csName)).cleanupRuntimeEnvironments(csName);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during cleanupRuntimeEnvironments", t);
        }
      }
    };

    t.start();

    MyUtil.myWaitForThread(t, csName, setupTimeOut, "cleanupRuntimeEnvironments");
  }
  
  public void reconnect(){
    try{
      init();
    }
    catch (Throwable e1) {
      e1.printStackTrace();
      Debug.debug("WARNING: could not reload computing system plugins", 1);
    }
    
    for(int i=0; i<csNames.length ; ++i){
      Shell shellMgr = null;
      try{
        shellMgr = GridPilot.getClassMgr().getShell(csNames[i]);
      }
      catch(Exception e){
        //e.printStackTrace();
        continue;
      }
      if(shellMgr!=null && shellMgr instanceof MySecureShell){
        ((MySecureShell) shellMgr).reconnectAll();
      }
    }
  }
  public String getError() {
    // dummy method - here in GridPilot getError(csName) should always be used
    return null;
  }
  public long getRunningTime(JobInfo arg0) {
    return -1;
  }
  public VirtualMachine getVM(JobInfo arg0) {
    return null;
  }
  public boolean pauseJobs(Vector<JobInfo> arg0) {
    return false;
  }
  public boolean resumeJobs(Vector<JobInfo> arg0) {
    return false;
  }
  public boolean submit(JobInfo job) {
    // not used by GridPilot - see run()
    return false;
  }

}
