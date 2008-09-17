package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.ResThread;
import gridfactory.common.Shell;
import gridfactory.common.VirtualMachine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.safehaus.uuid.UUIDGenerator;

import com.jcraft.jsch.JSchException;

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
 * If the time out delay is elapsed before the end of a method, the user is asked to
 * interrupt the plug-in (if <code>askBeforeInterrupt==true</code>)
 */

public class CSPluginMgr implements MyComputingSystem{
  
  private ConfigFile configFile;

  private MyLogFile logFile;

  /** time out in ms for <code>submit</code> method */
  private int submissionTimeOut;
  /** time out in ms for <code>updateStatus</code> method */
  private int updateTimeOut;
  /** time out in ms for <code>exit</code> method */
  private int exitTimeOut;
  /** time out in ms for <code>killJob</code> method */
  private int killTimeOut;
  /** time out in ms for <code>getCurrentOutputs</code> method */
  public int currentOutputTimeOut;
  /** time out in ms for <code>clearOutputMapping</code> method */
  private int clearTimeOut;
  /** time out in ms for <code>getFullStatus</code> method */
  private int fullStatusTimeOut;
  /** time out in ms for <code>getUserInfoTimeOut</code> method */
  private int getUserInfoTimeOut;
  /** time out in ms for <code>copyFileTimeOut</code> method */
  private int copyFileTimeOut;
  /** time out in ms for <code>setupRuntimeEnvironments</code> method */
  private int setupTimeOut;
  /** time out in ms used if neither the specific time out nor "default timeout" is
   * defined in configFile */
  private int defaultTimeOut = 60*1000;

  private String [] csNames;
  private HashMap<String, Object> cs;
  private int threadI;
  private String [] enabledCSs;

  public CSPluginMgr() throws Throwable{
    init();
  }
  /**
   * Constructs a <code>CSPluginMgr</code>. <p>
   * Looks after plug-in names and class in configFile, load them, and read time out values.
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
    csNames = GridPilot.csNames;
    cs = new HashMap(csNames.length);

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

    String enabled = "no";

    for(int i=0; i<csNames.length; ++i){
      enabled = "no";
      try{
        enabled = configFile.getValue(csNames[i], "Enabled");
      }
      catch(Exception e){
        continue;
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
          !enabled.equalsIgnoreCase("true")){
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

      Class [] csArgsType = {String.class};
      Object [] csArgs = {csNames[i]};
      
      Debug.debug("class: "+csClass, 3);
      Debug.debug("argument types: "+MyUtil.arrayToString(csArgsType), 3);
      Debug.debug("arguments: "+MyUtil.arrayToString(csArgs), 3);
      try{
        cs.put(csNames[i], MyUtil.loadClass(csClass, csArgsType, csArgs));
      }
      catch(Throwable e){
        Debug.debug("ERROR: plugin " + csNames[i] + "(" + csClass + ") not loaded. "+e.getMessage(), 2);
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
        shellMgr = GridPilot.getClassMgr().getShellMgr(csNames[i]);
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
   * Reads time out values in configuration file.
   */
  public void loadValues(){

    String tmp;

    // default timeout
    tmp = configFile.getValue("GridPilot", "default timeout");
    if(tmp!=null){
      try{
        defaultTimeOut = new Integer(tmp).intValue();
        defaultTimeOut = 1000*defaultTimeOut;
      }
      catch(NumberFormatException nfa){
        logFile.addMessage("value of default timeout (" + tmp +") is not an integer");
      }
    }

    /**
     * method timeouts
     */
    String timeOutNames [] = {"submit", "updateStatus", "exit", "killJob", "getCurrentOutputs",
      "clearOutputMapping", "getFullStatus", "getUserInfo", "copyFile"};
    int values [] = new int[timeOutNames.length];

    for(int i=0; i<timeOutNames.length; ++i){

      tmp = configFile.getValue("GridPilot", timeOutNames[i] + " timeout");
      if(tmp!=null){
        try{
          values[i] = new Integer(tmp).intValue();
        }
        catch(NumberFormatException nfa){
          logFile.addMessage("value of " + timeOutNames[i] + " timeout (" + tmp +") is not an integer");
          values[i] = defaultTimeOut;
        }
      }
      else{
        values[i] = defaultTimeOut;
        Debug.debug(configFile.getMissingMessage("GridPilot", timeOutNames[i] + " timeout"), 3);
      }
    }

    submissionTimeOut = values[0] * 1000;
    updateTimeOut = values[1] * 1000;
    exitTimeOut = values[2] * 1000;
    killTimeOut = values[3] * 1000;
    currentOutputTimeOut = values[4] * 1000;
    clearTimeOut  = values[5] * 1000;
    fullStatusTimeOut = values[6] * 1000;
    getUserInfoTimeOut = values[7] * 1000;
    copyFileTimeOut = values[8] * 1000;

  }

  /**
   * Submits this job on the computing system specified by job.ComputingSystem
   * @see MyComputingSystem#submit(MyJobInfo)
   */
  public boolean submit(final JobInfo job){
    
    // first check if the runtime environments are present
    
    String[] rtes = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName()
        ).getRuntimeEnvironments(job.getIdentifier());

    for(int i=0; i<rtes.length; ++i){
      try{
        String id = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName()
        ).getRuntimeEnvironmentID(rtes[i], ((MyJobInfo) job).getCSName());
        if(id==null || id.equals("-1")){
          throw new IOException("Runtime environment "+rtes[i]+" not found. "+((MyJobInfo) job).getCSName()+
              ":"+((MyJobInfo) job).getDBName()+":"+id);
        }
      }
      catch(Exception e){
        //MessagePane.showMessage("Could not submit job. "+e.getMessage(), "Error");
        logFile.addMessage("Could not submit job. "+e.getMessage(), e);
        return false;
      }
    }

    ResThread t = new ResThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(((MyJobInfo) job).getCSName())).submit(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + ((MyJobInfo) job).getCSName() +
                             " during job " + job.getName() + " submission", (MyJobInfo) job, t);
          res = false;
        }
      }
      public boolean getBoolRes(){return res;}
    };

    t.start();

    if(MyUtil.waitForThread(t, ((MyJobInfo) job).getCSName(), submissionTimeOut, "submit")){
      return t.getBoolRes();
    }
    else{
      return false;
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
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during updateStatus", t);
        }
      }
    };

    t.start();

    MyUtil.waitForThread(t, csName, updateTimeOut, "updateStatus");
  }


  /**
   * Kills these jobs
   * @see MyComputingSystem#killJobs(Vector)
   */
  public boolean killJobs(final Vector<JobInfo> jobs){
    
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    statusBar.setLabel("Killing job(s)...");
    statusBar.animateProgressBar();
    
    HashMap csJobs = new HashMap();
    String csName = null;
    for(int i=0; i<jobs.size(); ++i){
      csName = ((MyJobInfo) jobs.get(i)).getCSName();
      if(!csJobs.keySet().contains(csName)){
        csJobs.put(csName, new Vector());
      }
      ((Vector) csJobs.get(csName)).add(jobs.get(i));
    }
    
    final HashMap csJobsFinal = csJobs;
    
    ResThread [] threads = new ResThread[csJobs.size()];
    for(threadI=0; threadI<threads.length; ++threadI){
 
      Debug.debug("Killing thread "+threads.length+":"+threadI, 3);
      
      threads[threadI] = new ResThread(){
        public void run(){
          try{
            String csName = (String) csJobsFinal.keySet().toArray()[threadI];
            ((MyComputingSystem) cs.get(csName)).killJobs((Vector) csJobsFinal.get(csName));
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
    MyUtil.waitForThread(threads[0],(String) csJobsFinal.keySet().toArray()[0],
        killTimeOut, "killJobs");
    if(csNames.length>1){
      for(int i=1; i<threads.length; ++i){
        MyUtil.waitForThread(threads[i], (String) csJobsFinal.keySet().toArray()[i],
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
                             " during job " + job.getName() + " clearOutputMapping", (MyJobInfo) job, t);
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };

    t.start();
    
    if(MyUtil.waitForThread(t, csName, clearTimeOut, "clean")){
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
    String enabled = null;
    for(int i=0; i<csNames.length; ++i){
      enabled = null;
      try{
        enabled = GridPilot.getClassMgr().getConfigFile().getValue(csNames[i], "Enabled");
      }
      catch(Exception e){
        continue;
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
          !enabled.equalsIgnoreCase("true")){
        continue;
      }
      final int k = i;
      ResThread t = new ResThread(){
        public void run(){
          try{
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

      MyUtil.waitForThread(t, csNames[k], exitTimeOut, "exit");

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
    String enabled = "no";
    for(int i=0; i<csNames.length; ++i){
      try{
        enabled = configFile.getValue(csNames[i], "Enabled");
      }
      catch(Exception e){
        continue;
      }
      if(enabled==null || !enabled.equalsIgnoreCase("yes") &&
          !enabled.equalsIgnoreCase("true")){
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

    if(MyUtil.waitForThread(t, csName, fullStatusTimeOut, "getFullStatus")){
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
        try{
          res = ((MyComputingSystem) cs.get(csName)).getCurrentOutput(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " getCurrentOutpus", (MyJobInfo) job, t);
          res = new String[]{null, null};
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };

    t.start();

    if(MyUtil.waitForThread(t, csName, currentOutputTimeOut, "getCurrentOutputs")){
      return t.getString2Res();
    }
    else{
      return new String [] {null, "No response"};
    }
  }

  /**
   * Gets the current outputs of the specified job on its ComputingSystem.
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

    if(MyUtil.waitForThread(t, csName, currentOutputTimeOut, "getScripts")){
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
      public Shell getShellMgr(){
        return res;
      }
    };

    t.start();

    if(MyUtil.waitForThread(t, csName, currentOutputTimeOut, "getScripts")){
      return t.getShellMgr();
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
          Debug.debug("Could not get user info. "+t.getMessage(), 1);
          t.printStackTrace();
          res = null;
        }
      }
      public String getStringRes(){
        return res;
      }
    };

    t.start();

    if(MyUtil.waitForThread(t, csName, getUserInfoTimeOut, "getUserInfo")){
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

    if(MyUtil.waitForThread(t, csName, defaultTimeOut, "getError")){
      return t.getStringRes();
    }
    else{
      return "No response";
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

    if(MyUtil.waitForThread(t, ((MyJobInfo) job).getCSName(), copyFileTimeOut, "postProcessing")){
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
            GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel(message);
            Debug.debug(message, 2);
            dbPluginMgr.registerFileLocation(datasetID, datasetName,
                uuid, lfn, remoteName, size, checksum, false);
            message = "Registration done";
            GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel(message);
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
   * @see MyComputingSystem#preProcess(MyJobInfo)
   */
  public boolean preProcess(final JobInfo job){

    ResThread t = new ResThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((MyComputingSystem) cs.get(((MyJobInfo) job).getCSName())).preProcess(job);
        }
        catch(Throwable t){
          t.printStackTrace();
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + ((MyJobInfo) job).getCSName() +
                             " during preProcessing", t);
          res = false;
        }
      }
      public boolean getBoolRes(){
        return res;
      }
    };

    t.start();

    if(MyUtil.waitForThread(t, ((MyJobInfo) job).getCSName(), copyFileTimeOut, "preProcessing")){
      return t.getBoolRes();
    }
    else{
      return false;
    }
  }
  
  public void setupRuntimeEnvironments(final String csName){
    if(csName==null || csName.equals("")){
      return;
    }
    ResThread t = new ResThread(){
      public void run(){
        try{
          if(!configFile.getValue(csName, "Enabled").equalsIgnoreCase("yes")){
            return;
          }
        }
        catch(Exception e){
          return;
        }
        try{
          ((MyComputingSystem) cs.get(csName)).setupRuntimeEnvironments(csName);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during setupRuntimeEnvironments", t);
        }
      }
    };

    t.start();

    MyUtil.waitForThread(t, csName, setupTimeOut, "setupRuntimeEnvironments");
  }

  public void cleanupRuntimeEnvironments(final String csName){
    if(csName==null || csName.equals("")){
      return;
    }
    ResThread t = new ResThread(){
      public void run(){
        try{
          try{
            if(!configFile.getValue(csName, "Enabled").equalsIgnoreCase("yes")){
              return;
            }
          }
          catch(Exception e){
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

    MyUtil.waitForThread(t, csName, setupTimeOut, "cleanupRuntimeEnvironments");
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
        shellMgr = GridPilot.getClassMgr().getShellMgr(csNames[i]);
      }
      catch(Exception e){
        //e.printStackTrace();
        continue;
      }
      if(shellMgr!=null && shellMgr instanceof MySecureShell){
        try {
          ((MySecureShell) shellMgr).reconnect();
        }
        catch(JSchException e){
          e.printStackTrace();
          logFile.addMessage("WARNING: Could not reconnect.", e);
        }
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

}
