package gridpilot;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.safehaus.uuid.UUIDGenerator;

/**
 * The purpose of this class is to protect GridPilot from plugin errors, exceptions, abnormal behaviours. <p>
 *
 * The class <code>CSPluginMgr</code> defines the same methods as <code>ComputingSystem</code>,
 * and some others. <br>
 * For all methods from <code>ComputingSystem</code>, this class chooses the right plug-in,
 * calls it, catching all exceptions, and controls the duration of the function.<br>
 * For all methods from plug-in, an attribute &lt;method name&gt; timeout in seconds is defined
 * in configuration file ; If one of them is not defined in this file,
 * "default timeout" is used, and if this one is not defined either,
 * <code>defaultTimeOut</code> is used. <p>
 * If the time out delay is elapsed before the end of a method, the user is asked to
 * interrupt the plug-in (if <code>askBeforeInterrupt==true</code>)
 *
 * <p><a href="CSPluginMgr.java.html">see sources</a>
 */

public class CSPluginMgr implements ComputingSystem{
  
  private ConfigFile configFile;

  private LogFile logFile;

  /** time out in ms for <code>submit</code> method */
  private int submissionTimeOut;
  /** time out in ms for <code>updateStatus</code> method */
  private int updateTimeOut;
  /** time out in ms for <code>exit</code> method */
  private int exitTimeOut;
  /** time out in ms for <code>killJob</code> method */
  private int killTimeOut;
  /** time out in ms for <code>getCurrentOutputs</code> method */
  private int currentOutputTimeOut;
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
  private HashMap cs;
  private int threadI;
  private HashMap pullDBs;

  public CSPluginMgr() throws Throwable{
    init();
  }
  /**
   * Constructs a <code>CSPluginMgr</code>. <p>
   * Looks after plug-in names and class in configFile, load them, and read time out values.
   * @throws Throwable if <ul>
   * <li>There is no ComputingSystem specified in configuration file
   * <li>One of theses computing system hasn't a class name defined
   * <li>An Exception occurs when AtCom tries to load these classes (by example because
   * the constructor with one parameter (String) is not defined)
   * </ul>
   */
  public void init() throws Throwable{

    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    
    pullDBs = new HashMap();

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
    String pullDB = null;

    for(int i=0; i<csNames.length; ++i){
      enabled = "no";
      pullDB = null;
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
        pullDB = configFile.getValue(csNames[i], "Remote pull database");
      }
      catch(Exception e){
      }
      if(pullDB!=null && pullDB.length()>0){
        pullDBs.put(csNames[i], pullDB);
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
      Debug.debug("argument types: "+Util.arrayToString(csArgsType), 3);
      Debug.debug("arguments: "+Util.arrayToString(csArgs), 3);
      try{
        cs.put(csNames[i], Util.loadClass(csClass, csArgsType, csArgs));
      }
      catch(Throwable e){
        Debug.debug("ERROR: plugin " + csNames[i] + "(" + csClass + ") not loaded. "+e.getMessage(), 2);
        e.printStackTrace();
      }
    }
  }

  void disconnect(){
    for(int i=0; i<csNames.length ; ++i){
      ShellMgr shellMgr = null;
      try{
        shellMgr = GridPilot.getClassMgr().getShellMgr(csNames[i]);
      }
      catch(Exception e){
        continue;
      }
      if(shellMgr instanceof SecureShellMgr){
        ((SecureShellMgr) shellMgr).exit();
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
   * @see ComputingSystem#submit(JobInfo)
   */
  public boolean submit(final JobInfo job){
    
    // first check if the runtime environments are present
    
    String[] rtes = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()
        ).getRuntimeEnvironments(job.getJobDefId());

    for(int i=0; i<rtes.length; ++i){
      try{
        String id = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()
        ).getRuntimeEnvironmentID(rtes[i], job.getCSName());
        if(id==null || id.equals("-1")){
          throw new IOException("Runtime environment "+rtes[i]+" not found. "+job.getCSName()+
              ":"+job.getDBName()+":"+id);
        }
      }
      catch(Exception e){
        //MessagePane.showMessage("Could not submit job. "+e.getMessage(), "Error");
        logFile.addMessage("Could not submit job. "+e.getMessage(), e);
        return false;
      }
    }

    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(job.getCSName())).submit(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + job.getCSName() +
                             " during job " + job.getName() + " submission", job, t);
          res = false;
        }
      }
      public boolean getBooleanRes(){return res;}
    };

    t.start();

    if(Util.waitForThread(t, job.getCSName(), submissionTimeOut, "submit")){
      return t.getBooleanRes();
    }
    else{
      return false;
    }
  }

  /**
   * Update the status of this job on the computing system specified by job.ComputingSystem
   * @see ComputingSystem#updateStatus(Vector)
   */
  public void updateStatus(final Vector jobs){
    final String csName = ((JobInfo) jobs.get(0)).getCSName();
    if(csName==null || csName.equals("")){
      return;
    }
    MyThread t = new MyThread(){
      public void run(){
        if(csName.equalsIgnoreCase("gpss")){
          HashMap jobCSs = new HashMap();
          String pullCSName = null;
          JobInfo job = null;
          Enumeration en = jobs.elements();
          while(en.hasMoreElements()){
            job = (JobInfo) en.nextElement();
            pullCSName = GridPilot.getClassMgr().getJobCS(job.getJobDefId());
            if(!jobCSs.containsKey(pullCSName)){
              jobCSs.put(pullCSName, new Vector());
            }
            ((Vector) jobCSs.get(pullCSName)).add(job);
          }
          for(Iterator it=jobCSs.keySet().iterator(); it.hasNext();){
            pullCSName = (String) it.next();
            try{
              Debug.debug("Updating status of pulled jobs on "+pullCSName, 2);
              ((ComputingSystem) cs.get(csName)).updateStatus((Vector) jobCSs.get(pullCSName));
            }
            catch(Throwable t){
              logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                                 " from plugin " + csName +
                                 " during updateStatus", t);
            }
          }
        }       
        else{
          try{
            ((ComputingSystem) cs.get(csName)).updateStatus(jobs);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + csName +
                               " during updateStatus", t);
          }
        }
      }
    };

    t.start();

    Util.waitForThread(t, csName, updateTimeOut, "updateStatus");
  }


  /**
   * Kills these jobs
   * @see ComputingSystem#killJobs(Vector)
   */
  public boolean killJobs(final Vector jobs){
    
    StatusBar statusBar = GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar;
    statusBar.setLabel("Killing job(s)...");
    statusBar.animateProgressBar();
    
    HashMap csJobs = new HashMap();
    String csName = null;
    for(int i=0; i<jobs.size(); ++i){
      csName = ((JobInfo) jobs.get(i)).getCSName();
      if(!csJobs.keySet().contains(csName)){
        csJobs.put(csName, new Vector());
      }
      ((Vector) csJobs.get(csName)).add(jobs.get(i));
    }
    
    final HashMap csJobsFinal = csJobs;
    
    MyThread [] threads = new MyThread[csJobs.size()];
    for(threadI=0; threadI<threads.length; ++threadI){
 
      Debug.debug("Killing thread "+threads.length+":"+threadI, 3);
      
      threads[threadI] = new MyThread(){
        public void run(){
          try{
            String csName = (String) csJobsFinal.keySet().toArray()[threadI];
            ((ComputingSystem) cs.get(csName)).killJobs((Vector) csJobsFinal.get(csName));
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
    Util.waitForThread(threads[0],(String) csJobsFinal.keySet().toArray()[0],
        killTimeOut, "killJobs");
    if(csNames.length>1){
      for(int i=1; i<threads.length; ++i){
        Util.waitForThread(threads[i], (String) csJobsFinal.keySet().toArray()[i],
            0, "killJobs");
      }
    }
    statusBar.stopAnimation();
    statusBar.setLabel("Killing job(s) done.");
    return true;
  }

  public void clearOutputMapping(final JobInfo job) {
    final String csName = job.getCSName();
    if(csName==null || csName.equals("")){
      return;
    }

    MyThread t = new MyThread(){
      public void run(){
        try{
          ((ComputingSystem) cs.get(csName)).clearOutputMapping(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " clearOutputMapping", job, t);
        }
      }
    };

    t.start();

    Util.waitForThread(t, csName, clearTimeOut, "clearOutputMapping");
  }


  /**
   * Calls exit of all plug-ins
   * @see ComputingSystem#exit()
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
      MyThread t = new MyThread(){
        public void run(){
          try{
            ((ComputingSystem) cs.get(csNames[k])).exit();
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               "Exception from plugin " + csNames[k] +
                               " during exit", t);
          }
        }
      };

      t.start();

      Util.waitForThread(t, csNames[k], exitTimeOut, "exit");

    }
  }

  /**
   * Gets the full status of the specified job on its ComputingSystem
   * @see ComputingSystem#getFullStatus(JobInfo)
   */
  public String getFullStatus(final JobInfo job) {
    final String csName = job.getCSName();
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

    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).getFullStatus(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " getFullStatus", job, t);
          res = null;
        }
      }
      public String getStringRes(){
        return res;
      }
    };

    t.start();

    if(Util.waitForThread(t, csName, fullStatusTimeOut, "getFullStatus")){
      return t.getStringRes();
    }
    else{
      return "No response";
    }
  }

  /**
   * Gets the current outputs of the specified job on its ComputingSystem.
   * @see ComputingSystem#getCurrentOutputs(JobInfo)
   */
  public String[] getCurrentOutputs(final JobInfo job){
    final String csName = job.getCSName();
    if(csName==null || csName.equals("")){
      return null;
    }

    MyThread t = new MyThread(){
      String [] res = new String[]{null,null};
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).getCurrentOutputs(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " getCurrentOutpus", job, t);
          res = new String[]{null, null};
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };

    t.start();

    if(Util.waitForThread(t, csName, currentOutputTimeOut, "getCurrentOutputs")){
      return t.getString2Res();
    }
    else{
      return new String [] {null, "No response"};
    }
  }

  /**
   * Gets the current outputs of the specified job on its ComputingSystem.
   * @see ComputingSystem#getCurrentOutputs(JobInfo)
   */
  public String[] getScripts(final JobInfo job) {
    final String csName = job.getCSName();
    if(csName==null || csName.equals("")){
      return null;
    }

    MyThread t = new MyThread(){
      String [] res = new String[]{null,null};
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).getScripts(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during job " + job.getName() + " getScripts", job, t);
          res = new String[]{null, null};
        }
      }
      public String [] getString2Res(){
        return res;
      }
    };

    t.start();

    if(Util.waitForThread(t, csName, currentOutputTimeOut, "getScripts")){
      return t.getString2Res();
    }
    else{
      return new String [] {null, "No response"};
    }
  }

  /**
   * @see ComputingSystem#getUserInfo()
   */
  public String getUserInfo(final String csName) {
    if(csName==null || csName.equals("")){
      return null;
    }

    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).getUserInfo(csName);
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

    if(Util.waitForThread(t, csName, getUserInfoTimeOut, "getUserInfo")){
      return t.getStringRes();
    }
    else{
      return "No response";
    }
  }

  /**
   * @see ComputingSystem#getError()
   */
  public String getError(final String csName) {
    if(csName==null || csName.equals("")){
      return null;
    }

    MyThread t = new MyThread(){
      String res = null;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).getError(csName);
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

    if(Util.waitForThread(t, csName, defaultTimeOut, "getError")){
      return t.getStringRes();
    }
    else{
      return "No response";
    }
  }

  /**
   * @see ComputingSystem#postProcess(JobInfo)
   */
  public boolean postProcess(final JobInfo job){

    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(job.getCSName())).postProcess(job);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + job.getCSName() +
                             " during postProcessing", t);
          res = false;
        }
      }
      public boolean getBooleanRes(){
        return res;
      }
    };

    t.start();

    if(Util.waitForThread(t, job.getCSName(), copyFileTimeOut, "postProcessing")){
      if(t.getBooleanRes()){
        // Register the new file if the db is a file catalog
        try{
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
          String[] outputFiles = dbPluginMgr.getOutputFiles(job.getJobDefId());
          if(outputFiles.length>0 && dbPluginMgr.isFileCatalog()){
            String datasetID = dbPluginMgr.getJobDefDatasetID(job.getJobDefId());
            String datasetName = dbPluginMgr.getDatasetName(datasetID);
            String remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(),
                outputFiles[0]);
            String [] nameArray = Util.split(remoteName, "\\\\");
            nameArray = Util.split(nameArray[nameArray.length-1], "/");
            String lfn = nameArray[nameArray.length-1];
            String size = dbPluginMgr.getFileBytes(datasetName, job.getJobDefId());
            String checksum = dbPluginMgr.getFileChecksum(datasetName, job.getJobDefId());
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
   * @see ComputingSystem#preProcess(JobInfo)
   */
  public boolean preProcess(final JobInfo job){

    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(job.getCSName())).preProcess(job);
        }
        catch(Throwable t){
          t.printStackTrace();
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + job.getCSName() +
                             " during preProcessing", t);
          res = false;
        }
      }
      public boolean getBooleanRes(){
        return res;
      }
    };

    t.start();

    if(Util.waitForThread(t, job.getCSName(), copyFileTimeOut, "preProcessing")){
      return t.getBooleanRes();
    }
    else{
      return false;
    }
  }
  
  public void setupRuntimeEnvironments(final String csName){
    if(csName==null || csName.equals("")){
      return;
    }
    MyThread t = new MyThread(){
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
          ((ComputingSystem) cs.get(csName)).setupRuntimeEnvironments(csName);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during setupRuntimeEnvironments", t);
        }
      }
    };

    t.start();

    Util.waitForThread(t, csName, setupTimeOut, "setupRuntimeEnvironments");
  }

  public void cleanupRuntimeEnvironments(final String csName){
    if(csName==null || csName.equals("")){
      return;
    }
    MyThread t = new MyThread(){
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
          ((ComputingSystem) cs.get(csName)).cleanupRuntimeEnvironments(csName);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during cleanupRuntimeEnvironments", t);
        }
      }
    };

    t.start();

    Util.waitForThread(t, csName, setupTimeOut, "cleanupRuntimeEnvironments");
  }

  /**
   * Returns the setting of "remote pull database" in the config file
   */
  public String getPullDatabase(final String csName){
    if(csName==null || csName.equals("")){
      return null;
    }
    if(pullDBs==null || pullDBs.get(csName)==null){
      return null;
    }
    else{
      return (String) pullDBs.get(csName);
    }
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
      ShellMgr shellMgr = null;
      try{
        shellMgr = GridPilot.getClassMgr().getShellMgr(csNames[i]);
      }
      catch(Exception e){
        //e.printStackTrace();
        continue;
      }
      if(shellMgr!=null && shellMgr instanceof SecureShellMgr){
        ((SecureShellMgr) shellMgr).reconnect();
      }
    }
  }


}
