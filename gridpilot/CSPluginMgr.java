package gridpilot;

import javax.swing.JOptionPane;
import java.util.HashMap;
import java.util.Vector;

import javax.swing.*;

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
  private HashMap cs ;
  private HashMap shellMgr;

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

    csNames = GridPilot.csNames;

    cs = new HashMap(csNames.length);
    shellMgr = new HashMap(csNames.length);

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
      try{
        GridPilot.splashShow("Connecting to "+csNames[i]+"...");
      }
      catch(Exception e){
        // if we cannot show text on splash, just silently ignore
      }
      String host = configFile.getValue(csNames[i], "host");
      if(host!=null && !host.endsWith("localhost")){
        String user = configFile.getValue(csNames[i], "user");
        String password = configFile.getValue(csNames[i], "password");
        String remoteHome = configFile.getValue(csNames[i], "working directory");
        shellMgr.put(csNames[i],
           new SecureShellMgr(host, user, password, remoteHome));
      }
      else if(host!=null && host.endsWith("localhost")){
        shellMgr.put(csNames[i], new LocalShellMgr());
      }
      else{
        // no shell used by this plugin
      }

      // Arguments and class name for <ComputingSystemName>ComputingSystem
      String csClass = configFile.getValue(csNames[i], "class");
      if(csClass==null){
        throw new Exception("Cannot load classes for system " + csNames[i] + " : \n"+
                            configFile.getMissingMessage(csNames[i], "class"));
      }

      Class [] csArgsType = {String.class};
      Object [] csArgs = {csNames[i]};
      
      boolean loadfailed = false;
      Debug.debug("argument types: "+Util.arrayToString(csArgsType), 3);
      Debug.debug("arguments: "+Util.arrayToString(csArgs), 3);
      try{
        /*Class newClass = this.getClass().getClassLoader().loadClass(csClass);
        cs.put(csNames[i],
            (newClass.getConstructor(csArgsType).newInstance(csArgs)));*/
        // Why doesn't this work?
        cs.put(csNames[i], Util.loadClass(csClass, csArgsType, csArgs));
      }
      catch(Exception e){
        loadfailed = true;
        Debug.debug("plugin " + csNames[i] + "(" + csClass + ") loaded", 2);
        //e.printStackTrace();
        //do nothing, will try with MyClassLoader.
      }
      if(loadfailed){
        try{
          try{
            try{
              // loading of this plug-in
              MyClassLoader mcl = new MyClassLoader();
              Debug.debug("Loading class "+csClass, 3);
              if(mcl!=null && csClass!=null && csArgsType!=null && csArgs!=null &&
                 mcl.findClass(csClass)!=null){
                cs.put(csNames[i],
                   (ComputingSystem)(mcl.findClass(csClass).getConstructor(csArgsType).newInstance(csArgs)));

                Debug.debug("plugin " + csNames[i] + "(" + csClass + ") loaded", 2);
              }
            }
            catch(ClassNotFoundException e){
              logFile.addMessage("Cannot load class for " + csNames[i], e);
              //throw e;
            }      
          }
          catch(IllegalArgumentException iae){
          logFile.addMessage("Cannot load class for " + csNames[i] + ".\nThe plugin constructor " +
                             "must have one parameter (String)", iae);
          //throw iae;
          }
        }
        catch(Exception e){
          logFile.addMessage("Cannot load class for " + csNames[i], e);
        }
      }
    }
  }

  void reconnect(){
    for(int i=0; i<csNames.length ; ++i){
      if(shellMgr.get(csNames[i]) instanceof SecureShellMgr){
        ((SecureShellMgr) shellMgr.get(csNames[i])).reconnect();
      }
    }
  }

  void disconnect(){
    for(int i=0; i<csNames.length ; ++i){
      if(shellMgr.get(csNames[i]) instanceof SecureShellMgr){
        ((SecureShellMgr) shellMgr.get(csNames[i])).exit();
      }
    }
  }

  /**
   * Return the Shell Manager for this job
   */
  public ShellMgr getShellMgr(JobInfo job) throws Exception{
    String csName = job.getCSName();
    if(csName==null || csName.equals("")){
      return askWhichShell(job);
    }
    else{
      return getShellMgr(csName);
    }
  }

  public ShellMgr getShellMgr(String csName) throws Exception{
    ShellMgr smgr = (ShellMgr) shellMgr.get(csName);
    if(smgr==null){
      Debug.debug("No computing system "+csName, 3);
      throw new Exception("No computing system "+csName);
    }
    else{
      return smgr;
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
        try{
          ((ComputingSystem) cs.get(csName)).updateStatus(jobs);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during updateStatus", t);
        }
      }
    };

    t.start();

    Util.waitForThread(t, csName, updateTimeOut, "updateStatus");
  }


  private int killThreadIndex = 0;
  /**
   * Kills these jobs
   * @see ComputingSystem#killJobs(Vector)
   */
  public boolean killJobs(final Vector jobs){
    
    StatusBar statusBar = GridPilot.getClassMgr().getStatusBar();
    statusBar.setLabel("Killing jobs ...");
    statusBar.animateProgressBar();
    
    HashMap csJobs = new HashMap();
    for(int i=0; i<csNames.length; ++i){
      csJobs.put(csNames[i], new Vector());
    }
    for(int i=0; i<jobs.size(); ++i){
      ((Vector) csJobs.get(((JobInfo) jobs.get(i)).getCSName())
          ).add(jobs.get(i));
    }
    
    //final Vector [] csJobsArray = (Vector []) csJobs.values().toArray();
    final Vector [] csJobsArray = new Vector[csNames.length];
    for(int i=0; i<csNames.length; ++i){
      csJobsArray[i] = (Vector) csJobs.get(csNames[i]);
    }
    
    MyThread [] threads = new MyThread[csNames.length];
    for(killThreadIndex=0; killThreadIndex<csNames.length;
       ++killThreadIndex){
 
      Debug.debug("Killing thread "+csJobsArray.length+":"+killThreadIndex, 3);
      
      if(csJobsArray==null || csJobsArray[killThreadIndex]==null ||
          csJobsArray[killThreadIndex].size()==0 ||
          ((JobInfo) csJobsArray[killThreadIndex].get(0)).getCSName()==null){
        continue;
      }
            
      threads[killThreadIndex] = new MyThread(){
        public void run(){
          try{
            Debug.debug("Killing thread "+csJobsArray.length+":"+killThreadIndex, 3);

            ((ComputingSystem) cs.get(((JobInfo) csJobsArray[killThreadIndex].get(0)).getCSName())
                ).killJobs(csJobsArray[killThreadIndex]);
          }
          catch(Exception t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " during kill job ", t);
          }
          ++killThreadIndex;
        }
      };

    }
    killThreadIndex = 0;
    for(int i=0; i<csNames.length; ++i){
      threads[i].start();
    }

    // When killing jobs from several CSs, we wait the timeout of
    // the first one for the first jobs and then just move with
    // timeout 0 to kill the others
    Util.waitForThread(threads[0],
        ((JobInfo) csJobsArray[0].get(0)).getCSName(),
        killTimeOut, "killJobs");
    if(csNames.length>1){
      for(killThreadIndex=1; killThreadIndex<csNames.length;
      ++killThreadIndex){
        Util.waitForThread(threads[killThreadIndex],
            ((JobInfo) csJobsArray[killThreadIndex].get(0)).getCSName(),
            0, "killJobs");
      }
    }
    statusBar.stopAnimation();
    statusBar.setLabel("Killing jobs done.");
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
    for(int i=0; i<csNames.length; ++i){
      final int k = i;
      MyThread t = new MyThread(){
        public void run(){
          try{
            ((ComputingSystem) cs.get(csNames[k])).exit();
            // TODO: is this necessary?
            //    - No (already done by disconnect), and it causes exception on exit.
            //((ShellMgr) shellMgr.get(csNames[k])).exit();
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
    for(int i=0; i<GridPilot.csNames.length; ++i){
      if(GridPilot.csNames[i].equalsIgnoreCase(csName)){
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

  private ShellMgr askWhichShell(JobInfo job){

    JComboBox cb = new JComboBox();
    for(int i=0; i<shellMgr.size() ; ++i){
      String type = "";
      if(shellMgr.get(csNames[i]) instanceof SecureShellMgr){
        type = " (remote)";
      }
      if(shellMgr.get(csNames[i]) instanceof LocalStaticShellMgr){
        type = " (local)";
      }
      cb.addItem(csNames[i] + type);
    }
    cb.setSelectedIndex(0);


    JPanel p = new JPanel(new java.awt.BorderLayout());
    p.add(new JLabel("Which shell do you want to use for this job (" +
                     job.getName() +")"), java.awt.BorderLayout.NORTH );
    p.add(cb, java.awt.BorderLayout.CENTER);

    JOptionPane.showMessageDialog(JOptionPane.getRootFrame(), p,
                                  "This job doesn't have a shell",
                                  JOptionPane.PLAIN_MESSAGE);

    int ind = cb.getSelectedIndex();
    if(ind>=0 && ind<shellMgr.size()){
      return (ShellMgr) shellMgr.get(csNames[ind]);
    }
    else{
      return null;
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
        // Register the new file
        try{
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
          String datasetID = dbPluginMgr.getJobDefDatasetID(job.getJobDefId());
          String datasetName = dbPluginMgr.getDatasetName(datasetID);
          String[] outputFiles = dbPluginMgr.getOutputFiles(job.getJobDefId());
          String remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(),
              outputFiles[0]);
          String [] nameArray = Util.split(remoteName, "\\\\");
          nameArray = Util.split(nameArray[nameArray.length-1], "/");
          String lfn = nameArray[nameArray.length-1];
          String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
          String message = "Registering UUID "+uuid.toString()+" and LFN "+lfn+
             " for new location "+remoteName;
          GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.statusBar.setLabel(message);
          Debug.debug(message, 2);
          dbPluginMgr.registerFileLocation(datasetID, datasetName,
              uuid, lfn, remoteName, false);
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
  
  /**
   * Update the status of this job on the computing system specified by job.ComputingSystem
   * @see ComputingSystem#updateStatus(Vector)
   */
  public void setupRuntimeEnvironments(final String csName){
    if(csName==null || csName.equals("")){
      return;
    }
    MyThread t = new MyThread(){
      public void run(){
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

}
