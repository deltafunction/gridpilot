package gridpilot;

import javax.swing.JOptionPane;
import java.util.HashMap;
import java.util.Vector;

import javax.swing.*;

/**
 * The purpose of this class is to protect AtCom from plugin errors, exceptions, abnormal behaviours. <p>
 *
 * The class <code>CSPluginMgr</code> defines the same methods as <code>ComputingSystem</code>,
 * and some others. <br>
 * For all methods from <code>ComputingSystem</code>, this class chooses the good plug-in,
 * calls it catching all exceptions, and controls the duration of the function.<br>
 * For all methods from plug-in, an attribute &lt;method name&gt; timeout in seconds is defined
 * in configuration file ; If one of them is not defined in this file, AtCom uses
 * "default timeout", and if this one is not defined either, AtCom uses
 * <code>defaultTimeOut</code>. <p>
 * If the time out delay is elapsed before the end of a method, the user is asked for
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
  /** time out in ms used if neither the specific time out nor "default timeout" is
   * defined in configFile */
  private int defaultTimeOut = 60*1000;

  private boolean askBeforeInterrupt = true;

  private String [] csNames;
  private HashMap cs ;
  private HashMap shellMgr;

  public CSPluginMgr(){}
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
        GridPilot.splash.show("Connecting to "+csNames[i]+"...");
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
      else if(host.endsWith("localhost")){
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
        Class newClass = this.getClass().getClassLoader().loadClass(csClass);
        cs.put(csNames[i],
            (newClass.getConstructor(csArgsType).newInstance(csArgs)));
        Debug.debug("plugin " + csNames[i] + "(" + csClass + ") loaded", 2);
      }
      catch(Exception e){
        loadfailed = true;
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
   * Returns the names of all computing system
   * @return a String array with the names of all computing system
   */
  public String [] getCSNames(){
    return csNames;
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

    if(waitForThread(t, job.getCSName(), submissionTimeOut, "submit")){
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

    waitForThread(t, csName, updateTimeOut, "updateStatus");
  }


  private int killThreadIndex = 0;
  private int getKillThreadIndex(){
    return killThreadIndex;
  }
  /**
   * Kills these jobs
   * @see ComputingSystem#killJobs(Vector)
   */
  public void killJobs(final Vector jobs){
    
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
    
    final Vector [] csJobsArray = (Vector []) csJobs.values().toArray();
    
    MyThread [] threads = new MyThread[csNames.length];
    for(killThreadIndex=0; killThreadIndex<csNames.length;
       ++killThreadIndex){
 
      if(csJobsArray==null || csJobsArray[killThreadIndex]==null ||
          csJobsArray[killThreadIndex].size()==0 ||
          ((JobInfo) csJobsArray[killThreadIndex].get(0)).getCSName()==null){
        continue;
      }
            
      threads[killThreadIndex] = new MyThread(){
        public void run(){
          int i = getKillThreadIndex();
          try{
            ((ComputingSystem) cs.get(((JobInfo) csJobsArray[i].get(0)).getCSName())
                ).killJobs(csJobsArray[i]);
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               " from plugin " + i +
                               " during job " + ((JobInfo) csJobsArray[i].get(0)).getCSName() + " killing",
                               (JobInfo) csJobsArray[i].get(0),
                               t);
          }
        }
      };

      threads[killThreadIndex].start();

      waitForThread(threads[killThreadIndex],
          ((JobInfo) csJobsArray[killThreadIndex].get(0)).getCSName(),
          killTimeOut, "killJobs");
    }
    statusBar.stopAnimation();
    statusBar.setLabel("Killing jobs done.");
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

    waitForThread(t, csName, clearTimeOut, "clearOutputMapping");
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
            ((ShellMgr) shellMgr.get(csNames[k])).exit();
          }
          catch(Throwable t){
            logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                               "Exception from plugin " + csNames[k] +
                               " during exit", t);
          }
        }
      };

      t.start();

      waitForThread(t, csNames[k], exitTimeOut, "exit");

    }
  }

  /**
   * Gets the full status of the specified job on its ComputingSystem
   * @see ComputingSystem#getFullStatus(JobInfo)
   */
  public String getFullStatus(final JobInfo job) {
    final String csName = job.getCSName();
    if(csName==null || csName.equals("")){
      return null;
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

    if(waitForThread(t, csName, fullStatusTimeOut, "getFullStatus")){
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

    if(waitForThread(t, csName, currentOutputTimeOut, "getCurrentOutputs")){
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

    if(waitForThread(t, csName, currentOutputTimeOut, "getScripts")){
      return t.getString2Res();
    }
    else{
      return new String [] {null, "No response"};
    }
  }

  /**
   * Asks the user if he wants to interrupt a plug-in
   */
  private boolean askForInterrupt(String csName, String fct){
    String msg = "No response from plugin " + csName +
                 " for " + fct + "\n"+
                 "Do you want to interrupt it ?";
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), msg, "No response from plugin",
        JOptionPane.YES_NO_OPTION);
    if(choice==JOptionPane.YES_OPTION){
      return true;
    }
    else{
      return false;
    }
  }

  /**
   * Waits the specified <code>MyThread</code> during maximum <code>timeOut</code> ms.
   * @return true if <code>t</code> ended normally, false if <code>t</code> has been interrupted
   */
  private boolean waitForThread(MyThread t, String csName, int _timeOut, String function){
    // TODO: if RemoteShellMgr from AtCom does not work better, replace with
    // RemoteShellMgr from AtCom1 and reenable this timeOut stuff.
    //int shellTimeout = RemoteShellMgr.sshOpenChannelRetries;
    int timeOut;
    /*if(shellTimeout>_timeOut){
      Debug.debug("WARNING: increasing thread timeout to "+shellTimeout,1);
      timeOut = shellTimeout;
    }
    else{*/
      timeOut = _timeOut;
    //}
    do{
      try{
        t.join(timeOut);
      }
      catch(InterruptedException ie){}

      if(t.isAlive()){
        if(!askBeforeInterrupt || askForInterrupt(csName, function)){
          logFile.addMessage("No response from plugin " +
              csName + " for " + function);
          t.interrupt();
          return false;
        }
      }
      else{
        break;
      }
    }
    while(true);
    return true;
  }


  private ShellMgr askWhichShell(JobInfo job){

    JComboBox cb = new JComboBox();
    for(int i=0; i<shellMgr.size() ; ++i){
      String type = "";
      if(shellMgr.get(csNames[i]) instanceof SecureShellMgr){
        type = " (remote)";
      }
      if(shellMgr.get(csNames[i]) instanceof LocalShellMgr){
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
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " for getUserInfo", t);
          res = null;
        }
      }
      public String getStringRes(){
        return res;
      }
    };

    t.start();

    if(waitForThread(t, csName, getUserInfoTimeOut, "getUserInfo")){
      return t.getStringRes();
    }
    else{
      return "No response";
    }
  }

  /**
   * @see ComputingSystem#copyFile(String, String, String)
   */
  public boolean copyFile(final String csName, final String src, final String dest){

    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).copyFile(csName, src, dest);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during copyFile", t);
          res = false;
        }
      }
      public boolean getBooleanRes(){
        return res;
      }
    };

    t.start();

    if(waitForThread(t, csName, copyFileTimeOut, "copyFile")){
      return t.getBooleanRes();
    }
    else{
      return false;
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

    if(waitForThread(t, job.getCSName(), copyFileTimeOut, "postProcessing")){
      return t.getBooleanRes();
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

    if(waitForThread(t, job.getCSName(), copyFileTimeOut, "preProcessing")){
      return t.getBooleanRes();
    }
    else{
      return false;
    }
  }
  
  /**
   * @see ComputingSystem#deleteFile(String, String)
   */
  public boolean deleteFile(final String csName, final String src){

    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).deleteFile(csName, src);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during deleteFile", t);
          res = false;
        }
      }
      public boolean getBooleanRes(){
        return res;
      }
    };

    t.start();

    if(waitForThread(t, csName, copyFileTimeOut, "deleteFile")){
      return t.getBooleanRes();
    }
    else{
      return false;
    }
  }

  /**
   * @see ComputingSystem#existsFile(String, String)
   */
  public boolean existsFile(final String csName, final String src){

    MyThread t = new MyThread(){
      boolean res = false;
      public void run(){
        try{
          res = ((ComputingSystem) cs.get(csName)).existsFile(csName, src);
        }
        catch(Throwable t){
          logFile.addMessage((t instanceof Exception ? "Exception" : "Error") +
                             " from plugin " + csName +
                             " during existsFile", t);
          res = false;
        }
      }
      public boolean getBooleanRes(){
        return res;
      }
    };

    t.start();

    if(waitForThread(t, csName, copyFileTimeOut, "existsFile")){
      return t.getBooleanRes();
    }
    else{
      return false;
    }
  }

}
