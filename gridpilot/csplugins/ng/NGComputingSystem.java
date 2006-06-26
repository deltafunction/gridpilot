package gridpilot.csplugins.ng;

import java.io.*;
import java.util.Enumeration;
import java.util.Vector;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalShellMgr;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.Util;
import gridpilot.ConfigFile;

/**
 * Main class for the NorduGrid plugin. <br>
 * <p><a href="NGComputingSystem.java.html">see sources</a>
 */

public class NGComputingSystem implements ComputingSystem{

  public static final String NG_STATUS_ACCEPTED =  "ACCEPTED";
  public static final String NG_STATUS_PREPARING = "PREPARING" ;
  public static final String NG_STATUS_FINISHING = "FINISHING" ;
  public static final String NG_STATUS_FINISHED = "FINISHED" ;
  public static final String NG_STATUS_DELETED = "DELETED" ;
  public static final String NG_STATUS_CANCELLING = "CANCELLING";
  public static final String NG_STATUS_SUBMITTING = "SUBMITTING";
  public static final String NG_STATUS_INLRMSQ = "INLRMS: Q";
  public static final String NG_STATUS_INLRMSR = "INLRMS: R";
  public static final String NG_STATUS_INLRMSE = "INLRMS: E";

  public static final String NG_STATUS_FAILURE = "FAILURE";
  public static final String NG_STATUS_FAILED = "FAILED";
  public static final String NG_STATUS_ERROR = "ERROR";

  private NGScriptGenerator scriptGenerator;
  private NGSubmission ngSubmission;
  private NGUpdateStatus ngUpdateStatus;
  private Boolean gridProxyInitialized = Boolean.FALSE;
  private Timer timerProxy = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      Debug.debug("actionPerformed timeProxy", 3);
      gridProxyInitialized = Boolean.FALSE;
    }
  });
  protected LocalShellMgr shell;
  private static ConfigFile configFile;
  private static String systemName;
  private static LogFile logFile;
  private String workingDir;

  public NGComputingSystem(String _systemName) {
    systemName = _systemName;
    scriptGenerator = new NGScriptGenerator(systemName);
    ngSubmission = new NGSubmission(systemName);
    ngUpdateStatus = new NGUpdateStatus(systemName);
    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(systemName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    timerProxy.setRepeats(false);
    shell = new LocalShellMgr();
  }
  
  /*
   * Local directory to keep xrsl, shell script and temporary copies of stdin/stdout
   */
  private String runDir(JobInfo job){
    return workingDir +"/"+job.getName();
  }

  public boolean submit(JobInfo job) {
    
    Debug.debug("submitting..."+gridProxyInitialized, 3);

    synchronized(gridProxyInitialized){
      // avoids that dozens of popup open if
      // you submit dozen of jobs and proxy not initialized
      String reason = needInitProxy();
      if(reason != null)
        initGridProxy(reason);
    }
    Debug.debug("Using NGSubmission "+ngSubmission.toString(), 3);
    return ngSubmission.submit(job);

  }


  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i){
      updateStatus((JobInfo) jobs.get(i));
    }
  }

  private int updateStatus(JobInfo job){
    
    synchronized(gridProxyInitialized){
      // avoids that dozen of popup open if
      // you submit dozen of jobs and proxy not initialized
      String reason = needInitProxy();
      if(reason != null){
        initGridProxy(reason);
      }
    }
    
    if(ngUpdateStatus.updateStatus(job)){
      Debug.debug("Updating status of job "+job.getName(), 2);
      if(job.getJobStatus()==null){
        Debug.debug("No status found for job "+job.getName(), 2);
        job.setInternalStatus(ComputingSystem.STATUS_WAIT);
      }
      else if(job.getJobStatus().equals(NG_STATUS_FINISHED)){
        if(getOutput(job)){
          job.setInternalStatus(ComputingSystem.STATUS_DONE);
        }
        else{
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
        }
      }
      if(job.getJobStatus().equals(NG_STATUS_FAILURE)){
        //getOutput(job);
        return ComputingSystem.STATUS_FAILED;
      }
      if(job.getJobStatus().equals(NG_STATUS_ERROR)){
        getOutput(job);
        return ComputingSystem.STATUS_ERROR;
      }
      if(job.getJobStatus().equals(NG_STATUS_DELETED)){
        return ComputingSystem.STATUS_ERROR;
      }
      if(job.getJobStatus().equals(NG_STATUS_FAILED)){
        return ComputingSystem.STATUS_ERROR;
      }
      if(job.getJobStatus().equals(NG_STATUS_INLRMSR)){
        return ComputingSystem.STATUS_RUNNING;
      }
      return ComputingSystem.STATUS_WAIT;
    }
      return job.getInternalStatus();
  }

  public void killJobs(Vector jobsToKill){
    String killCommand = configFile.getValue(systemName, "Kill command");
    if(killCommand == null){
      logFile.addMessage("Could not kill job : " + configFile.getMissingMessage(systemName, "kill command"));
      return;
    }

    Vector cmds = new Vector();
    cmds.add(killCommand);

    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
    	cmds.add(((JobInfo) en.nextElement()).getJobId());
    }
    String [] cmd = (String[]) cmds.toArray(new String[cmds.size()]);
    

    try{
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();

      shell.exec((String []) cmd, stdOut, stdErr);

      Debug.debug("Kill job : " + stdOut, 3);

      if(stdErr.length()!=0)
        logFile.addMessage("Job killing :\n" +
                           "\tCommand\t: " + Util.arrayToString(cmd) +
                           "\tError\t: " + stdErr);

    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job killing :\n" +
                         "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
    }
  }

  public void clearOutputMapping(JobInfo job){
    
    // Delete files that may have been copied to storage elements
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String[] outputMapping = dbPluginMgr.getOutputMapping(job.getJobDefId());
    String localName;
    String logicalName;
    String physicalName;
    for (int i=0; i<outputMapping.length; ++i) {
      localName = Util.addFile(outputMapping[2*i]);
      logicalName = Util.addFile(outputMapping[2*i+1]);
      if(logicalName.startsWith("gsiftp://") ||
         logicalName.startsWith("ftp://")){
         physicalName = logicalName;
      }
      else{
        physicalName = logicalName;//ReplicaMgt.logToPhys(logicalName);
      }
      deleteFile(physicalName);
    }

    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      if(!deleteFile(finalStdOut)){
        logFile.addMessage("Cannot remove log " + finalStdOut, job);
      }
    }
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      if(!deleteFile(finalStdErr)){
        logFile.addMessage("Cannot remove log " + finalStdErr, job);
      }
    }
    String [][] cmd = new String [2][];
    cmd[0] = new String [] {configFile.getValue(systemName, "clean command"),  job.getJobId()};
    String dirName = runDir(job);
    if(dirName.startsWith("C:\\")){
      dirName = dirName.replaceAll(new String("C:\\\\"), new String("/"));
      dirName = dirName.replaceAll(new String("\\\\"), new String("/"));
    }
    cmd[1] = new String [] {"rm -rf", dirName};
    
    for(int i=0; i<2; ++i){
      Debug.debug("clean : command = " + Util.arrayToString(cmd[i]), 3);
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();
      try{
        shell.exec(cmd[i], stdOut, stdErr);
      }
      catch(IOException ioe){
        Debug.debug("IOException during job " + job.getName() + " clean :\n" +
                                    "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                    "\tException\t: " + ioe.getMessage()+ioe, 2);
        continue;
      }

      if(stdErr.length()!=0){
      	Debug.debug("Error while cleaning up after job " + job.getName() + " :\n" +
                                " Command : " + Util.arrayToString(cmd) + "\n" +
                                " Error : " + stdErr + "\n" +
                                " Output : " + stdOut, 2);
      }

      Debug.debug("end of cmd : " + stdOut, 3);

    }
  }

  public void exit() {
  }
  
  private boolean getOutput(JobInfo job){
    
    String dirName = runDir(job);
    
    Debug.debug("getOutput : " + job.getName() + " ("+job.getStdOut() + ")", 3);
    String cmdName = configFile.getValue(systemName, "getoutput command");
    if(cmdName==null){
      logFile.addMessage("Cannot get output for job " + job.getName() + " : \n" +
                              configFile.getMissingMessage(systemName, "getoutput command"));
      return false;
    }
    // After a crash some unfinished downloads may be around.
    // Clean up before downloading.
    String [] dlDirArray = Util.split(job.getJobId(), "/");
    String dlDir = dlDirArray[dlDirArray.length-1];
    String [] cmd = {"mv "+
    		dirName+"/"+dlDir+" "+
				dirName+"/"+dlDir+".`date +%s` >& /dev/null ;",
    		cmdName, /*changed from --dir. FO*/"-dir", dirName,
        job.getJobId()};
    Debug.debug("getOutput : command = " + Util.arrayToString(cmd), 3);
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();

    try{
      shell.exec(cmd, stdOut, stdErr);
      //Thread.sleep(500);
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job " + job.getName() + " get output :\n" +
                                  "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return false;
    }

    if(stdErr.length()!=0){
      logFile.addMessage("Error during get output of job " + job.getName() + " :\n" +
                              " Command : " + Util.arrayToString(cmd) + "\n" +
                              " Error : " + stdErr + "\n" +
                              " Output : " + stdOut);
      return false;
    }
    Debug.debug("end of cmd : " + stdOut, 3);
    
    // Move validation and extraction scripts up to top level directory
    String [] jobIdArr = Util.split(job.getJobId(), "/");
    String jobIdNum = jobIdArr[jobIdArr.length-1];
    cmd = new String [] {
        "chmod", "+x", dirName+"/"+jobIdNum+"/*",";",
        "chmod", "-x", dirName+"/"+jobIdNum+"/stdout",";",
        "chmod", "-x", dirName+"/"+jobIdNum+"/stderr",";",
        "cp -r", dirName+"/"+jobIdNum+"/*", dirName+"/"};

    try{
      shell.exec(cmd, stdOut, stdErr);
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job " + job.getName() + " get output :\n" +
                                  "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return false;
    }

    if(stdErr.length()!=0){
      logFile.addMessage("Error during get output of job " + job.getName() + " :\n" +
                              " Command : " + Util.arrayToString(cmd) + "\n" +
                              " Error : " + stdErr + "\n" +
                              " Output : " + stdOut);
      return false;
    }
    Debug.debug("end of cmd : " + stdOut, 3);    
    
    String [] jobIdList = job.getJobId().split("/");
    String jobIdNumber = jobIdList[jobIdList.length-1];
    if(job.getStdOut()!=null && !job.getStdOut().equals("")){      
      try{
        shell.copyFile(dirName+"/"+jobIdNumber+"/stdout", job.getStdOut());
      } 
      catch(IOException ioe){
        logFile.addMessage("IOException during job " + job.getName() + " getFullStatus :\n" +
                                    "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                    "\tException\t: " + ioe.getMessage(), ioe);
        return false;
      }
    }
    if(job.getStdErr() != null && !job.getStdErr().equals("")){
      try{
        shell.copyFile(dirName+"/"+jobIdNumber+"/stderr", job.getStdErr());
      }
      catch(IOException ioe){
        logFile.addMessage("IOException during job " + job.getName() + " getFullStatus :\n" +
                                    "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                    "\tException\t: " + ioe.getMessage(), ioe);
        return false;
      }
    }
    return true;
  }

  public String getFullStatus(JobInfo job){
    String checkingCommand = configFile.getValue(systemName, "Checking command");
    if(checkingCommand == null){
      logFile.addMessage(configFile.getMissingMessage(systemName, "Checking command"));
      return "Error : no checking command defined in configuration file";
    }

    synchronized(gridProxyInitialized){
      // avoids that dozen of popup open if
      // you submit dozen of jobs and proxy not initialized
      String reason = needInitProxy();
      if(reason!=null)
        initGridProxy(reason);
    }

    String [] cmd = {checkingCommand, "-l", job.getJobId()};

    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();

    try{
      shell.exec(cmd, stdOut, stdErr);
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job " + job.getName() + " getFullStatus :\n" +
                                  "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return ioe.getMessage();
    }
    return stdOut + (stdErr.length() ==0 ? "" : "\n(" + stdErr + ")");
  }

  public String [] getCurrentOutputs(JobInfo job){
    Debug.debug("getCurrentOutputs", 3);

    synchronized(gridProxyInitialized){
      // avoids that dozen of popup open if
      // you submit dozen of jobs and proxy not initialized
      String reason = needInitProxy();
      if(reason != null)
        initGridProxy(reason);
    }

    String [] res = new String[2];

    String catCommand = configFile.getValue(systemName, "output command");
    if(catCommand == null){
      logFile.addMessage(configFile.getMissingMessage(systemName, "output command"));
      res[0] = res[1] = null;
      return res;
    }
    if(job.getStdOut()!=null){

      String stdOutFile = job.getStdOut();
      String [] cmd = {"rm -f", stdOutFile, catCommand,
          "-o ", job.getJobId(), ">", stdOutFile+".tmp"};
      Debug.debug("getCurrentOutput : sdtOut command : " + Util.arrayToString(cmd), 3);
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();

      try{
        shell.exec(cmd, stdOut, stdErr);
        //res[0] = stdOut.toString();
        res[0] = shell.readFile(stdOutFile+".tmp");
      }catch(IOException ioe){
        logFile.addMessage("IOException during job " + job.getName() + " stdOut ngcat :\n" +
                                    "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                    "\tException\t: " + ioe.getMessage(), ioe);

      }
      if(stdErr.length()!=0){
        logFile.addMessage("Error during ngcat command : \n" +
                                "\tCommand : " + Util.arrayToString(cmd) + "\n" +
                                "\tError : " + stdErr);
        res[0] = "Error : " + stdErr;
      }
    }

    if(job.getStdErr() != null){
      String stdErrFile = job.getStdErr();
      String [] cmd = {"rm -f", stdErrFile, catCommand,
          "-e ", job.getJobId(), ">", stdErrFile+".tmp"};

      Debug.debug("getCurrentOutput : sdtErr command : " + Util.arrayToString(cmd), 3);

      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();

      try{
        shell.exec(cmd, stdOut, stdErr);
        //res[1] = stdOut.toString();
        res[1] = shell.readFile(stdErrFile+".tmp");
      }catch(IOException ioe){
        logFile.addMessage("IOException during job " + job.getName() + " stdErr ngcat :\n" +
                           "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                           "\tException\t: " + ioe.getMessage(), ioe);
      }
      if(stdErr.length()!=0){
        logFile.addMessage("Error during ngcat command : \n" +
                           "\tCommand : " + Util.arrayToString(cmd) + "\n" +
                           "\tError : " + stdErr);
        res[1] = "Error : " + stdErr;
      }
    }

    Debug.debug("getCurrentOutputs : end", 3);

    return res;
  }

  /**
   * @return
   * <li>null if grid-proxy-init is not required
   * <li>a string which contains reason is grid-proxy-init is required
   */
  public String needInitProxy(){
    if(gridProxyInitialized.booleanValue()){
      return null;
    }
    String timeLimit = configFile.getValue(systemName, "timeleft limit");
    if(timeLimit==null){
      logFile.addMessage(configFile.getMissingMessage(systemName, "timeleft limit"));
      timeLimit = "0";
      //return configFile.getMissingMessage(systemName, "timeleft limit");
    }
    String proxyTimeLeft = configFile.getValue(systemName, "proxy timeleft");
    if(proxyTimeLeft == null){
      logFile.addMessage(configFile.getMissingMessage(systemName, "proxy timeleft"));
      return configFile.getMissingMessage(systemName, "proxy timeleft");
    }

    String [] cmd = {proxyTimeLeft};

    try{
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();

      shell.exec(cmd, stdOut, stdErr);
      if(stdOut.toString().trim().length() != 0){
        int intTimeLimit = new Integer(timeLimit).intValue();
        if(stdOut.toString().endsWith("\n"))
          stdOut.deleteCharAt(stdOut.length()-1);

        int intTimeLeft = new Integer(stdOut.toString()).intValue();
        if(intTimeLeft <=0)
          return "Proxy expired";

        Debug.debug("Timeleft : " + intTimeLeft + ", time limit : " + intTimeLimit, 3);
        if(intTimeLimit<intTimeLeft){
          timerProxy.setInitialDelay((intTimeLeft - intTimeLimit) * 1000);
          timerProxy.start();
          gridProxyInitialized = Boolean.TRUE;
          return null;
        }
        else{
          return "Timeleft = " + intTimeLeft + " >= " + "time limit : " + intTimeLimit;
        }
      }
      else{
        return stdErr.toString();
      }

    }
    catch(Exception e){
      logFile.addMessage("Exception during evaluation of timeleft proxy :\n" +
                         "\tCommand\t: " +Util.arrayToString(cmd) + "\n" +
                         "\tException\t: " + e.getMessage(), e);
      return e.getMessage()==null ? e.getClass().getName() : e.getMessage();
    }
  }

  
  private void initGridProxy(String reason){
    
    int rep = 0;
    boolean success = false;
    
    do{
      success = askForGridProxyPassword(reason);
      ++rep;
      if(success){
        Debug.debug("grid init done", 2);
        gridProxyInitialized = Boolean.TRUE;
      }
    }   
    while(!success && rep<4);
    
  }

  private boolean askForGridProxyPassword(String reason){

    JPanel panel = new JPanel(new GridBagLayout());
    JTextPane tp = new JTextPane();
    tp.setText(reason);
    tp.setEditable(false);
    tp.setOpaque(false);
    tp.setBorder(null);

    JPasswordField pwf = new JPasswordField();

    panel.add(tp, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(new JLabel("Please enter your grid password:"),
        new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(pwf, new GridBagConstraints(0, 2, 1, 1, 1.0, 1.0,
        GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(5, 5, 5, 5),
        0, 0));

    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), panel,
        "Enter Grid Password", JOptionPane.OK_CANCEL_OPTION);

    if(choice != JOptionPane.OK_OPTION){
      return false;
    }

    String gridProxyInit = configFile.getValue(systemName, "proxy init");
    if(gridProxyInit == null){
      configFile.missingMessage(systemName, "proxy init");
      return false;
    }

    String [] cmd  = {"echo \""+new String(pwf.getPassword())+"\" | "+gridProxyInit+" -pwstdin"};

    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      shell.exec(cmd, stdOut, stdErr);
      Debug.debug("grid init :\n" +
                  "\tStdOut = " + stdOut + "\n" +
                  "\tStdErr = " + stdErr, 2);

    }catch(Exception e){
      Debug.debug("Cannot init grid proxy : \n" +
                         "\tCommand\t: " + Util.arrayToString(cmd) + "\n"+
                         "\tException\t: " + e.getMessage(), 1);
      return false;
    }

    return stdErr.length()==0;
  }
  
  public boolean copyFile(JobInfo job, String src, String dest){
    Debug.debug("Copying file "+ src + "->" + dest, 3);
    try{
      if(dest.startsWith("gsiftp://") && (src.startsWith("/") ||
          src.startsWith("file:///"))){
        Debug.debug("Using gridftp" + dest, 3);
        if(src.startsWith("/")){
          src = "file://"+src;
        }
        String [] cmd = {"ngcopy", src, dest};

        try{
          StringBuffer stdOut = new StringBuffer();
          StringBuffer stdErr = new StringBuffer();

          shell.exec(cmd, stdOut, stdErr);

          if(stdErr.length()!=0){
            logFile.addMessage("Could not copy " + src + "->" + dest +
                                    "\tCommand\t: " + Util.arrayToString(cmd) +
                                    "\tError\t: " + stdErr);
            return false;
          }
          else{
            return true;
          }

        }
        catch(IOException ioe){
          logFile.addMessage("Could not copy " + src + "->" + dest +
                                      "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                      "\tException\t: " + ioe.getMessage(), ioe);
          return false;
        }
      }
      else{
        Debug.debug("Using cp" + dest, 3);
        return AtCom.getClassMgr().getShellMgr(systemName).copyFile(src, dest);
      }
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during copying of file " +
          job.getName() + " : \n" +
          "\tSource\t: " + src + "\n" +
          "\tDestination\t: " + dest + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("IOException during copying of file " +ioe.getMessage(), 3);
      return false;
    }
  }

  public boolean deleteFile(String dest){
    Debug.debug("Deleting file "+ dest, 3);
    try{
      if(dest.startsWith("gsiftp://")){
        Debug.debug("Using gridftp" + dest, 3);
        if(dest.startsWith("/")){
          dest = "file://"+dest;
        }
        String [] cmd = {"ngremove", dest};

        try{
          StringBuffer stdOut = new StringBuffer();
          StringBuffer stdErr = new StringBuffer();

          shell.exec(cmd, stdOut, stdErr);

          if(stdErr.length()!=0){
            logFile.addMessage("Could not delete " + dest +
                                    "\tCommand\t: " + Util.arrayToString(cmd) +
                                    "\tError\t: " + stdErr);
            return false;
          }
          else{
            return true;
          }

        }
        catch(IOException ioe){
          logFile.addMessage("Could not delete " + dest +
                                      "\tCommand\t: " + Util.arrayToString(cmd) +"\n" +
                                      "\tException\t: " + ioe.getMessage(), ioe);
          return false;
        }
      }
      else{
        Debug.debug("Using rm" + dest, 3);
        return shell.deleteFile(dest);
      }
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during deletion of file : \n" +
          "\tDestination\t: " + dest + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("IOException during deletion of file " +ioe.getMessage(), 3);
      return false;
    }
  }

}