package gridpilot.csplugins.fork;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalShellMgr;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.Util;

public class ForkComputingSystem implements ComputingSystem{

  String [] env = {
    "STATUS_WAIT="+ComputingSystem.STATUS_WAIT,
    "STATUS_RUNNING="+ComputingSystem.STATUS_RUNNING,
    "STATUS_DONE="+ComputingSystem.STATUS_DONE,
    "STATUS_ERROR="+ComputingSystem.STATUS_ERROR,
    "STATUS_FAILED="+ComputingSystem.STATUS_FAILED};

  private LogFile logFile;
  private String systemName;
  private LocalShellMgr shellMgr;
  private String workingDir;
  private String commandSuffix;

  public ForkComputingSystem(String _systemName){
    systemName = _systemName;
    logFile = GridPilot.getClassMgr().getLogFile();
    shellMgr = new LocalShellMgr();
    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(systemName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir.endsWith("/")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    commandSuffix = ".sh";
    if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      commandSuffix = ".bat";
    }
  }

  private String runDir(JobInfo job){
    return workingDir +"/"+job.getName();
  }
  
  /**
   * Script :
   *  params : partId stdOut stdErr
   *  return : 0 -> OK, job submitted, other values : job not submitted
   *  stdOut : jobId
   */
  public boolean submit(final JobInfo job){
    
    // create the run directory
    if(!shellMgr.existsFile(runDir(job))){
      shellMgr.mkdirs(runDir(job));
    }
    
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    job.setOutputs(stdoutFile, stderrFile);
    ForkScriptGenerator scriptGenerator =
      new ForkScriptGenerator(systemName, runDir(job));

    scriptGenerator.createWrapper(job, job.getName()+commandSuffix);
    
    try{
      Process proc = ((LocalShellMgr) shellMgr).submit(cmd, runDir(job), stdoutFile, stderrFile);
      job.setJobId(Integer.toString(proc.hashCode()));   
    }
    catch(Exception ioe){
      ioe.printStackTrace();
      logFile.addMessage("Exception during job " + job.getName() + " submission : \n" +
                         "\tCommand\t: " + cmd +"\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      return false;
    }
    return true;
  }

  /**
   * Script :
   *  param : jobId
   *  stdOut : status \n[host]
   *  return : ComputingSystem.STATUS_WAIT, STATUS_RUNNING, STATUS_DONE, STATUS_ERROR or STATUS_FAILED
   * (cf ComputingSystem.java)
   *
   */
  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i)
      updateStatus((JobInfo) jobs.get(i));
  }
  
  private void updateStatus(JobInfo job){
    
    // Host.
    job.setHost("localhost");

    // Status. Either running or not. 
    boolean jobRunning = false;
    Process proc = null;
    Iterator it = ((LocalShellMgr) shellMgr).processes.values().iterator();
    while(it.hasNext()){
      proc = ((Process) it.next());
       if(proc!=null &&
          Integer.parseInt(job.getJobId())==proc.hashCode()){
        jobRunning = true;
        break;
      }
    }
    if(jobRunning/*stdOut.length()!=0 &&
        stdOut.indexOf(job.getName())>-1*/
        ){
      job.setJobStatus("Running");
      job.setLocalStatus(ComputingSystem.STATUS_RUNNING);
    }
    else{
      File stdErrFile = new File(job.getStdErr());
      File stdOutFile = new File(job.getStdOut());
      if(stdErrFile.exists() && stdErrFile.length()>0){
        job.setJobStatus("Done with errors");
        job.setLocalStatus(ComputingSystem.STATUS_DONE);
      }
      else if(stdOutFile.exists()){
        job.setJobStatus("Done");
        job.setLocalStatus(ComputingSystem.STATUS_DONE);
      }
      else{
        job.setJobStatus("Error");
        job.setLocalStatus(ComputingSystem.STATUS_ERROR);
      }
      // Output file copy.
      // Try copying file(s) to output destination
      int jobDefID = job.getJobDefId();
      DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
      String[] outputMapping = dbPluginMgr.getOutputs(jobDefID);
      String localName = null;
      String remoteName = null;
      for(int i=0; i<outputMapping.length; ++i){
        try{
          localName = runDir(job) +"/"+dbPluginMgr.getJobDefOutLocalName(jobDefID,
              outputMapping[i]);
          localName = Util.clearFile(localName);
          remoteName = dbPluginMgr.getJobDefOutRemoteName(jobDefID, outputMapping[i]);
          remoteName = Util.clearFile(remoteName);
          Debug.debug(localName + ": -> " + remoteName, 2);
          shellMgr.copyFile(localName, remoteName);
        }
        catch(Exception e){
          job.setJobStatus("Error");
          job.setLocalStatus(ComputingSystem.STATUS_ERROR);          
          logFile.addMessage("Exception during copying of output file(s) for job : " + job.getName() + "\n" +
              "\tCommand\t: " + localName + ": -> " + remoteName +"\n" +
              "\tException\t: " + e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Script :
   *  param : jobId
   */
  public void killJobs(Vector jobsToKill){
    Process proc = null;
    String cmd = null;
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        Iterator it = ((LocalShellMgr) shellMgr).processes.keySet().iterator();
        while(it.hasNext()){
          cmd = (String) it.next();
          proc = (Process) ((LocalShellMgr) shellMgr).processes.get(
              (cmd));
          if(proc!=null &&
              Integer.parseInt(((JobInfo) en.nextElement()).getJobId())==
                proc.hashCode()){
            Debug.debug("killing job #"+proc.hashCode()+" : "+cmd, 2);
            proc.destroy();
            // should not be necessary
            try{
              ((LocalShellMgr) shellMgr).removeProcess(cmd);
            }
            catch(Exception ee){
            }
            return;
          }
        }
      }
      catch(Exception e){
        logFile.addMessage("Exception during job killing :\n" +
                                    "\tJob#\t: " + cmd +"\n" +
                                    "\tException\t: " + e.getMessage(), e);
      }
    }
  }

  public void clearOutputMapping(JobInfo job){
    try{
      (new File(runDir(job)+"/"+job.getName()+commandSuffix)).delete();
      (new File(runDir(job)+"/"+job.getName()+"stdout")).delete();
      (new File(runDir(job)+"/"+job.getName()+"stderr")).delete();
      (new File(runDir(job))).delete();
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during clearOutputMapping of job " + job.getName()+ "\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
    }
  }

  public void exit(){
    return;
  }

  public String getFullStatus(JobInfo job){
    Process proc = null;
    Iterator it = ((LocalShellMgr) shellMgr).processes.values().iterator();
    while(it.hasNext()){
      proc = ((Process) it.next());
       if(proc!=null &&
          Integer.parseInt(job.getJobId())==proc.hashCode()){
        return "Job #"+job.getJobId()+" is running.";
      }
    }
    return "";
  }

  public String[] getCurrentOutputs(JobInfo job){
    try{
      String stdOutText = shellMgr.readFile(job.getStdOut());
      String stdErrText = "";
      if(shellMgr.existsFile(job.getStdErr())){
        stdErrText = shellMgr.readFile(job.getStdErr());
      }
      return new String [] {stdOutText, stdErrText};
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during getFullStatus of job " + job.getName()+ "\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return null;
    }
  }
  
  public boolean copyFile(String csName, String src, String dest){
    try{
      return shellMgr.copyFile(src, dest);
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during copying of file " +
          csName + " : \n" +
          "\tSource\t: " + src + "\n" +
          "\tDestination\t: " + dest + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("IOException during copying of file " +ioe.getMessage(), 3);
      return false;
    }
  }
  
  public boolean deleteFile(String csName, String src){
    try{
      return shellMgr.deleteFile(src);
    }
    catch(Exception ioe){
      logFile.addMessage("IOException during copying of file " +
          csName + " : \n" +
          "\tSource\t: " + src + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("IOException during copying of file " +ioe.getMessage(), 3);
      return false;
    }
  }
  
  public String getUserInfo(String csName){
    String user = null;
    try{
      user = System.getProperty("user.name");
      /* remove leading whitespace */
      user = user.replaceAll("^\\s+", "");
      /* remove trailing whitespace */
      user = user.replaceAll("\\s+$", "");      
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during getUserInfo\n" +
                                 "\tException\t: " + ioe.getMessage(), ioe);
    }
    if(user==null){
      Debug.debug("Job user null, getting from config file", 3);
       user = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "user");
    }
    return user;
  }
}