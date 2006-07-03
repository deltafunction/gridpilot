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
import gridpilot.ShellMgr;
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
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
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
    if(!LocalShellMgr.existsFile(runDir(job))){
      LocalShellMgr.mkdirs(runDir(job));
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
      Process proc = shellMgr.submit(cmd, runDir(job), stdoutFile, stderrFile);
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
    Iterator it = shellMgr.processes.values().iterator();
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
      job.setInternalStatus(ComputingSystem.STATUS_RUNNING);
    }
    else{
      File stdErrFile = new File(job.getStdErr());
      File stdOutFile = new File(job.getStdOut());
      if(stdErrFile.exists() && stdErrFile.length()>0){
        job.setJobStatus("Done with errors");
        job.setInternalStatus(ComputingSystem.STATUS_DONE);
      }
      else if(stdOutFile.exists()){
        job.setJobStatus("Done");
        job.setInternalStatus(ComputingSystem.STATUS_DONE);
      }
      else{
        job.setJobStatus("Error");
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
      // Output file copy.
      // Try copying file(s) to output destination
      int jobDefID = job.getJobDefId();
      DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
      String[] outputMapping = dbPluginMgr.getOutputMapping(jobDefID);
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
          LocalShellMgr.copyFile(localName, remoteName);
        }
        catch(Exception e){
          job.setJobStatus("Error");
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);          
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
  public boolean killJobs(Vector jobsToKill){
    Process proc = null;
    String cmd = null;
    Vector errors = new Vector();
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        Iterator it = shellMgr.processes.keySet().iterator();
        while(it.hasNext()){
          cmd = (String) it.next();
          proc = (Process) shellMgr.processes.get(
              (cmd));
          if(proc!=null &&
              Integer.parseInt(((JobInfo) en.nextElement()).getJobId())==
                proc.hashCode()){
            Debug.debug("killing job #"+proc.hashCode()+" : "+cmd, 2);
            proc.destroy();
            // should not be necessary
            try{
              shellMgr.removeProcess(cmd);
            }
            catch(Exception ee){
            }
          }
        }
      }
      catch(Exception e){
        errors.add(e.getMessage());
        logFile.addMessage("Exception during job killing :\n" +
                                    "\tJob#\t: " + cmd +"\n" +
                                    "\tException\t: " + e.getMessage(), e);
      }
    }
    if(errors.size()!=0){
      return false;
    }
    else{
      return true;
    }
  }

  public void clearOutputMapping(JobInfo job){
    String runDir = runDir(job);
    String scriptFile = runDir+"/"+job.getName()+commandSuffix;
    String stdoutFile = runDir+"/"+job.getName()+".stdout";
    String stderrFile = runDir+"/"+job.getName()+".stderr";
    try{
      Debug.debug("Clearing "+runDir, 2);
      LocalShellMgr.deleteFile(scriptFile);
      LocalShellMgr.deleteFile(stderrFile);
      LocalShellMgr.deleteFile(stdoutFile);
      LocalShellMgr.deleteFile(runDir);
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
    Iterator it = shellMgr.processes.values().iterator();
    while(it.hasNext()){
      proc = ((Process) it.next());
       if(proc!=null &&
          Integer.parseInt(job.getJobId())==proc.hashCode()){
        return "Job #"+job.getJobId()+" is running.";
      }
    }
    return "Job #"+job.getJobId()+" is not running.";
  }

  public String[] getCurrentOutputs(JobInfo job){
    try{
      String stdOutText = LocalShellMgr.readFile(job.getStdOut());
      String stdErrText = "";
      if(LocalShellMgr.existsFile(job.getStdErr())){
        stdErrText = LocalShellMgr.readFile(job.getStdErr());
      }
      return new String [] {stdOutText, stdErrText};
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during getFullStatus of job " + job.getName()+ "\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return null;
    }
  }
  
  public String[] getScripts(JobInfo job){
    String jobScriptFile = runDir(job)+"/"+job.getName()+commandSuffix;
      return new String [] {jobScriptFile};
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
  
  public boolean postProcess(JobInfo job){
    Debug.debug("PostProcessing for job " + job.getName(), 2);
    return copyToFinalDest(job);
  }

  public boolean preProcess(JobInfo job){
    return getInputFiles(job);
  }

  /**
   * Copies input files to run directory.
   * Assumes job.stdout points to a file in the run directory.
   */
  private boolean getInputFiles(JobInfo job){
    Debug.debug("Getting input files for job " + job.getName(), 2);
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] inputFiles = dbPluginMgr.getInputs(job.getJobDefId());
    ShellMgr shell = null;
    try{
      shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
    }
    catch(Exception e){
      Debug.debug("ERROR: could not copy stdout. "+e.getMessage(), 3);
      logFile.addMessage("WARNING could not copy files "+
          Util.arrayToString(inputFiles)+". ");
      // No shell available
      return false;
      //throw e;
    }
    for(int i=0; i<inputFiles.length; ++i){
      if(inputFiles[i]!=null && inputFiles[i].trim().length()!=0){
        try{
          if(!shell.existsFile(inputFiles[i])){
            logFile.addMessage("File " + job.getStdOut() + " doesn't exist");
            return false;
          }
        }
        catch(Throwable e){
          Debug.debug("ERROR getting input file: "+e.getMessage(), 2);
          logFile.addMessage("ERROR getting input file: "+e.getMessage());
          //throw e;
        }
        Debug.debug("Post processing : Getting " + inputFiles[i], 2);
        String fileName = inputFiles[i];
        int lastSlash = fileName.lastIndexOf("/");
        if(lastSlash>-1){
          fileName = fileName.substring(lastSlash + 1);
        }
        try{
          if(!shell.copyFile(inputFiles[i], runDir(job)+"/"+fileName)){
            logFile.addMessage("Pre-processing : Cannot get " +
                inputFiles[i]);
            return false;
          }
        }
        catch(Throwable e){
          Debug.debug("ERROR getting input file: "+e.getMessage(), 2);
          logFile.addMessage("ERROR getting input file: "+e.getMessage());
          //throw e;
        }
      }
    }
    return true;
  }
  
  /**
   * Moves job.StdOut and job.StdErr to final destination specified in the DB. <p>
   * job.StdOut and job.StdErr are then set to these final values. <p>
   * @return <code>true</code> if the move went ok, <code>false</code> otherwise.
   * (from AtCom1)
   */
  private boolean copyToFinalDest(JobInfo job){
    // Will only run if there is a shell available for the computing system
    // in question - and if the destination is accessible from this shell.
    // For grids, stdout and stderr should be taken care of by the xrsl or jdsl
    // (*ScriptGenerator)
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    // TODO: should we support destinations like gsiftp:// and http://?
    // For grid systems they should already have been taken care of by
    // the job description.
    ShellMgr shell = null;
    try{
      shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
    }
    catch(Exception e){
      Debug.debug("ERROR: could not copy stdout. " +e.getMessage(), 3);
      logFile.addMessage("WARNING could not copy stdout to "+finalStdOut);
      // No shell available
      return false;
      //throw e;
    }
    /**
     * move temp StdOut -> finalStdOut
     */
    if(finalStdOut!=null && finalStdOut.trim().length()!=0){
      try{
        if(!shell.existsFile(job.getStdOut())){
          logFile.addMessage("Post processing : File " + job.getStdOut() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        Debug.debug("ERROR checking for stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stdout: "+e.getMessage());
        //throw e;
      }
      Debug.debug("Post processing : Renaming " + job.getStdOut() + " in " + finalStdOut, 2);
      // if(!shell.moveFile(job.getStdOut(), finalStdOut)){
      try{
        if(!shell.copyFile(job.getStdOut(), finalStdOut)){
          logFile.addMessage("Post processing : Cannot move \n\t" +
                             job.getStdOut() +
                             "\n into \n\t" + finalStdOut);
          return false;
        }
      }
      catch(Throwable e){
        Debug.debug("ERROR copying stdout: "+e.getMessage(), 2);
        logFile.addMessage("ERROR copying stdout: "+e.getMessage());
        //throw e;
      }
      job.setStdOut(finalStdOut);
    }

    /**
     * move temp StdErr -> finalStdErr
     */

    if(finalStdErr!=null && finalStdErr.trim().length()!=0){
      try{
        if(!shell.existsFile(job.getStdErr())){
          logFile.addMessage("Post processing : File " + job.getStdErr() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        Debug.debug("ERROR checking for stderr: "+e.getMessage(), 2);
        logFile.addMessage("ERROR checking for stderr: "+e.getMessage());
        //throw e;
      }
      Debug.debug("Post processing : Renaming " + job.getStdErr() + " in " + finalStdErr,2);
      //shell.moveFile(job.getStdOut(), finalStdOutName);
      try{
        if(!shell.copyFile(job.getStdErr(), finalStdErr)){
          logFile.addMessage("Post processing : Cannot move \n\t" +
                             job.getStdErr() +
                             "\n into \n\t" + finalStdErr);
          return false;
        }
      }
      catch(Throwable e){
        Debug.debug("ERROR copying stderr: "+e.getMessage(), 2);
        logFile.addMessage("ERROR copying stderr: "+e.getMessage());
        //throw e;
      }
      job.setStdErr(finalStdErr);
    }
    return true;
  }

}