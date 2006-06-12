package gridpilot.csplugins.fork;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
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

  /**
   * Script :
   *  params : partId stdOut stdErr
   *  return : 0 -> OK, job submitted, other values : job not submitted
   *  stdOut : jobId
   */
  public boolean submit(final JobInfo job){
    
    final String stdoutFile = workingDir +"/"+job.getName()+ ".stdout";
    final String stderrFile = workingDir +"/"+job.getName()+ ".stderr";
    final String cmd = workingDir+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    job.setOutputs(stdoutFile, stderrFile);
    ForkScriptGenerator scriptGenerator =
      new ForkScriptGenerator(systemName, workingDir);

    scriptGenerator.createWrapper(job, job.getName()+commandSuffix);
    
    try{
      Process proc = ((LocalShellMgr) shellMgr).submit(cmd, workingDir, stdoutFile, stderrFile);
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
    // Get a list of all processes and see if there is one with the
    // name of the job.
    String cmd = null;
    if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      cmd = "tasklist";
    }
    else{
      cmd = "ps auxw";
    }
    int retValue;
    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();
    try{
      retValue = shellMgr.exec(cmd, env, null, stdOut, stdErr);
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job " + job.getName() + " update : \n" +
                                  "\tCommand\t: " + cmd + "\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      job.setLocalStatus(ComputingSystem.STATUS_ERROR);
      return;
    }
    Debug.debug("job " + job.getName() + " updateStatus : " + retValue+" : "+
        stdOut, 3);
    if(stdErr.length()!=0){
      logFile.addMessage("Error during job "+ job.getName() + " update :\n" +
                              "Command : " + cmd + "\n"+
                              "StdOut : " + stdOut + "\n"+
                              "StdErr : " + stdErr );
    }
    if(stdOut.length()!=0 && stdOut.indexOf(job.getName()+commandSuffix)>-1){
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
          localName = workingDir +"/"+dbPluginMgr.getJobDefOutLocalName(jobDefID,
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

    //String cmd = "killall -9 ";
    
    String cmd = null;
    if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      cmd = "taskkill /IM ";
    }
    else{
      cmd = "killall -9 ";
    }


    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      cmd += " "+((JobInfo) en.nextElement()).getName()+commandSuffix;
    }
    try{
      StringBuffer stdErr = new StringBuffer();
      shellMgr.exec(cmd, null, stdErr);
      if(stdErr.length()!=0)
        logFile.addMessage("Error during killing of job " +
                                " on " + systemName +" : \n" +
                                "\tCommand\t: " + cmd +"\n" +
                                "\tMessage\t: " + stdErr);
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job killing :\n" +
                                  "\tCommand\t: " + cmd +"\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
    }
  }


  public void clearOutputMapping(JobInfo job){
    String cmd = "rm "+job.getName()+commandSuffix;
    try{
      (new File(workingDir+"/"+job.getName()+commandSuffix)).delete();
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during clearOutputMapping of job " + job.getName()+ "\n" +
                                  "\tCommand\t: " + cmd +"\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
    }
  }

  public void exit(){
    return;
  }

  public String getFullStatus(JobInfo job){

    String cmd = null;
    if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      cmd = "tasklist";
    }
    else{
      cmd = "ps auxw";
    }

    try{
      StringBuffer stdErr = new StringBuffer();
      StringBuffer stdOut = new StringBuffer();
      shellMgr.exec(cmd, stdOut, stdErr);
      //return stdOut.toString() + (stdErr.length()!=0 ? "(" + stdErr + ")" : "");
      String [] stdOutArray = Util.split(stdOut.toString(), "\n");
      for(int i=0; i<stdOutArray.length; ++i){
        if(stdOutArray[i].toString().toLowerCase().indexOf(
            job.getName().toLowerCase())>-1){
          return stdOutArray[0]+"\n"+stdOutArray[i];
        }
      }
      return "job not found";
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during getFullStatus of job " + job.getName()+ "\n" +
                                  "\tCommand\t: " + cmd +"\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return "job not found";
    }
  }

  public String[] getCurrentOutputs(JobInfo job){

    try{
      String stdOutText = shellMgr.readFile(workingDir+"/"+job.getName()+"stdout");
      String stdErrText = "";
      if(shellMgr.existsFile(workingDir+"/"+job.getName()+"stderr")){
        stdErrText = shellMgr.readFile(workingDir+"/"+job.getName()+"stderr");
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
  
  public String getUserInfo(String csName){
    String user = null;
    //String cmd = "whoami";
    try{
      //StringBuffer stdErr = new StringBuffer();
      //StringBuffer stdOut = new StringBuffer();
      //shellMgr.exec(cmd, stdOut, stdErr);
      //user = stdOut.toString();
      user = System.getProperty("user.name");
      /* remove leading whitespace */
      user = user.replaceAll("^\\s+", "");
      /* remove trailing whitespace */
      user = user.replaceAll("\\s+$", "");      
    }
    catch(Exception ioe){
      /*logFile.addMessage("Exception during getUserInfo\n" +
                                  "\tCommands\t: " + cmd +"\n" +
                                 "\tException\t: " + ioe.getMessage(), ioe);*/
    }
    if(user==null){
      Debug.debug("Job user null, getting from config file", 3);
       user = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "user");
    }
    return user;
  }
}