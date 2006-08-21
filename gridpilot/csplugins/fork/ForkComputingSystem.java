package gridpilot.csplugins.fork;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalStaticShellMgr;
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
  private String csName;
  private LocalStaticShellMgr shellMgr;
  private String workingDir;
  private String commandSuffix;
  private String defaultUser;
  private String error = "";
  private String runtimeDirectory = null;
  private String publicCertificate = null;
  private String remoteDB = null;
  private HashSet finalRuntimesLocal = null;
  private HashSet finalRuntimesRemote = null;

  public ForkComputingSystem(String _csName){
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    shellMgr = new LocalStaticShellMgr();
    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(workingDir.startsWith("~")){
      workingDir = System.getProperty("defaultUser.home")+workingDir.substring(1);
    }
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    commandSuffix = ".sh";
    if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      commandSuffix = ".bat";
    }
    defaultUser = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "default user");
    runtimeDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "runtime directory");
    publicCertificate = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "public certificate");
    remoteDB = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "remote database");
    
    setupRuntimeEnvironments(csName);
  }

  private String runDir(JobInfo job){
    return workingDir +"/"+job.getName();
  }
  
  /**
   * By convention the runtime environments are defined by the
   * scripts in the directory specified in the config file (runtime directory).
   */
  public void setupRuntimeEnvironments(String csName){
    finalRuntimesLocal = new HashSet();
    finalRuntimesRemote = new HashSet();
    HashSet runtimes = LocalStaticShellMgr.listFilesRecursively(runtimeDirectory);
    if(runtimes!=null && runtimes.size()>0){
      File fil = null;
      String hostName = null;
      
      String name = null;
      String cert = null;
      String url = null;
      
      DBPluginMgr localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
        "HSQLDB");
      
      String [] runtimeEnvironmentFields =
        localDBMgr.getFieldNames("runtimeEnvironment");
      String [] rtVals = new String [runtimeEnvironmentFields.length];

      for(Iterator it=runtimes.iterator(); it.hasNext();){
        
        name = null;
        cert = null;
        url = null;

        fil = (File) it.next();
        
        // Get the name
        Debug.debug("File found: "+fil.getName()+":"+fil.getAbsolutePath(), 3);
        name = fil.getAbsolutePath().substring(
            (new File(runtimeDirectory)).getAbsolutePath().length()+1).replaceAll(
                "\\\\", "/");
        
        // Get the URL.
        // The URL is only for allowing the submitter to
        // download stdout/stderr
        try{
          hostName = InetAddress.getLocalHost().getCanonicalHostName();
        }
        catch(Exception e){
          e.printStackTrace();
        }
        // unqualified names are of no use
        if(hostName.indexOf(".")<0){
          hostName = null;
        }
        if(hostName==null){
          try{
            hostName = InetAddress.getLocalHost().getHostAddress();
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
        if(hostName==null){
          try{
            hostName = Util.getIPNumber();
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
        // if we cannot get the host name, try to get the IP address
        if(hostName==null){
          try{
            hostName = Util.getIPAddress();
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
        if(hostName!=null){
          url = "gsiftp://"+hostName+"/";
        }
        Debug.debug("url: "+url, 3);
        
        // get the certificate
        try{
          cert = LocalStaticShellMgr.readFile(publicCertificate);
          // TODO: check if certificate includes private key
          // and discard the key if so
        }
        catch(Exception e){
          //e.printStackTrace();
        }

        if(name!=null && name.length()>0 &&
            url!=null && url.length()>0){
          // Write the entry in the local DB
          for(int i=0; i<runtimeEnvironmentFields.length; ++i){
            if(runtimeEnvironmentFields[i].equalsIgnoreCase("name")){
              rtVals[i] = name;
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("url")){
              rtVals[i] = url;
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
              rtVals[i] = csName;
            }
            else{
              rtVals[i] = "";
            }
          }
          try{
            if(localDBMgr.createRuntimeEnvironment(rtVals)){
              finalRuntimesLocal.add(name);
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
          if(cert!=null && cert.length()>0){
            // Write the entry in the remote DB
            for(int i=0; i<runtimeEnvironmentFields.length; ++i){
              if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
                rtVals[i] = "GPSS";
              }
              else if(runtimeEnvironmentFields[i].equalsIgnoreCase("certificate")){
                rtVals[i] = cert;
              }
              else{
                rtVals[i] = "";
              }
            }
            try{
              DBPluginMgr remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
                  remoteDB);
              if(remoteDBMgr.createRuntimeEnvironment(rtVals)){
                finalRuntimesRemote.add(name);
              }
            }
            catch(Exception e){
              Debug.debug("WARNING: could not access "+remoteDB+". Disabling" +
                  "remote registration of runtime environments", 1);
            }
          }
        }
      }
    }
    else{
      Debug.debug("WARNING: no runtime environment scripts found", 1);
    }
  }
  
  /**
   * Script :
   *  params : partId stdOut stdErr
   *  return : 0 -> OK, job submitted, other values : job not submitted
   *  stdOut : jobId
   */
  public boolean submit(final JobInfo job){
    
    // create the run directory
    if(!LocalStaticShellMgr.existsFile(runDir(job))){
      LocalStaticShellMgr.mkdirs(runDir(job));
    }
    
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    job.setOutputs(stdoutFile, stderrFile);
    ForkScriptGenerator scriptGenerator =
      new ForkScriptGenerator(job.getCSName(), runDir(job));

    scriptGenerator.createWrapper(job, job.getName()+commandSuffix);
    
    try{
      Process proc = shellMgr.submit(cmd, runDir(job), stdoutFile, stderrFile);
      job.setJobId(Integer.toString(proc.hashCode()));   
    }
    catch(Exception ioe){
      ioe.printStackTrace();
      error = "Exception during job " + job.getName() + " submission : \n" +
      "\tCommand\t: " + cmd +"\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
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
      String jobDefID = job.getJobDefId();
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
          LocalStaticShellMgr.copyFile(localName, remoteName);
        }
        catch(Exception e){
          job.setJobStatus("Error");
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
          error = "Exception during copying of output file(s) for job : " + job.getName() + "\n" +
          "\tCommand\t: " + localName + ": -> " + remoteName +"\n" +
          "\tException\t: " + e.getMessage();
          logFile.addMessage(error, e);
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
      error = Util.arrayToString(errors.toArray());
      return false;
    }
    else{
      return true;
    }
  }

  public void clearOutputMapping(JobInfo job){
    String runDir = runDir(job);
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    try{
      LocalStaticShellMgr.deleteFile(finalStdOut);
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    try{
      LocalStaticShellMgr.deleteFile(finalStdErr);
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    try{
      LocalStaticShellMgr.deleteDir(new File(runDir));
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
  }

  public void exit(){
    String runtimeName = null;
    String initText = null;
    String id = "-1";
    boolean ok = true;
    DBPluginMgr localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
       "HSQLDB");
    for(Iterator it=finalRuntimesLocal.iterator(); it.hasNext();){
      ok = true;
      runtimeName = (String )it.next();
      // Don't delete records with a non-empty initText.
      // These can only have been created by hand.
      initText = localDBMgr.getRuntimeInitText(runtimeName, csName);
      if(initText!=null && !initText.equals("")){
        continue;
      }
      id = localDBMgr.getRuntimeEnvironmentID(runtimeName, csName);
      if(!id.equals("-1")){
        ok = localDBMgr.deleteRuntimeEnvironment(id);
      }
      else{
        ok = false;
      }
      if(!ok){
        Debug.debug("WARNING: could not delete runtime environment " +
            runtimeName+
            " from database "+
            localDBMgr.getDBName(), 1);
      }
    }
    if(remoteDB==null){
      return;
    }
    DBPluginMgr remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
        remoteDB);
    for(Iterator it=finalRuntimesRemote.iterator(); it.hasNext();){
      ok = true;
      runtimeName = (String )it.next();
      // Don't delete records with a non-empty initText.
      // These can only have been created by hand.
      initText = remoteDBMgr.getRuntimeInitText(runtimeName, csName);
      if(initText!=null && !initText.equals("")){
        continue;
      }
      id = remoteDBMgr.getRuntimeEnvironmentID(runtimeName, csName);
      if(!id.equals("-1")){
        ok = remoteDBMgr.deleteRuntimeEnvironment(id);
      }
      else{
        ok = false;
      }
      if(!ok){
        Debug.debug("WARNING: could not delete runtime environment " +
            runtimeName+
            " from database "+
            remoteDBMgr.getDBName(), 1);
      }
    }
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
      String stdOutText = LocalStaticShellMgr.readFile(job.getStdOut());
      String stdErrText = "";
      if(LocalStaticShellMgr.existsFile(job.getStdErr())){
        stdErrText = LocalStaticShellMgr.readFile(job.getStdErr());
      }
      return new String [] {stdOutText, stdErrText};
    }
    catch(IOException ioe){
      error = "IOException during getFullStatus of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
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
      user = System.getProperty("defaultUser.name");
      /* remove leading whitespace */
      user = user.replaceAll("^\\s+", "");
      /* remove trailing whitespace */
      user = user.replaceAll("\\s+$", "");      
    }
    catch(Exception ioe){
      error = "Exception during getUserInfo\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    if(user==null && defaultUser!=null){
      Debug.debug("Job defaultUser null, using value from config file", 3);
      user = defaultUser;
    }
    else{
      Debug.debug("ERROR: no defaultUser defined!", 1);
    }
    return user;
  }
  
  public boolean postProcess(JobInfo job){
    Debug.debug("PostProcessing for job " + job.getName(), 2);
    String runDir = runDir(job);
    if(copyToFinalDest(job)){
      try{
        LocalStaticShellMgr.deleteDir(new File(runDir));
      }
      catch(Exception e){
        error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
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
    String [] inputFiles = dbPluginMgr.getJobDefInputFiles(job.getJobDefId());
    ShellMgr shell = null;
    try{
      shell = GridPilot.getClassMgr().getCSPluginMgr().getShellMgr(job);
    }
    catch(Exception e){
      Debug.debug("ERROR: could not copy stdout. "+e.getMessage(), 3);
      error = "WARNING could not copy files "+
      Util.arrayToString(inputFiles);
      logFile.addMessage(error);
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
          error = "ERROR getting input file: "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
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
          error = "ERROR getting input file: "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
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
      error = "ERROR: could not copy stdout to "+finalStdOut+"\n"+
      e.getMessage();
      Debug.debug(error, 3);
      logFile.addMessage(error);
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
          error = "Post processing : File " + job.getStdOut() + " doesn't exist";
          logFile.addMessage(error);
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for stdout: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        //throw e;
      }
      Debug.debug("Post processing : Renaming " + job.getStdOut() + " in " + finalStdOut, 2);
      // if(!shell.moveFile(job.getStdOut(), finalStdOut)){
      try{
        if(!shell.copyFile(job.getStdOut(), finalStdOut)){
          error = "Post processing : Cannot move \n\t" +
          job.getStdOut() +
          "\n into \n\t" + finalStdOut;
          logFile.addMessage(error);
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR copying stdout: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
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
        error = "ERROR checking for stderr: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
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
        error = "ERROR copying stderr: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        //throw e;
      }
      job.setStdErr(finalStdErr);
    }
    return true;
  }
  
  public String getError(String csName){
    return error;
  }

}