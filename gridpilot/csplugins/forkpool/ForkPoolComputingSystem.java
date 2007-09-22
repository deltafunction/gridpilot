package gridpilot.csplugins.forkpool;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;


import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobInfo;
import gridpilot.LocalShellMgr;
import gridpilot.SecureShellMgr;
import gridpilot.ShellMgr;
import gridpilot.TransferControl;
import gridpilot.Util;
import gridpilot.csplugins.fork.ForkComputingSystem;
import gridpilot.csplugins.fork.ForkScriptGenerator;

public class ForkPoolComputingSystem extends ForkComputingSystem implements ComputingSystem {

  // One ShellMgr per host
  protected HashMap remoteShellMgrs = null;
  protected String [] hosts = null;
  protected String [] maxJobs = null;
  
  protected String [] users = null;
  protected String [] passwords = null;
  
  public ForkPoolComputingSystem(String _csName) throws Exception{
    super(_csName);
    setupRemoteShellMgrs();
    setupRuntimeEnvironmentsSSH();
    Debug.debug("Using workingDir "+workingDir, 2);
  }
  
  /**
   * Sets up a HashMap of hosts -> ShellMgrs.
   */
  protected void setupRemoteShellMgrs(){
    remoteShellMgrs = new HashMap();
    // The host defined by 'host' is *the* host,
    // i.e. used when scanning for RTEs, etc.
    // The hosts defined by 'hosts' are used for running jobs.
    hosts = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Hosts");
    users = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Users");
    passwords = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Passwords");
    maxJobs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Max running jobs");
    String sshKeyFile = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Ssh key file");
    String sshKeyPassword = GridPilot.getClassMgr().getConfigFile().getValue(csName, "Ssh key passphrase");
    for(int i=0; i<hosts.length; ++i){
      if(hosts[i]!=null &&
          !hosts[i].startsWith("localhost") && !hosts[i].equals("127.0.0.1")){
        remoteShellMgrs.put(hosts[i],
           new SecureShellMgr(hosts[i],
               users!=null&&users.length>i&&users[i]!=null?users[i]:null/*null will cause prompting*/,
               passwords!=null&&passwords.length>i&&passwords[i]!=null?passwords[i]:null/*null will cause prompting*/,
                   sshKeyFile==null?null:new File(Util.clearTildeLocally(Util.clearFile(sshKeyFile))),
                   sshKeyPassword));
      }
      else if(hosts[i]!=null &&
          (hosts[i].startsWith("localhost") || hosts[i].equals("127.0.0.1"))){
        remoteShellMgrs.put(hosts[i], new LocalShellMgr());
      }
      else{
        // host null not accepted...
      }
    }
  }
  
  /**
   * Finds a ShellMgr for the host/user/password of the job.
   * If the shellMgr is dead it is attempted to start a new one.
   * 
   * @param job
   * @return a ShellMgr
   */
  protected ShellMgr getShellMgr(String host){
    ShellMgr mgr = null;
    if(host!=null &&
        !host.startsWith("localhost") && !host.equals("127.0.0.1")){
      SecureShellMgr sMgr = (SecureShellMgr) remoteShellMgrs.get(host);
      if(!sMgr.isConnected()){
        sMgr.reconnect();
      }
      mgr = sMgr;
    }
    else if(host!=null &&
        (host.startsWith("localhost") || host.equals("127.0.0.1"))){
      mgr = (ShellMgr) remoteShellMgrs.get(host);
    }
    return mgr;
  }
  
  /**
   * The brokering algorithm. As simple as possible: FIFO.
   */
  protected String selectHost(JobInfo job){
    ShellMgr mgr = null;
    String host = null;
    int maxR = 1;
    for(int i=0; i<hosts.length; ++i){
      host = hosts[i];
      maxR = 1;
      try{
        mgr = getShellMgr(host);
        if(maxJobs!=null && maxJobs.length>i && maxJobs[i]!=null){
          maxR = Integer.parseInt(maxJobs[i]);
        }
        if(mgr.getJobsNumber()<maxR){
          return host;
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return null;
  }
  
  protected String runDir(JobInfo job){
    return Util.clearFile(workingDir +"/"+job.getName());
  }

  public boolean submit(final JobInfo job){
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    job.setOutputs(stdoutFile, stderrFile);
    ForkScriptGenerator scriptGenerator =
      new ForkScriptGenerator(job.getCSName(), runDir(job));
    ShellMgr mgr = getShellMgr(job.getHost());
    scriptGenerator.createWrapper(mgr, job, job.getName()+commandSuffix);
    try{
      String id = mgr.submit(cmd, runDir(job), stdoutFile, stderrFile);
      job.setJobId(id!=null?id:"");
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

  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i)
      updateStatus((JobInfo) jobs.get(i), getShellMgr(((JobInfo) jobs.get(i)).getHost()));
  }

  public boolean killJobs(Vector jobsToKill){
    Vector errors = new Vector();
    JobInfo job = null;
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (JobInfo) en.nextElement();
        getShellMgr(job.getHost()).killProcess(job.getJobId());
      }
      catch(Exception e){
        errors.add(e.getMessage());
        logFile.addMessage("Exception during job killing :\n" +
                                    "\tJob#\t: " + job.getName() +"\n" +
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

    // Delete files that may have been copied to final destination.
    // Files starting with file: are considered to locally available, accessed
    // with shellMgr
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getJobDefId());
    String fileName;
    Vector remoteFiles = new Vector();
    for(int i=0; i<outputFileNames.length; ++i){
      fileName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(), outputFileNames[i]);
      if(fileName.startsWith("file:")){
        shellMgr.deleteFile(fileName);
      }
      else{
        remoteFiles.add(fileName);
      }
    }
    String [] remoteFilesArr = new String [remoteFiles.size()];
    for(int i=0; i<remoteFilesArr.length; ++i){
      remoteFilesArr[i] = (String) remoteFiles.get(i);
    }
    try{
      TransferControl.deleteFiles(remoteFilesArr);
    }
    catch(Exception e){
      error = "WARNING: could not delete output files. "+e.getMessage();
      Debug.debug(error, 3);
    }
    
    // Delete stdout/stderr that may have been copied to final destination
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        if(finalStdOut.startsWith("file:")){
          shellMgr.deleteFile(finalStdOut);
        }
        else{
          TransferControl.deleteFiles(new String [] {finalStdOut});
        }
      }
      catch(Exception e){
        error = "WARNING: could not delete "+finalStdOut+". "+e.getMessage();
        Debug.debug(error, 2);
      }
      catch(Throwable e){
        error = "WARNING: could not delete "+finalStdOut+". "+e.getMessage();
        Debug.debug(error, 2);
      }
    }
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      try{
        if(finalStdErr.startsWith("file:")){
          shellMgr.deleteFile(finalStdErr);
        }
        else{
          TransferControl.deleteFiles(new String [] {finalStdErr});
        }
      }
      catch(Exception e){
        error = "WARNING: could not delete "+finalStdErr+". "+e.getMessage();
        Debug.debug(error, 2);
      }
      catch(Throwable e){
        error = "WARNING: could not delete "+finalStdErr+". "+e.getMessage();
        Debug.debug(error, 2);
      }
    }

    try{
      getShellMgr(job.getHost()).deleteDir(runDir);
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
  }

  public String getFullStatus(JobInfo job){
    Debug.debug("Checking job "+job.getHost()+":"+job.getJobId(), 2);
    if(getShellMgr(job.getHost()).isRunning(job.getJobId())){
      return "Job #"+job.getJobId()+" is running.";
    }
    else{
      return "Job #"+job.getJobId()+" is not running.";
    }
  }

  public String[] getCurrentOutputs(JobInfo job){
    try{
      String stdOutText = getShellMgr(job.getHost()).readFile(job.getStdOut());
      String stdErrText = "";
      if(getShellMgr(job.getHost()).existsFile(job.getStdErr())){
        stdErrText = getShellMgr(job.getHost()).readFile(job.getStdErr());
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
    // In case this is not a local shell, first get the script to a local tmp file.
    if(!getShellMgr(job.getHost()).isLocal()){
      try{
        File tmpFile = File.createTempFile(/*prefix*/"GridPilot-Fork-", /*suffix*/"");
        // hack to have the file deleted on exit
        GridPilot.tmpConfFile.put(tmpFile.getAbsolutePath(), tmpFile);
        getShellMgr(job.getHost()).download(jobScriptFile, tmpFile.getAbsolutePath());
        jobScriptFile = tmpFile.getAbsolutePath();
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return new String [] {jobScriptFile};
  }

  public boolean postProcess(JobInfo job){
    Debug.debug("Post processing job " + job.getName(), 2);
    String runDir = runDir(job);
    boolean ok = true;
    if(copyToFinalDest(job, getShellMgr(job.getHost()))){
      // Delete the run directory
      try{
        ok = getShellMgr(job.getHost()).deleteDir(runDir);
      }
      catch(Exception e){
        e.printStackTrace();
        ok = false;
      }
      if(!ok){
        error = "Exception during postProcess of job " + job.getName();
        logFile.addMessage(error);
      }
      return ok;
    }
    else{
      return false;
    }
  }

  public boolean preProcess(JobInfo job){
    // choose the host
    String host = selectHost(job);
    if(host==null){
      logFile.addInfo("No free slots on any hosts.");
      return false;
    }
    
    ShellMgr mgr = getShellMgr(host);
    Debug.debug("Getting ShellMgr for host "+host, 2);
    job.setHost(host);
    job.setUser(mgr.getUserName());
    
    // create the run directory
    try{
      if(!getShellMgr(job.getHost()).existsFile(runDir(job))){
        getShellMgr(job.getHost()).mkdirs(runDir(job));
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not create run directory for job.", e);
      return false;
    }
    if(!getShellMgr(job.getHost()).isLocal()){
      try{
        writeUserProxy(getShellMgr(job.getHost()));
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not write user proxy.", e);
      }
    }
    return setupJobRTEs(job, getShellMgr(job.getHost())) &&
       setRemoteOutputFiles(job) && getInputFiles(job, getShellMgr(job.getHost()));
  }
  
  /**
   * The same as setupRuntimeEnvironments, but iterated over all remote ShellMgrs.
   */
  protected void setupRuntimeEnvironmentsSSH(){
    for(Iterator it=remoteShellMgrs.values().iterator(); it.hasNext();){
      setupRuntimeEnvironmentsSSH((ShellMgr) it.next());
    }
  }
  
  protected void setupRuntimeEnvironmentsSSH(ShellMgr shellMgr){
    for(int i=0; i<localRuntimeDBs.length; ++i){
      DBPluginMgr localDBMgr = null;
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
            localRuntimeDBs[i]);
      }
      catch(Exception e){
        Debug.debug("WARNING: Could not load runtime DB "+
            localRuntimeDBs[i]+". Runtime environments must be defined by hand. "+
            e.getMessage(), 1);
        continue;
      }
      try{
        scanRTEDir(localDBMgr, i>0?null:remoteDBPluginMgr, csName, shellMgr);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(localRuntimeDBs.length==0 && remoteDBPluginMgr!=null){
      try{
        scanRTEDir(null, remoteDBPluginMgr, csName, shellMgr);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
  }
  
  public void exit() {
    // Inform that running jobs will not be bookkept and ask for confirmation.
    ShellMgr shellMgr = null;
    boolean anyRunning = false;
    String host = null;
    int jobs = 0;
    String message = "WARNING: You have ";
    boolean oneIterationDone = false;
    for(Iterator it=remoteShellMgrs.values().iterator(); it.hasNext();){
      shellMgr = (ShellMgr) it.next();
      host = shellMgr.getHostName();
      jobs = shellMgr.getJobsNumber();
      if(oneIterationDone){
        message += ", \n";
      }
      if(jobs>0){
        message += jobs+" jobs running on "+host;
      }
      oneIterationDone = true;
    }
    if(anyRunning){
      message += ". The stdout/stderr of these jobs will be lost\n" +
          "and their output files will not be catalogued.";
      try{
        Util.showError(message);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    super.exit();
  }

  public ShellMgr getShellMgr(JobInfo job){
    return (ShellMgr) remoteShellMgrs.get(job.getHost());
  }

}