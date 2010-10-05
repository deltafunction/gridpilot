package gridpilot.csplugins.fork;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.AbstractList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.LogFile;
import gridfactory.common.Shell;
import gridfactory.common.Util;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.common.jobrun.VirtualMachine;

import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.MyJobInfo;
import gridpilot.GridPilot;
import gridpilot.MyTransferControl;
import gridpilot.MyUtil;

public class ForkComputingSystem implements MyComputingSystem{
  
  private long submitTimeout;
  
  protected RTEMgr rteMgr;

  protected String [] env = {
    "STATUS_WAIT="+MyJobInfo.STATUS_READY,
    "STATUS_RUNNING="+MyJobInfo.STATUS_RUNNING,
    "STATUS_DONE="+MyJobInfo.STATUS_DONE,
    "STATUS_ERROR="+MyJobInfo.STATUS_ERROR,
    "STATUS_FAILED="+MyJobInfo.STATUS_FAILED};

  protected LogFile logFile;
  protected String csName;
  protected Shell shell;
  protected String workingDir;
  protected String defaultUser;
  protected String userName;
  protected String error = "";
  protected String runtimeDirectory = null;
  protected String executableDirectory = null;
  protected String publicCertificate = null;
  protected String [] runtimeDBs = null;
  protected HashMap<String, String> toDeleteRTEs = null;
  // List of (Janitor) catalogs from where to get RTEs
  protected String [] rteCatalogUrls = null;
  protected MyTransferControl transferControl;
  protected boolean mkLocalOSRTE = true;
  protected boolean includeVMRTEs = true;
  protected String [] basicOSRTES = {"Linux"};
  protected boolean ignoreBaseSystemAndVMRTEs = true;
  protected String [] submitEnvironment = null;
  
  protected static HashMap<String, String> remoteCopyCommands = null;


  public ForkComputingSystem(String _csName) throws Exception{
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    transferControl = GridPilot.getClassMgr().getTransferControl();
    toDeleteRTEs = new HashMap<String, String>();
    String [] rtCpCmds = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "Remote copy commands");
    if(rtCpCmds!=null && rtCpCmds.length>1){
      remoteCopyCommands = new HashMap<String, String>();
      for(int i=0; i<rtCpCmds.length/2; ++i){
        remoteCopyCommands.put(rtCpCmds[2*i], rtCpCmds[2*i+1]);
      }
    }
    
    GridPilot.splashShow("Setting up shells...");
    
    try{
      shell = GridPilot.getClassMgr().getShell(csName);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get shell manager: "+e.getMessage(), 1);
      if(csName.equalsIgnoreCase("fork")){
        throw e;
      }
    }
    
    if(shell==null || shell.getOSName().toLowerCase().startsWith("windows")){
      basicOSRTES = new String [] {"Windows"};
    }
    
    defaultUser = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "default user");
    try{
      userName = shell.getUserName();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    workingDir = configFile.getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(MyUtil.onWindows() &&
        (shell==null || shell.isLocal()) && workingDir!=null && workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir!=null && workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    if(workingDir!=null && shell!=null && !shell.existsFile(workingDir)){
      logFile.addInfo("Working directory "+workingDir+" does not exist, creating.");
      shell.mkdirs(workingDir);
    }
    Debug.debug("Using workingDir "+workingDir, 2);
    
    runtimeDirectory = GridPilot.RUNTIME_DIR;
    
    rteCatalogUrls = configFile.getValues(GridPilot.TOP_CONFIG_SECTION, "runtime catalog URLs");

    publicCertificate = configFile.getValue(csName, "public certificate");
    runtimeDBs = configFile.getValues(csName, "runtime databases");
    
    submitTimeout = 700000L;
    String st = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "submit timeout");
    if(st!=null && !st.equals("")){
      submitTimeout = Long.parseLong(st)*1000L;
    }
    
    GridPilot.splashShow("Setting up RTEs for "+csName);
    Debug.debug("Setting up RTEs for "+csName, 2);
    if(runtimeDirectory!=null){
      if(shell!=null && !shell.existsFile(runtimeDirectory)){
        logFile.addInfo("Runtime directory "+runtimeDirectory+" does not exist, creating.");
        shell.mkdirs(runtimeDirectory);
      }
    }
    executableDirectory = configFile.getValue(csName, "Executable directory");   
    //if(shellMgr.isLocal() && executableDirectory!=null && executableDirectory.startsWith("~")){
    //  executableDirectory = System.getProperty("user.home")+executableDirectory.substring(1);
    //}
    MyUtil.checkAndActivateSSL(GridPilot.getClassMgr().getGlobalFrame(), rteCatalogUrls);
    rteMgr = GridPilot.getClassMgr().getRTEMgr(runtimeDirectory, rteCatalogUrls);
  }
  
  protected String getCommandSuffix(MyJobInfo job){
    String commandSuffix = ".sh";
    if(shell!=null){
      if(shell.isLocal() && MyUtil.onWindows()){
        commandSuffix = ".bat";
      }
    }
    return commandSuffix;
  }
  
  protected String runDir(JobInfo job){
    if(getShell(job).isLocal()){
      return MyUtil.clearTildeLocally(MyUtil.clearFile(workingDir+"/"+job.getName()));
    }
    else{
      return MyUtil.clearFile(workingDir +"/"+job.getName());
    }
  }
  
  /**
   * By convention the runtime environments are defined by the
   * scripts in the directory specified in the config file (runtime directory).
   */
  public void setupRuntimeEnvironments(String thisCs){
    for(int i=0; i<runtimeDBs.length; ++i){
      DBPluginMgr localDBMgr = null;
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
            runtimeDBs[i]);
      }
      catch(Exception e){
        Debug.debug("WARNING: Could not load runtime DB "+
            runtimeDBs[i]+". Runtime environments must be defined by hand. "+
            e.getMessage(), 1);
        continue;
      }
      try{
        scanRTEDir(localDBMgr, thisCs, shell);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, runtimeDBs, toDeleteRTEs,
        mkLocalOSRTE, includeVMRTEs, basicOSRTES, false);
  }
  
  /**
   * Scan runtime environment directory for runtime environment setup scripts;
   * register the found RTEs in local database with computing system cs;
   * @param dbMgr Local DBPluginMgr
   * @param cs Computing system name
   */
  protected void scanRTEDir(DBPluginMgr dbMgr, String cs, Shell mgr){
    String name = null;   
    HashSet<String> runtimes = mgr.listFilesRecursively(runtimeDirectory);
    String [] expandedRuntimeDirs = mgr.listFiles(MyUtil.clearFile(runtimeDirectory));
    String dirName = null;
    if(shell!=null && shell.isLocal() && MyUtil.onWindows()){
      dirName = runtimeDirectory.replaceFirst("^.*\\\\([^\\\\]+)$", "$1");
    }
    else{
      dirName = runtimeDirectory.replaceFirst("^.*/([^/]+)$", "$1");
    }
    if(expandedRuntimeDirs.length==0 ||
        expandedRuntimeDirs.length==1 && expandedRuntimeDirs[0].endsWith(dirName)){
      Debug.debug("No RTE files in "+runtimeDirectory, 2);
      return;
    }
    String expandedRuntimeDir = null;
    if(shell!=null && shell.isLocal() && MyUtil.onWindows()){
      expandedRuntimeDir = expandedRuntimeDirs[0].replaceFirst("^(.*)\\\\[^\\\\]+$", "$1");
      expandedRuntimeDir = expandedRuntimeDir.replaceFirst("^(.*)\\\\[^\\\\]+\\$", "$1")+"\\";
    }
    else{
      expandedRuntimeDir = expandedRuntimeDirs[0].replaceFirst("^(.*)/[^/]+$", "$1");
      expandedRuntimeDir = expandedRuntimeDir.replaceFirst("^(.*)/[^/]+/$", "$1")+"/";
    }

    if(runtimes!=null && runtimes.size()>0){
      String fil = null;      
      for(Iterator<String> it=runtimes.iterator(); it.hasNext();){
        
        name = null;
        fil = it.next();
        
        // Get the name
        Debug.debug("File found: "+expandedRuntimeDirs[0]+":"+expandedRuntimeDir+":"+fil, 3);
        name = fil.substring(expandedRuntimeDir.length());
        if(name.matches(".*/\\..*") ||  name.matches(".*\\..*") ||
            mgr.isDirectory(name) || name.matches(".*/pkg/.*") || name.matches(".*/data/.*") || name.matches(".*/control/.*")){
          continue;
        }
        // the first dependency is the OS
        String depends = "";
        String provides = "";
        try{
          depends = mgr.getOSName()+(mgr.getProvides()==null?"":(" "+MyUtil.arrayToString(mgr.getProvides())));
        }
        catch(Exception e1){
          depends = "";
          e1.printStackTrace();
        }        
        // Read dependencies from file.
        // The notation is:
        // #PROVIDES: <RTE name 1> <RTE name 2> ...
        // #DEPENDS: <RTE name 1> <RTE name 2> ...
        try{
          String content = mgr.readFile(fil);
          InputStream dis = new ByteArrayInputStream(content.getBytes());
          BufferedReader in = new BufferedReader(new InputStreamReader(dis));
          String line = null;
          String providesPattern = "(?i)^\\s*#PROVIDES: (.+)";
          String dependsPattern = "(?i)^\\s*#PROVIDES: (.+)";
          while((line=in.readLine())!=null){
            if(line.matches(dependsPattern)){
              depends += " ";
              depends += line.replaceFirst(dependsPattern, "$1");
            }
            if(line.matches(providesPattern)){
              provides += " ";
              provides += line.replaceFirst(providesPattern, "$1");
            }
          }
          in.close();
        }
        catch(IOException e){
          String error = "Could not open "+fil;
          e.printStackTrace();
          Debug.debug(error, 2);
        }
        if(name!=null && name.length()>0){
          // Write the entry in the local DB
          Debug.debug("Writing RTE "+name+" in local DB "+dbMgr.getDBName(), 3);
          createLocalRTE(dbMgr, name, cs, provides.trim(), depends.trim());
        }
      }
    }
    else{
      Debug.debug("WARNING: no local runtime environment scripts found", 1);
    }
  }
  
  protected String getUrl(){
    String hostName = null;
    String url = null;
    // Get the URL - not used at the moment
    try{
      hostName = MyUtil.getIPAddress();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(hostName!=null){
      url = "gsiftp://"+hostName+"/";
    }
    Debug.debug("url: "+url, 3);
    return url;
  }
  
  protected String getCertificate(Shell shellMgr){
    String cert = null;
    if(publicCertificate!=null){
      // get the certificate
      try{
        cert = shellMgr.readFile(publicCertificate);
        // TODO: check if certificate includes private key
        // and discard the key if so
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return cert;
  }
  
  private void createLocalRTE(DBPluginMgr dbPluginMgr, String name, String csName,
      String provides, String depends){
    if(dbPluginMgr==null){
      return;
    }
    String [] runtimeEnvironmentFields = null;
    String [] rtVals = null;
    try{
      runtimeEnvironmentFields = dbPluginMgr.getFieldNames("runtimeEnvironment");
    }
    catch(Exception e){
      e.printStackTrace();
    }
    if(runtimeEnvironmentFields==null){
      return;
    }
    rtVals = new String [runtimeEnvironmentFields.length];
    Debug.debug("runtimeEnvironmentFields: "+runtimeEnvironmentFields.length, 3);
    for(int i=0; i<runtimeEnvironmentFields.length; ++i){
      if(runtimeEnvironmentFields[i].equalsIgnoreCase("name")){
        rtVals[i] = name;
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
        rtVals[i] = csName;
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("depends") && depends!=null &&! depends.equals("")){
        rtVals[i] = depends;
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("provides") && provides!=null &&!provides.equals("")){
        rtVals[i] = provides;
      }
      else{
        rtVals[i] = "";
      }
    }
    boolean rteExists = false;
    String rtId = null;
    if(dbPluginMgr!=null){
      try{
        rtId = getLocalRuntimeEnvironmentID(dbPluginMgr, name, depends);
        if(rtId!=null && !rtId.equals("-1")){
          rteExists = true;
        }
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    // create if not there
    Debug.debug("rteExists: "+rteExists, 3);
    if(!rteExists){
      try{
        dbPluginMgr.createRuntimeEnvironment(rtVals);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not access "+dbPluginMgr.getDBName()+". Cannot" +
        "register runtime environments");
        e.printStackTrace();
      }
    }
    // tag for deletion in any case
    rtId = getLocalRuntimeEnvironmentID(dbPluginMgr, name, depends);
    toDeleteRTEs.put(rtId, dbPluginMgr.getDBName());
  }
  
  private String getLocalRuntimeEnvironmentID(DBPluginMgr dbPluginMgr, String name, String depends) {
    DBResult rtes = dbPluginMgr.getRuntimeEnvironments();
    DBRecord row;
    for(int i=0; i<rtes.values.length; ++i){
      row = rtes.get(i);
      if(csName.equalsIgnoreCase((String) row.getValue("computingSystem")) &&
          name.equalsIgnoreCase((String) row.getValue(MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "name"))) &&
          depends.equalsIgnoreCase((String) row.getValue("depends"))){
        return (String) row.getValue(MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "runtimeEnvironment"));
      }
    }
    return null;
  }
  
  public int run(final MyJobInfo job){
    try{
      if(submit(job)){
        return MyComputingSystem.RUN_OK;
      }
      else{
        return MyComputingSystem.RUN_FAILED;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return MyComputingSystem.RUN_FAILED;
    }
  }

  public boolean submit(JobInfo job){
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+getCommandSuffix((MyJobInfo) job);
    Debug.debug("Executing "+cmd, 2);
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    try{
      ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job),
          ignoreBaseSystemAndVMRTEs, shell.getOSName().toLowerCase().startsWith("windows"));
      if(!scriptGenerator.createWrapper(shell, (MyJobInfo) job, job.getName()+getCommandSuffix((MyJobInfo) job))){
        throw new IOException("Could not create wrapper script.");
      }
      String id = shell.submit(MyUtil.clearFile(cmd),
                                  submitEnvironment,
                                  runDir(job),
                                  MyUtil.clearFile(stdoutFile),
                                  MyUtil.clearFile(stderrFile),
                                  logFile);
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

  public void updateStatus(Vector<JobInfo> jobs) throws Exception{
    for(int i=0; i<jobs.size(); ++i)
      updateStatus((MyJobInfo) jobs.get(i), shell);
  }
  
  protected void updateStatus(MyJobInfo job, Shell shellMgr){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    
    if(shellMgr==null){
      // If there is no ShellMgr, the job was probably started in another session...
      job.setStatusError();
      job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_ERROR));
      return;
    }
    
    // Host.
    job.setHost(shellMgr.getHostName());
    if(!dbPluginMgr.updateJobDefinition(
        job.getIdentifier(),
        new String []{"host"},
        new String []{job.getHost()})){
      logFile.addMessage("DB update of job " + job.getIdentifier()+" failed");    
    }

    if(shellMgr.isRunning(job.getJobId())/*stdOut.length()!=0 &&
        stdOut.indexOf(job.getName())>-1*/
        ){
      job.setStatusRunning();
      job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_RUNNING));
    }
    else{
      if(shellMgr.existsFile(job.getErrTmp())){
        boolean ok = true;
        String stdErr = "";
        try{
          stdErr = shellMgr.readFile(job.getErrTmp());
        }
        catch(Exception e){
          ok = false;
        }
        if(stdErr!=null && stdErr.length()>0){
          ok = false;
        }
        if(!ok){
          job.setStatusError("Done with errors");
          job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_ERROR));
        }
        else{
          job.setStatusDone();
          job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_DONE));
        }
        job.setStatusDone();
        job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_DONE));
      }
      else if(shellMgr.existsFile(job.getOutTmp())){
        job.setStatusDone();
        job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_DONE));
      }
      else{
        // If there is no stdout and no stderr, the job is considered to have failed...
        job.setStatusError();
        job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_ERROR));
      }
    }
  }

  public boolean killJobs(Set<JobInfo> jobsToKill){
    Vector<String> errors = new Vector<String>();
    MyJobInfo job = null;
    for(Iterator<JobInfo> it=jobsToKill.iterator(); it.hasNext();){
      try{
        job = (MyJobInfo) it.next();
        shell.killProcess(job.getJobId(), logFile);
      }
      catch(Exception e){
        errors.add(e.getMessage());
        logFile.addMessage("Exception during job killing :\n" +
                                    "\tJob#\t: " + job.getName() +"\n" +
                                    "\tException\t: " + e.getMessage(), e);
      }
    }
    if(errors.size()!=0){
      error = MyUtil.arrayToString(errors.toArray());
      return false;
    }
    else{
      return true;
    }
  }

  private boolean deleteFile(String url, JobInfo job){
    boolean ret = true;
    if(url==null || url.trim().equals("")){
      return ret;
    }
    try{
      if(!MyUtil.urlIsRemote(url)){
        ret = getShell(job).deleteFile(url);
      }
      else{
        transferControl.deleteFiles(new String [] {url});
      }
    }
    catch(Throwable e){
      error = "WARNING: could not delete "+url+". "+e.getMessage();
      Debug.debug(error, 2);
      ret = false;
    }
    return ret;
  }
  
  private boolean purgeTmpStdoutErr(JobInfo job, Shell thisShell){
    boolean ret = true;
    deleteFile(job.getOutTmp(), job);
    deleteFile(job.getErrTmp(), job);
    //DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    //deleteFile(dbPluginMgr.getStdOutFinalDest(job.getIdentifier()), job);
    //deleteFile(dbPluginMgr.getStdErrFinalDest(job.getIdentifier()), job);
    return ret;
  }


  public boolean cleanup(JobInfo job){
    boolean ret = true;
    String runDir = runDir(job);
    try{
      getShell(job).deleteDir(runDir);
      purgeTmpStdoutErr(job, getShell(job));
    }
    catch(Exception ioe){
      error = "Exception during cleanup of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      ret = false;
    }
    return ret;
  }

  public void exit(){
  }
  
  public void cleanupRuntimeEnvironments(String csName){
    if(toDeleteRTEs==null){
      return;
    }
    String id = "-1";
    String dbName = null;
    boolean ok = true;
    DBPluginMgr dbPluginMgr = null;
    for(Iterator<String> itt = toDeleteRTEs.keySet().iterator(); itt.hasNext();){
      id = itt.next();
      try{
        dbName = toDeleteRTEs.get(id);
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
        if(dbPluginMgr!=null){
          ok = true;
          if(id!=null && !id.equals("-1")){
            ok = dbPluginMgr.deleteRuntimeEnvironment(id);
          }
          else{
            ok = false;
          }
          if(!ok){
            Debug.debug("WARNING: could not delete runtime environment " +
                id+" from database "+dbName, 1);
          }
        }
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("WARNING: could not delete runtime environment " +
            id+" from database "+dbName, 1);
      }
    }
  }

  public String getFullStatus(JobInfo job){
    if(shell.isRunning(job.getJobId())){
      return "Job #"+job.getJobId()+" is running.";
    }
    else{
      return "Job #"+job.getJobId()+" is not running.";
    }
  }

  public String[] getCurrentOutput(JobInfo job) {
    try{
      String stdOutText = shell.readFile(job.getOutTmp());
      String stdErrText = "";
      if(shell.existsFile(job.getErrTmp())){
        stdErrText = shell.readFile(job.getErrTmp());
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
  
  public String[] getScripts(JobInfo job) {
    String jobScriptFile = runDir(job)+"/"+job.getName()+getCommandSuffix((MyJobInfo) job);
    // In case this is not a local shell, first get the script to a local tmp file.
    if(!shell.isLocal()){
      try{
        File tmpFile = File.createTempFile(/*prefix*/MyUtil.getTmpFilePrefix()+"-Fork-", /*suffix*/"");
        // have the file deleted on exit
        GridPilot.addTmpFile(tmpFile.getAbsolutePath(), tmpFile);
        shell.download(jobScriptFile, tmpFile.getAbsolutePath());
        jobScriptFile = tmpFile.getAbsolutePath();
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    return new String [] {jobScriptFile};
  }
    
  public String getUserInfo(String csName){
    if(userName!=null && !userName.equals("")){
      return userName;
    }
    else if(defaultUser!=null){
      return defaultUser;
    }
    else{
      Debug.debug("default user null, using system user", 3);
    }
    String user = null;
    try{
      user = System.getProperty("user.name").trim();
    }
    catch(Exception ioe){
      error = "Exception during getUserInfo\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    return user;
  }
  
  public boolean postProcess(JobInfo job) {
    Debug.debug("Post processing job " + job.getName(), 2);
    boolean ok = true;
    if(copyToFinalDest((MyJobInfo) job, shell)){
      // Delete the run directory
      try{
        ok = cleanup(job);
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

  public boolean preProcess(JobInfo job) throws Exception{
    // create the run directory
    try{
      if(!shell.existsFile(runDir(job))){
        shell.mkdirs(runDir(job));
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not create run directory for job.", e);
      return false;
    }
    if(!setupJobRTEs((MyJobInfo) job, shell)){
      logFile.addMessage("Preparation of job " + job.getIdentifier() +
          " failed. Could not set up runtime environment(s).");
      return false;
    }
    if(!setRemoteOutputFiles((MyJobInfo) job)){
      logFile.addMessage("Preparation of job " + job.getIdentifier() +
          " failed. Could not set up remote output file(s).");
      return false;
    }
    if(!getInputFiles((MyJobInfo) job, shell)){
      logFile.addMessage("Preparation of job " + job.getIdentifier() +
          " failed. Could not copy over input file(s).");
      return false;
    }
    return true;
  }
  
  /**
   * Checks which output files are remote and can be uploaded with
   * command(s) from remoteCopyCommands and adds these
   * with job.setUploadFiles. They will then be taken care of by the
   * job script itself.
   * @param job description of the computing job
   * @return True if the operation completes, false otherwise
   */
  public static boolean setRemoteOutputFiles(MyJobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] outputFiles = dbPluginMgr.getOutputFiles(job.getIdentifier());
    Vector<String> remoteNamesVector = new Vector<String>();
    String remoteName = null;
    Vector<String> outNames = new Vector<String>();
    Vector<String> outDestinations = new Vector<String>();
    boolean ok = true;
    try{
      for(int i=0; i<outputFiles.length; ++i){
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFiles[i]);
        String protocol = remoteName.replaceFirst("^(\\w+):.*$", "$1");
        if(remoteCopyCommands==null || remoteName.equals(protocol) || 
           !remoteCopyCommands.containsKey(protocol)){
          continue;
        }
        // These are considered remote
        if(remoteName!=null && !remoteName.equals("") && !remoteName.startsWith("file:") &&
            !remoteName.startsWith("/") && !remoteName.matches("\\w:.*")){
          remoteNamesVector.add(outputFiles[i]);
        }
        outNames.add(outputFiles[i]);
        outDestinations.add(remoteName);
      }
      if(job.getUploadFiles()==null){
        String [][] uploadFiles = new String [2][remoteNamesVector.size()];
        for(int i=0; i<remoteNamesVector.size(); ++i){
          uploadFiles[0][i] = dbPluginMgr.getJobDefOutLocalName(job.getIdentifier(),
              remoteNamesVector.get(i));
          uploadFiles[1][i] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(),
              remoteNamesVector.get(i));
        }
        job.setUploadFiles(uploadFiles);
      }
      // job.getOutputFile* are used only by GridFactoryComputingSystem and copyToFinalDest
      job.setOutputFileNames(outNames.toArray(new String[outNames.size()]));
      job.setOutputFileDestinations(outDestinations.toArray(new String[outDestinations.size()]));
      Debug.debug("Output files: "+MyUtil.arrayToString(job.getOutputFileNames())+"-->"+
          MyUtil.arrayToString(job.getOutputFileDestinations()), 2);
      //
    }
    catch(Exception e){
      e.printStackTrace();
      ok = false;
    }
    return ok;
  }
  
  /**
   * If a setup script for an RTE required by job is not present
   * and the corresponding tarball URL is set, the tarball is downloaded
   * and extracted/installed and a setup script is written in the runtime directory.
   * @param job
   * @param shell
   * @return false if a required RTE is not present and could not be downloaded.
   */
  protected boolean setupJobRTEs(MyJobInfo job, Shell shell){
    try{
      MyUtil.setupJobRTEs(job, shell, rteMgr,
          GridPilot.getClassMgr().getTransferStatusUpdateControl(),
          runtimeDirectory, runtimeDirectory, false);
      return true;
    }
    catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Copies input files to run directory.
   * Assumes job.stdout points to a file in the run directory.
   * 
   * Notice that we use job.getDownloadFiles() in a different way than GridFactory.
   * 
   * Convention: if a remote copy command is not defined, remote input files will be downloaded
   * and copied directly into the execution host with transferControl.copyInputFile.
   * If a remote copy command is defined, job.getDownloadFiles() will be used to remember which
   * files should be downloaded by the job script itself.
   * 
   * For reference, this is the convention of GridFactory: if a job has already had remote input
   * files downloaded, job.getDownloadFiles() will be set (to local files). These files will
   * then be copied to the run directory along with any local input files.
   *
   */
  protected boolean getInputFiles(MyJobInfo job, Shell thisShellMgr){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String transID = dbPluginMgr.getJobDefExecutableID(job.getIdentifier());
    Debug.debug("Getting input files for executable " + transID, 2);
    String [] transInputFiles = dbPluginMgr.getExecutableInputs(transID);
    Debug.debug("Getting input files for job " + job.getName(), 2);
    String [] jobInputFiles = dbPluginMgr.getJobDefInputFiles(job.getIdentifier());
    String [] dlInputFiles = new String [] {};
    boolean ignoreRemoteInputs = false;
    if(job.getDownloadFiles()!=null && job.getDownloadFiles().length>0){
      dlInputFiles = job.getDownloadFiles();
      Vector<String> jobInputFilesVector = new Vector<String>();
      for(int i=0; i<jobInputFiles.length; ++i){
        if(!MyUtil.urlIsRemote(jobInputFiles[i])){
          jobInputFilesVector.add(jobInputFiles[i]);
        }        
      }
      jobInputFiles = new String [jobInputFilesVector.size()];
      for(int i=0; i<jobInputFiles.length; ++i){
        jobInputFiles[i] = jobInputFilesVector.get(i);
      }
      ignoreRemoteInputs = true;
    }
    job.setDownloadFiles(new String [] {});
    String [] inputFiles = new String [transInputFiles.length+jobInputFiles.length+
                                       dlInputFiles.length];
    for(int i=0; i<transInputFiles.length; ++i){
      inputFiles[i] = transInputFiles[i];
    }
    for(int i=0; i<jobInputFiles.length; ++i){
      inputFiles[i+transInputFiles.length] = jobInputFiles[i];
    }
    for(int i=0; i<dlInputFiles.length; ++i){
      inputFiles[i+transInputFiles.length+jobInputFiles.length] = dlInputFiles[i];
    }
    Vector<String> downloadVector = new Vector<String>();
    String [] downloadFiles = null;
    boolean ok = true;
    for(int i=0; i<inputFiles.length; ++i){
      try{
        inputFiles[i] = Util.fixSrmUrl(inputFiles[i]);
      }
      catch(MalformedURLException e) {
        e.printStackTrace();
      }
      Debug.debug("Pre-processing : Getting " + inputFiles[i], 2);
      String fileName = inputFiles[i];
      String urlDir = "/";
      int lastSlash = inputFiles[i].lastIndexOf(File.separator);
      if(lastSlash>-1){
        fileName = inputFiles[i].substring(lastSlash + 1);
        urlDir = inputFiles[i].substring(0, lastSlash + 1);
      }
      else{
        lastSlash = inputFiles[i].lastIndexOf("/");
        if(lastSlash>-1){
          fileName = inputFiles[i].substring(lastSlash + 1);
          urlDir = inputFiles[i].substring(0, lastSlash + 1);
        }
      }
      fileName = Util.removeQuotes(fileName);
      if(inputFiles[i]!=null && inputFiles[i].trim().length()!=0){
        // Remote shell
        if(!thisShellMgr.isLocal()){
          ok = ok && remoteShellCopy(inputFiles[i], fileName, job, thisShellMgr, ignoreRemoteInputs, downloadVector);
        }
        // Local shell
        else{
          ok = ok && localShellCopy(inputFiles[i], fileName, job, thisShellMgr, ignoreRemoteInputs, urlDir, downloadVector);
        }
      }
    }
    downloadFiles = new String[downloadVector.size()];
    for(int i=0; i<downloadVector.size(); ++i){
      if(downloadVector.get(i)!=null){
        downloadFiles[i] = (String) downloadVector.get(i);
      }
    }
    job.setDownloadFiles(downloadFiles);
    return ok;
  }
  
  private boolean localShellCopy(String inputFile, String fileName, JobInfo job, Shell thisShellMgr,
      boolean ignoreRemoteInputs, String urlDir, AbstractList<String> downloadVector) {
    boolean ok = false;
    // If source starts with file:/, / or c:\ /, just copy over the local file.
    if(inputFile.startsWith("/") ||
        inputFile.matches("\\w:.*") ||
        inputFile.startsWith("file:")){
      inputFile = MyUtil.clearFile(inputFile);
      try{
        if(!thisShellMgr.existsFile(inputFile)){
          ok = false;
          throw new IOException("File " + inputFile + " doesn't exist");
        }
      }
      catch(Throwable e){
        error = "ERROR getting input file: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        ok = false;
      }
      try{
        if(!thisShellMgr.copyFile(inputFile, runDir(job)+"/"+fileName)){
          logFile.addMessage("ERROR: Cannot get input file " + inputFile);
          ok = false;
        }
      }
      catch(Throwable e){
        error = "ERROR getting input file: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        ok = false;
      }
    }
    // If source is remote, get it
    else if(!ignoreRemoteInputs && MyUtil.urlIsRemote(inputFile)){
      try{
        // This method uses fileTransfer.getFile(), which is not implemented by the SRM plugin.
        //transferControl.download(urlDir + fileName, new File(runDir(job)));
        // This method uses the queue() method, which is implemented by all plugins.
        GridPilot.getClassMgr().getTransferStatusUpdateControl().localDownload(
            urlDir + fileName, fileName, new File(runDir(job)), submitTimeout);
      }
      catch(Exception ioe){
        logFile.addMessage("WARNING: GridPilot could not get input file "+inputFile+
            ".", ioe);
        ioe.printStackTrace();
        // If we could not get file natively, as a last resort, try and
        // have the job script get it (assuming that e.g. the runtime environment ARC has been required)
        downloadVector.add(inputFile);
      }
    }
    // Relative paths are not supported
    else{
      logFile.addMessage("ERROR: could not get input file "+inputFile+
          ". Names must be fully qualified.");
      ok = false;
    }
    return ok;
  }

  private boolean remoteShellCopy(String inputFile, String fileName, JobInfo job, Shell thisShellMgr,
      boolean ignoreRemoteInputs, AbstractList<String> downloadVector) {
    boolean ok = true;
    // If source starts with file:/, scp the file from local disk.
    if(inputFile.matches("^file:/*[^/]+.*")){
      inputFile = MyUtil.clearTildeLocally(MyUtil.clearFile(inputFile));
      Debug.debug("Uploading "+fileName+" via SSH: "+inputFile+" --> "+runDir(job)+"/"+fileName, 3);
      ok = thisShellMgr.upload(inputFile, runDir(job)+"/"+fileName);
      if(!ok){
        logFile.addMessage("ERROR: could not put input file "+inputFile);
      }
    }
    // If source starts with /, just use the remote file.
    else if(inputFile.startsWith("/")){
    }
    // If source is remote, have the job script get it
    // (assuming that e.g. the runtime environment ARC has been required)
    else if(!ignoreRemoteInputs && MyUtil.urlIsRemote(inputFile)){
      String protocol = inputFile.replaceFirst("^(\\w+):.*$", "$1");
      try{
        if(remoteCopyCommands!=null && !inputFile.equals(protocol) && 
            remoteCopyCommands.containsKey(protocol)
            ){
          // If a remote copy command is defined and matches the protocol, use it, i.e.
          // have the job script get input files.
          downloadVector.add(inputFile);
        }
        else{
          Debug.debug("Getting input file "+inputFile+" --> "+runDir(job), 2);
          GridPilot.getClassMgr().getTransferStatusUpdateControl().copyInputFile(
              MyUtil.clearFile(inputFile), runDir(job)+"/"+fileName, thisShellMgr, true, submitTimeout, error);
        }
      }
      catch(Exception ioe){
        ok = false;
        logFile.addMessage("WARNING: could not get input file "+inputFile+".", ioe);
        ioe.printStackTrace();
      }
    }
    // Relative paths are not supported
    else{
      logFile.addMessage("ERROR: could not get input file "+inputFile+
          ". Names must be fully qualified.");
      ok = false;
    }
    return ok;
  }

  /**
   * Moves job.StdOut and job.StdErr to final destination specified in the DB. <p>
   * job.StdOut and job.StdErr are then set to these final values. <p>
   * @return <code>true</code> if the move went ok, <code>false</code> otherwise.
   */
  protected boolean copyToFinalDest(MyJobInfo job, Shell shellMgr){
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    
    // Output files
    // Try copying file(s) to output destination
    String jobDefID = job.getIdentifier();
    String [] outputNames = dbPluginMgr.getOutputFiles(jobDefID);
    String localName = null;
    String remoteName = null;
    // This is to identify files that have already ceen copied by the
    // job script itself.
    setRemoteOutputFiles((MyJobInfo) job);
    String [] alreadyCopiedNames;
    boolean ok = true;
    // Horrible clutch because Globus gass copy fails on empty files...
    boolean emptyFile = false;
    for(int i=0; i<outputNames.length; ++i){
      try{
        alreadyCopiedNames = ((MyJobInfo) job).getUploadFiles()[0];
        if(MyUtil.arrayContains(alreadyCopiedNames, outputNames[i])){
          continue;
        }
        localName = runDir(job) +"/"+dbPluginMgr.getJobDefOutLocalName(jobDefID,
            outputNames[i]);
        localName = MyUtil.clearFile(localName);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(jobDefID, outputNames[i]);
        emptyFile = remoteName.startsWith("https") && (new File(localName)).length()==0;
        ok = ok && (transferControl.copyOutputFile(localName, remoteName, shellMgr, error) ||
            emptyFile);
      }
      catch(Exception e){
        // Horrible clutch because Globus gass copy fails on empty files...
        if(!emptyFile){
          job.setStatusError();
          job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_ERROR));
          error = "Exception during copying of output file(s) for job : " + job.getName() + "\n" +
          "\tCommand\t: " + localName + ": -> " + remoteName +"\n" +
          "\tException\t: " + e.getMessage();
          logFile.addMessage(error, e);
          ok = false;
        }
      }
    }
    if(!ok){
      return ok;
    }
    
    // Stdout/stderr
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    
    /**
     * move temp StdOut -> finalStdOut
     */
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      emptyFile = finalStdOut.startsWith("https") && (new File(job.getOutTmp())).length()==0;
      if(transferControl.copyOutputFile(job.getOutTmp(), finalStdOut, shellMgr, error) ||
          emptyFile){
        job.setOutTmp(finalStdOut);
      }
      else{
        ok = false;
      }
    }

    /**
     * move temp StdErr -> finalStdErr
     */
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      emptyFile = finalStdErr.startsWith("https") && (new File(job.getErrTmp())).length()==0;
      if(transferControl.copyOutputFile(job.getErrTmp(), finalStdErr, shellMgr, error) ||
          emptyFile){
        job.setErrTmp(finalStdErr);
      }
      else{
        ok = false;
      }
    }

    return ok;
    
  }
  
  public Shell getShell(JobInfo job){
    return shell;
  }
  
  public String getError(){
    return error;
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