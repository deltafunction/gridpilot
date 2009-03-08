package gridpilot.csplugins.fork;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
import gridfactory.common.LogFile;
import gridfactory.common.Shell;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.common.jobrun.VirtualMachine;

import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.MyJobInfo;
import gridpilot.GridPilot;
import gridpilot.MyTransferControl;
import gridpilot.MyUtil;

public class ForkComputingSystem implements MyComputingSystem{
  
  private RTEMgr rteMgr;

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
  protected String transformationDirectory = null;
  protected String publicCertificate = null;
  protected String [] runtimeDBs = null;
  protected HashMap toDeleteRTEs = null;
  // List of (Janitor) catalogs from where to get RTEs
  protected String [] rteCatalogUrls = null;
  protected MyTransferControl transferControl;
  protected boolean mkLocalOSRTE = true;
  protected boolean includeVMRTEs = true;
  protected String [] basicOSRTES = {"Linux"};

  protected boolean ignoreBaseSystemAndVMRTEs = true;

  public ForkComputingSystem(String _csName) throws Exception{
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    transferControl = GridPilot.getClassMgr().getTransferControl();
    toDeleteRTEs = new HashMap();
    
    GridPilot.splashShow("Setting up shells...");
    
    try{
      shell = GridPilot.getClassMgr().getShellMgr(csName);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get shell manager: "+e.getMessage(), 1);
      if(csName.equalsIgnoreCase("fork")){
        throw e;
      }
    }
    
    if(shell.getOS().toLowerCase().startsWith("windows")){
      basicOSRTES = new String [] {"Windows"};
    }
    
    defaultUser = configFile.getValue(GridPilot.topConfigSection, "default user");
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
        shell.isLocal() && workingDir!=null && workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir!=null && workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    if(workingDir!=null && !shell.existsFile(workingDir)){
      logFile.addInfo("Working directory "+workingDir+" does not exist, creating.");
      shell.mkdirs(workingDir);
    }
    Debug.debug("Using workingDir "+workingDir, 2);
    
    runtimeDirectory = GridPilot.runtimeDir;
    if(runtimeDirectory!=null && runtimeDirectory.startsWith("~")){
      // Expand ~
      if(MyUtil.onWindows() &&
          shell.isLocal()){
        runtimeDirectory = System.getProperty("user.home")+runtimeDirectory.substring(1);
      }
      else{
        // remote shells are always non-Windows, so just discard c: and replace \ -> /
        // Hmm, and if the directory is empty...
        /*runtimeDirectory = (new File(shellMgr.listFiles(runtimeDirectory)[0])
            ).getParentFile().getAbsolutePath().replaceAll("\\\\", "/"
                ).replaceFirst("^\\w:", "");*/
      }
    }
    
    rteCatalogUrls = configFile.getValues(GridPilot.topConfigSection, "runtime catalog URLs");

    publicCertificate = configFile.getValue(csName, "public certificate");
    runtimeDBs = configFile.getValues(csName, "runtime databases");
    
    GridPilot.splashShow("Setting up RTEs for "+csName);
    Debug.debug("Setting up RTEs for "+csName, 2);
    if(runtimeDirectory!=null){
      if(!shell.existsFile(runtimeDirectory)){
        logFile.addInfo("Runtime directory "+runtimeDirectory+" does not exist, creating.");
        shell.mkdirs(runtimeDirectory);
      }
    }
    transformationDirectory = configFile.getValue(csName, "transformation directory");   
    //if(shellMgr.isLocal() && transformationDirectory!=null && transformationDirectory.startsWith("~")){
    //  transformationDirectory = System.getProperty("user.home")+transformationDirectory.substring(1);
    //}
    MyUtil.checkAndActivateSSL(rteCatalogUrls);
    rteMgr = GridPilot.getClassMgr().getRTEMgr(runtimeDirectory, rteCatalogUrls);
  }
  
  protected String getCommandSuffix(MyJobInfo job){
    Shell thisShell = null;
    try{
      thisShell = GridPilot.getClassMgr().getShellMgr(job);
    }
    catch(Exception e){
    }
    String commandSuffix = ".sh";
    if(thisShell!=null){
      if(thisShell.isLocal() && MyUtil.onWindows()){
        commandSuffix = ".bat";
      }
    }
    else{
      if(shell.isLocal() && MyUtil.onWindows()){
        commandSuffix = ".bat";
      }
    }
    return commandSuffix;
  }
  
  protected String runDir(JobInfo job){
    if(getShell(job).isLocal()){
      return MyUtil.clearTildeLocally(MyUtil.clearFile(workingDir +"/"+job.getName()));
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
    HashSet runtimes = mgr.listFilesRecursively(runtimeDirectory);
    String [] expandedRuntimeDirs = mgr.listFiles(MyUtil.clearFile(runtimeDirectory));
    String dirName = null;
    if(shell.isLocal() && MyUtil.onWindows()){
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
    if(shell.isLocal() && MyUtil.onWindows()){
      expandedRuntimeDir = expandedRuntimeDirs[0].replaceFirst("^(.*)\\\\[^\\\\]+$", "$1");
      expandedRuntimeDir = expandedRuntimeDir.replaceFirst("^(.*)\\\\[^\\\\]+\\$", "$1")+"\\";
    }
    else{
      expandedRuntimeDir = expandedRuntimeDirs[0].replaceFirst("^(.*)/[^/]+$", "$1");
      expandedRuntimeDir = expandedRuntimeDir.replaceFirst("^(.*)/[^/]+/$", "$1")+"/";
    }

    if(runtimes!=null && runtimes.size()>0){
      String fil = null;      
      for(Iterator it=runtimes.iterator(); it.hasNext();){
        
        name = null;
        fil = (String) it.next();
        
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
          depends = mgr.getOS()+(mgr.getProvides()==null?"":(" "+MyUtil.arrayToString(mgr.getProvides())));
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
        hostName = MyUtil.getIPNumber();
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    // if we cannot get the host name, try to get the IP address
    if(hostName==null){
      try{
        hostName = MyUtil.getIPAddress();
      }
      catch(Exception e){
        e.printStackTrace();
      }
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

  public boolean submit(JobInfo job){
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+getCommandSuffix((MyJobInfo) job);
    Debug.debug("Executing "+cmd, 2);
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    try{
      ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job),
          ignoreBaseSystemAndVMRTEs );
      if(!scriptGenerator.createWrapper(shell, (MyJobInfo) job, job.getName()+getCommandSuffix((MyJobInfo) job))){
        throw new IOException("Could not create wrapper script.");
      }
      String id = shell.submit(MyUtil.clearFile(cmd),
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

  public void updateStatus(Vector<JobInfo> jobs){
    for(int i=0; i<jobs.size(); ++i)
      updateStatus((MyJobInfo) jobs.get(i), shell);
  }
  
  protected void updateStatus(MyJobInfo job, Shell shellMgr){
    
    if(shellMgr==null){
      // If there is no ShellMgr, the job was probably started in another session...
      job.setStatusError();
      job.setCSStatus(JobInfo.getStatusName(JobInfo.STATUS_ERROR));
      return;
    }
    
    // Host.
    job.setHost(shellMgr.getHostName());

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

  public boolean killJobs(Vector<JobInfo> jobsToKill){
    Vector errors = new Vector();
    MyJobInfo job = null;
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (MyJobInfo) en.nextElement();
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

  public boolean cleanup(JobInfo job){
    boolean ret = true;
    String runDir = runDir(job);
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());

    // Delete files that may have been copied to final destination.
    // Files starting with file: are considered to locally available, accessed
    // with shellMgr
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String fileName;
    Vector remoteFiles = new Vector();
    for(int i=0; i<outputFileNames.length; ++i){
      fileName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
      if(fileName.startsWith("file:")){
        shell.deleteFile(fileName);
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
      transferControl.deleteFiles(remoteFilesArr);
    }
    catch(Exception e){
      error = "WARNING: could not delete output files. "+e.getMessage();
      Debug.debug(error, 3);
    }
    
    // Delete stdout/stderr that may have been copied to final destination
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      try{
        if(finalStdOut.startsWith("file:")){
          shell.deleteFile(finalStdOut);
        }
        else{
          transferControl.deleteFiles(new String [] {finalStdOut});
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
          shell.deleteFile(finalStdErr);
        }
        else{
          transferControl.deleteFiles(new String [] {finalStdErr});
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
      shell.deleteDir(runDir);
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
    String initText = null;
    String id = "-1";
    String dbName = null;
    boolean ok = true;
    DBPluginMgr dbPluginMgr = null;
    for(Iterator itt = toDeleteRTEs.keySet().iterator(); itt.hasNext();){
      id = (String) itt.next();
      try{
        dbName = (String) toDeleteRTEs.get(id);
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
    String runDir = runDir(job);
    boolean ok = true;
    if(copyToFinalDest((MyJobInfo) job, shell)){
      // Delete the run directory
      try{
        ok = shell.deleteDir(runDir);
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
    return setupJobRTEs((MyJobInfo) job, shell) &&
       setRemoteOutputFiles((MyJobInfo) job) && getInputFiles((MyJobInfo) job, shell);
  }
  
  /**
   * Checks output files for remote URLs and adds these
   * with job.setUploadFiles
   * @param job description of the computing job
   * @return True if the operation completes, false otherwise
   */
  public static boolean setRemoteOutputFiles(MyJobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] outputFiles = dbPluginMgr.getOutputFiles(job.getIdentifier());
    Vector remoteNamesVector = new Vector();
    String remoteName = null;
    Vector<String> outNames = new Vector<String>();
    Vector<String> outDestinations = new Vector<String>();
    boolean ok = true;
    try{
      for(int i=0; i<outputFiles.length; ++i){
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFiles[i]);
        // These are considered remote
        if(remoteName!=null && !remoteName.equals("") && !remoteName.startsWith("file:") &&
            !remoteName.startsWith("/") && !remoteName.matches("\\w:.*")){
          remoteNamesVector.add(outputFiles[i]);
        }
        outNames.add(outputFiles[i]);
        outDestinations.add(remoteName);
      }
      String [][] remoteNames = new String [remoteNamesVector.size()][2];
      for(int i=0; i<remoteNamesVector.size(); ++i){
        remoteNames[i][0] = dbPluginMgr.getJobDefOutLocalName(job.getIdentifier(),
            (String )remoteNamesVector.get(i));
        remoteNames[i][1] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(),
            (String) remoteNamesVector.get(i));
      }
      job.setUploadFiles(remoteNames);
      // This is used only by GridFactoryComputingSystem
      job.setOutputFileNames(outNames.toArray(new String[outNames.size()]));
      job.setOutputFileDestinations(outDestinations.toArray(new String[outDestinations.size()]));
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
          runtimeDirectory, runtimeDirectory);
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
   */
  protected boolean getInputFiles(MyJobInfo job, Shell thisShellMgr){
    
    boolean ok = true;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String transID = dbPluginMgr.getJobDefTransformationID(job.getIdentifier());
    Debug.debug("Getting input files for transformation " + transID, 2);
    String [] transInputFiles = dbPluginMgr.getTransformationInputs(transID);

    Debug.debug("Getting input files for job " + job.getName(), 2);
    String [] jobInputFiles = dbPluginMgr.getJobDefInputFiles(job.getIdentifier());

    // CONVENTION: if job has already had remote input files downloaded,
    // job.getDownloadFiles() will be set (to local files). These files should then be copied to the
    // run directory along with any local input files.
    // Moreover, the remote files from transInputFiles and jobInputFiles should be ignored.
    String [] dlInputFiles = new String [] {};
    boolean ignoreRemoteInputs = false;
    if(job.getDownloadFiles()!=null && job.getDownloadFiles().length>0){
      dlInputFiles = job.getDownloadFiles();
      Vector jobInputFilesVector = new Vector();
      for(int i=0; i<jobInputFiles.length; ++i){
        if(!MyUtil.urlIsRemote(jobInputFiles[i])){
          jobInputFilesVector.add(jobInputFiles[i]);
        }        
      }
      jobInputFiles = new String [jobInputFilesVector.size()];
      for(int i=0; i<jobInputFiles.length; ++i){
        jobInputFiles[i] = (String) jobInputFilesVector.get(i);
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
    Vector downloadVector = new Vector();
    String [] downloadFiles = null;
    // TODO: clean up this mess! Use method from MyUtil
    for(int i=0; i<inputFiles.length; ++i){
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
      if(inputFiles[i]!=null && inputFiles[i].trim().length()!=0){
        // Remote shell
        if(!thisShellMgr.isLocal()){
          // If source starts with file:/, scp the file from local disk.
          if(inputFiles[i].matches("^file:/*[^/]+.*")){
            inputFiles[i] = MyUtil.clearTildeLocally(MyUtil.clearFile(inputFiles[i]));
            Debug.debug("Uploading "+fileName+" via SSH: "+inputFiles[i]+" --> "+runDir(job)+"/"+fileName, 3);
            ok = thisShellMgr.upload(inputFiles[i], runDir(job)+"/"+fileName);
            if(!ok){
              logFile.addMessage("ERROR: could not put input file "+inputFiles[i]);
            }
          }
          // If source starts with /, just use the remote file.
          else if(inputFiles[i].startsWith("/")){
          }
          // If source is remote, have the job script get it
          // (assuming that e.g. the runtime environment ARC has been required)
          else if(!ignoreRemoteInputs && MyUtil.urlIsRemote(inputFiles[i])){
            try{
              Debug.debug("Getting input file "+inputFiles[i]+" --> "+runDir(job), 3);
              transferControl.copyInputFile(MyUtil.clearFile(inputFiles[i]), runDir(job)+"/"+fileName, thisShellMgr, true, error);
            }
            catch(Exception ioe){
              logFile.addMessage("WARNING: GridPilot could not get input file "+inputFiles[i]+
                  ".", ioe);
              ioe.printStackTrace();
              // If we could not get file natively, as a last resort, try and
              // have the job script get it (assuming that e.g. the runtime environment ARC has been required)
              downloadVector.add(inputFiles[i]);
            }
          }
          // Relative paths are not supported
          else{
            logFile.addMessage("ERROR: could not get input file "+inputFiles[i]+
                ". Names must be fully qualified.");
            ok = false;
          }
        }
        // Local shell
        else{
          // If source starts with file:/, / or c:\ /, just copy over the local file.
          if(inputFiles[i].startsWith("/") ||
             inputFiles[i].matches("\\w:.*") ||
             inputFiles[i].startsWith("file:")){
            inputFiles[i] = MyUtil.clearFile(inputFiles[i]);
            try{
              if(!thisShellMgr.existsFile(inputFiles[i])){
                logFile.addMessage("File " + inputFiles[i] + " doesn't exist");
                ok = false;
                continue;
              }
            }
            catch(Throwable e){
              error = "ERROR getting input file: "+e.getMessage();
              Debug.debug(error, 2);
              logFile.addMessage(error);
              ok = false;
            }
            try{
              if(!thisShellMgr.copyFile(inputFiles[i], runDir(job)+"/"+fileName)){
                logFile.addMessage("ERROR: Cannot get input file " + inputFiles[i]);
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
          else if(!ignoreRemoteInputs && MyUtil.urlIsRemote(inputFiles[i])){
            try{
              transferControl.download(urlDir + fileName, new File(runDir(job)));
            }
            catch(Exception ioe){
              logFile.addMessage("WARNING: GridPilot could not get input file "+inputFiles[i]+
                  ".", ioe);
              ioe.printStackTrace();
              // If we could not get file natively, as a last resort, try and
              // have the job script get it (assuming that e.g. the runtime environment ARC has been required)
              downloadVector.add(inputFiles[i]);
            }
          }
          // Relative paths are not supported
          else{
            logFile.addMessage("ERROR: could not get input file "+inputFiles[i]+
                ". Names must be fully qualified.");
            ok = false;
          }
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
    boolean ok = true;
    // Horrible clutch because Globus gass copy fails on empty files...
    boolean emptyFile = false;
    for(int i=0; i<outputNames.length; ++i){
      try{
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