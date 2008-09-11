package gridpilot.csplugins.fork;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Enumeration;
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
import gridfactory.common.VirtualMachine;

import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.MyJobInfo;
import gridpilot.GridPilot;
import gridpilot.MySSL;
import gridpilot.RteRdfParser;
import gridpilot.TransferControl;
import gridpilot.MyUtil;

public class ForkComputingSystem implements MyComputingSystem{

  protected String [] env = {
    "STATUS_WAIT="+MyJobInfo.STATUS_READY,
    "STATUS_RUNNING="+MyJobInfo.STATUS_RUNNING,
    "STATUS_DONE="+MyJobInfo.STATUS_DONE,
    "STATUS_ERROR="+MyJobInfo.STATUS_ERROR,
    "STATUS_FAILED="+MyJobInfo.STATUS_FAILED};

  protected LogFile logFile;
  protected String csName;
  protected Shell shellMgr;
  protected String workingDir;
  protected String commandSuffix;
  protected String defaultUser;
  protected String userName;
  protected String error = "";
  protected String runtimeDirectory = null;
  protected String transformationDirectory = null;
  protected String publicCertificate = null;
  protected String remotePullDB = null;
  protected String [] localRuntimeDBs = null;
  protected HashSet toCleanupRTEs = null;
  protected DBPluginMgr remoteDBPluginMgr = null;
  // List of (Janitor) catalogs from where to get RTEs
  protected String [] rteCatalogUrls = null;

  public ForkComputingSystem(String _csName) throws Exception{
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    
    try{
      remoteDBPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(remotePullDB);
    }
    catch(Exception e){
      Debug.debug("WARNING: Could not load remote pull DB "+
          remoteDBPluginMgr+". Runtime environments must be defined by hand. "+
          e.getMessage(), 1);
    }
    
    try{
      shellMgr = GridPilot.getClassMgr().getShellMgr(csName);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get shell manager: "+e.getMessage(), 1);
      if(csName.equalsIgnoreCase("fork")){
        throw e;
      }
    }
    
    defaultUser = configFile.getValue("GridPilot", "default user");
    try{
      userName = shellMgr.getUserName();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    workingDir = configFile.getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(System.getProperty("os.name").toLowerCase().startsWith("windows") &&
        shellMgr.isLocal() && workingDir!=null && workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir!=null && workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    if(workingDir!=null && !shellMgr.existsFile(workingDir)){
      logFile.addInfo("Working directory "+workingDir+" does not exist, creating.");
      shellMgr.mkdirs(workingDir);
    }
    Debug.debug("Using workingDir "+workingDir, 2);
    
    commandSuffix = ".sh";
    if(shellMgr.isLocal() && System.getProperty("os.name").toLowerCase().startsWith("windows")){
      commandSuffix = ".bat";
    }
    
    runtimeDirectory = configFile.getValue(csName, "runtime directory");   
    if(runtimeDirectory!=null && runtimeDirectory.startsWith("~")){
      // Expand ~
      if(System.getProperty("os.name").toLowerCase().startsWith("windows") &&
          shellMgr.isLocal()){
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
    
    rteCatalogUrls = configFile.getValues("GridPilot", "runtime catalog URLs");

    publicCertificate = configFile.getValue(csName, "public certificate");
    remotePullDB = configFile.getValue(csName, "remote pull database");
    localRuntimeDBs = configFile.getValues(csName, "runtime databases");
    
    Debug.debug("Setting up RTEs for "+csName, 2);
    if(runtimeDirectory!=null){
      if(!shellMgr.existsFile(runtimeDirectory)){
        logFile.addInfo("Runtime directory "+runtimeDirectory+" does not exist, creating.");
        shellMgr.mkdirs(runtimeDirectory);
      }
      setupRuntimeEnvironments(csName);
    }
    transformationDirectory = configFile.getValue(csName, "transformation directory");   
    //if(shellMgr.isLocal() && transformationDirectory!=null && transformationDirectory.startsWith("~")){
    //  transformationDirectory = System.getProperty("user.home")+transformationDirectory.substring(1);
    //}
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
        scanRTEDir(localDBMgr, i>0?null:remoteDBPluginMgr, thisCs, shellMgr);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(localRuntimeDBs.length==0 && remoteDBPluginMgr!=null){
      scanRTEDir(null, remoteDBPluginMgr, thisCs, shellMgr);
    }
    syncRTEsFromCatalogs();
  }
  
  /**
   * Scan runtime environment directory for runtime environment setup scripts;
   * register the found RTEs in local database with computing system cs;
   * register them in remote database (if defined) with computing system "GPSS".
   * @param localDBMgr Local DBPluginMgr
   * @param remoteDBMgr Remote DBPluginMgr
   * @param cs Computing system name
   */
  protected void scanRTEDir(DBPluginMgr localDBMgr, DBPluginMgr remoteDBMgr,
      String cs, Shell mgr){

    if(mgr.isLocal() &&
        System.getProperty("os.name").toLowerCase().startsWith("linux") ||
        // remote shells always run on Linux
        !mgr.isLocal()){
      Debug.debug("Setting up Linux runtime environment.", 3);
      try{
        try{
          if(!mgr.existsFile(runtimeDirectory)){
            mgr.mkdirs(runtimeDirectory);
          }
        }
        catch(Exception e){
          logFile.addMessage("ERROR: could not create runtimeDirectory "+runtimeDirectory+" with ShellMgr on "+mgr.getHostName(), e);
          return;
        }    
        String filePath = null;
        filePath = runtimeDirectory+"/"+"Linux";
        if(!mgr.existsFile(filePath)){
          Debug.debug("Writing "+filePath, 3);
          mgr.writeFile(filePath, "# This is a dummy runtime environment" +
                " description file. Its presence just means that we are running on Linux.", false);
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: Could not create Linux runtime environment file",
            e);
      }
    }
    
    String name = null;
    String deps = "";
    
    toCleanupRTEs = new HashSet();
    HashSet runtimes = mgr.listFilesRecursively(runtimeDirectory);
    String [] expandedRuntimeDirs = mgr.listFiles(MyUtil.clearFile(runtimeDirectory));
    String dirName = null;
    if(shellMgr.isLocal() && System.getProperty("os.name").toLowerCase().startsWith("windows")){
      dirName = runtimeDirectory.replaceFirst("^.*\\([^\\]+)$", "$1");
    }
    else{
      dirName = runtimeDirectory.replaceFirst("^.*/([^/]+)$", "$1");
    }
    if(expandedRuntimeDirs.length==1 && expandedRuntimeDirs[0].endsWith(dirName)){
      Debug.debug("No RTE files in "+runtimeDirectory, 2);
      return;
    }
    String expandedRuntimeDir = null;
    if(shellMgr.isLocal() && System.getProperty("os.name").toLowerCase().startsWith("windows")){
      expandedRuntimeDir = expandedRuntimeDirs[0].replaceFirst("^(.*\\)[^\\]+$", "$1");
    }
    else{
      expandedRuntimeDir = expandedRuntimeDirs[0].replaceFirst("^(.*/)[^/]+$", "$1");
    }

    if(runtimes!=null && runtimes.size()>0){
      String fil = null;      
      for(Iterator it=runtimes.iterator(); it.hasNext();){
        
        name = null;
        fil = (String) it.next();
        
        // Get the name
        Debug.debug("File found: "+expandedRuntimeDir+":"+fil, 3);
        name = fil.substring(expandedRuntimeDir.length());
        if(name.toLowerCase().endsWith(".gz") || name.toLowerCase().endsWith(".tar") ||
            name.toLowerCase().endsWith(".tgz") || name.toLowerCase().endsWith(".zip") ||
            mgr.isDirectory(name)){
          continue;
        }
        // Read dependencies from the file.
        // The notation is:
        // # ARC_RTE_DEP=<RTE name 1>
        // # ARC_RTE_DEP=<RTE name 2>
        // ...
        try{
          String content = mgr.readFile(fil);
          InputStream dis = new ByteArrayInputStream(content.getBytes());
          BufferedReader in = new BufferedReader(new InputStreamReader(dis));
          String line = null;
          String depPattern = "^\\S*#\\sARC_RTE_DEP=([^#]+).*";
          while((line=in.readLine())!=null){
            if(line.matches(depPattern)){
              if(deps.length()>0){
                deps += " ";
              }
              deps += line.replaceFirst(depPattern, "$1");
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
          Debug.debug("Writing RTE "+name+" in local DB "+localDBMgr.getDBName(), 3);
          createRTE(localDBMgr, name, cs, deps, null, null);
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
  
  protected void createRTE(DBPluginMgr dbPluginMgr, String name, String csName,
      String depends, String cert, String url){
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
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("url") && url!=null){
        rtVals[i] = url;
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
        rtVals[i] = csName;
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("depends") && depends!=null){
        rtVals[i] = depends;
      }
      else if(runtimeEnvironmentFields[i].equalsIgnoreCase("certificate") && cert!=null){
        rtVals[i] = cert;
      }
      else{
        rtVals[i] = "";
      }
    }
    boolean rteExists = false;
    if(dbPluginMgr!=null){
      try{
        String rtId = dbPluginMgr.getRuntimeEnvironmentID(name, csName);
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
        if(dbPluginMgr.createRuntimeEnvironment(rtVals)){
          toCleanupRTEs.add(new String [] {name, csName, dbPluginMgr.getDBName()});
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not access "+dbPluginMgr.getDBName()+". Disabling" +
        "registration of runtime environments");
        e.printStackTrace();
      }
    }
    // tag for deletion in any case
    else{
      toCleanupRTEs.add(new String [] {name, csName, dbPluginMgr.getDBName()});
    }
  }
  
  public boolean submit(JobInfo job){
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
    try{
      ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job));
      if(!scriptGenerator.createWrapper(shellMgr, (MyJobInfo) job, job.getName()+commandSuffix)){
        throw new IOException("Could not create wrapper script.");
      }
      String id = shellMgr.submit(MyUtil.clearTildeLocally(MyUtil.clearFile(cmd)),
                                  runDir(job),
                                  MyUtil.clearTildeLocally(MyUtil.clearFile(stdoutFile)),
                                  MyUtil.clearTildeLocally(MyUtil.clearFile(stderrFile)),
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
      updateStatus((MyJobInfo) jobs.get(i), shellMgr);
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
        shellMgr.killProcess(job.getJobId(), logFile);
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
      shellMgr.deleteDir(runDir);
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
      ret = false;
    }
    return ret;
  }

  public void exit(){
    try{
      cleanupRuntimeEnvironments(csName);
    }
    catch(Exception e){
      e.printStackTrace();
      Debug.debug("WARNING: could not cleanup runtime environments.", 1);
    }
  }
  
  public void cleanupRuntimeEnvironments(String csName){
    if(toCleanupRTEs==null){
      return;
    }
    String runtimeName = null;
    String myCSName = null;
    String initText = null;
    String id = "-1";
    boolean ok = true;
    DBPluginMgr dbPluginMgr = null;
    String [] triplet = null;
    for(Iterator itt = toCleanupRTEs.iterator(); itt.hasNext();){
      triplet = (String []) itt.next();
      try{
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(triplet[2]);
        if(dbPluginMgr!=null){
          ok = true;
          runtimeName = triplet[0];
          myCSName = triplet[1];
          // Don't delete records with a non-empty initText.
          // These can only have been created by hand.
          initText = null;
          try{
            initText = dbPluginMgr.getRuntimeInitText(runtimeName, myCSName);
          }
          catch(Exception e){
            e.printStackTrace();
          }
          if(initText!=null && !initText.equals("")){
            continue;
          }
          id = dbPluginMgr.getRuntimeEnvironmentID(runtimeName, myCSName);
          if(!id.equals("-1")){
            ok = dbPluginMgr.deleteRuntimeEnvironment(id);
          }
          else{
            ok = false;
          }
          if(!ok){
            Debug.debug("WARNING: could not delete runtime environment " +
                runtimeName+" from database "+triplet[2], 1);
          }
        }
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("WARNING: could not delete runtime environment " +
            runtimeName+" from database "+triplet[2], 1);
      }
    }
  }

  public String getFullStatus(JobInfo job){
    if(shellMgr.isRunning(job.getJobId())){
      return "Job #"+job.getJobId()+" is running.";
    }
    else{
      return "Job #"+job.getJobId()+" is not running.";
    }
  }

  public String[] getCurrentOutput(JobInfo job) {
    try{
      String stdOutText = shellMgr.readFile(job.getOutTmp());
      String stdErrText = "";
      if(shellMgr.existsFile(job.getErrTmp())){
        stdErrText = shellMgr.readFile(job.getErrTmp());
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
    String jobScriptFile = runDir(job)+"/"+job.getName()+commandSuffix;
    // In case this is not a local shell, first get the script to a local tmp file.
    if(!shellMgr.isLocal()){
      try{
        File tmpFile = File.createTempFile(/*prefix*/"GridPilot-Fork-", /*suffix*/"");
        // hack to have the file deleted on exit
        GridPilot.tmpConfFile.put(tmpFile.getAbsolutePath(), tmpFile);
        shellMgr.download(jobScriptFile, tmpFile.getAbsolutePath());
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
    if(copyToFinalDest((MyJobInfo) job, shellMgr)){
      // Delete the run directory
      try{
        ok = shellMgr.deleteDir(runDir);
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
      if(!shellMgr.existsFile(runDir(job))){
        shellMgr.mkdirs(runDir(job));
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: could not create run directory for job.", e);
      return false;
    }    
    if(!shellMgr.isLocal()){
      try{
        writeUserProxy(shellMgr);
      }
      catch(Exception e){
        logFile.addMessage("WARNING: could not write user proxy.", e);
      }
    }
    return setupJobRTEs((MyJobInfo) job, shellMgr) &&
       setRemoteOutputFiles((MyJobInfo) job) && getInputFiles((MyJobInfo) job, shellMgr);
  }
  
  protected void writeUserProxy(Shell shellMgr) throws IOException{
    try{
      StringBuffer stdout = new StringBuffer();
      StringBuffer stderr = new StringBuffer();
      if(shellMgr.exec("id -u", stdout, stderr)!=0 ||
          stderr!=null && stderr.length()!=0){
        //logFile.addMessage("Could not get user id. "+stderr);
        throw new FileNotFoundException(stderr.toString());
      }
      String uid = stdout.toString().trim();
      shellMgr.upload(MySSL.getProxyFile().getAbsolutePath(), "/tmp/x509up_u"+uid);     
    }
    catch(Exception e){
      throw new IOException("WARNING: NOT writing user proxy. " +"Probably not on UNIX. "+e.getMessage());
    }
  }
  
  /**
   * Checks output files for remote URLs and adds these
   * with job.setUploadFiles
   * @param job description of the computing job
   * @return True if the operation completes, false otherwise
   */
  protected boolean setRemoteOutputFiles(MyJobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] outputFiles = dbPluginMgr.getOutputFiles(job.getIdentifier());
    Vector remoteNamesVector = new Vector();
    String remoteName = null;
    boolean ok = true;
    try{
      for(int i=0; i<outputFiles.length; ++i){
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFiles[i]);
        // These are considered remote
        if(remoteName!=null && !remoteName.equals("") && !remoteName.startsWith("file:") &&
            !remoteName.startsWith("/") && !remoteName.matches("\\w:.*")){
          remoteNamesVector.add(outputFiles[i]);
        }
      }
      String [][] remoteNames = new String [remoteNamesVector.size()][2];
      for(int i=0; i<remoteNamesVector.size(); ++i){
        remoteNames[i][0] = dbPluginMgr.getJobDefOutLocalName(job.getIdentifier(),
            (String )remoteNamesVector.get(i));
        remoteNames[i][1] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(),
            (String) remoteNamesVector.get(i));
      }
      job.setUploadFiles(remoteNames);
    }
    catch(Exception e){
      e.printStackTrace();
      ok = false;
    }
    return ok;
  }
  
  /**
   * If "runtime catalog URLs" is defined, copies records from them
   * to the 'local' runtime DBs. The copying is not done if there's
   * already a record with the same name and CS.
    */
  private void syncRTEsFromCatalogs(){
    if(rteCatalogUrls==null){
      return;
    }
    DBPluginMgr localDBMgr = null;
    RteRdfParser rteRdfParser = new RteRdfParser(rteCatalogUrls, csName);
    String id = null;
    String rteNameField = null;
    String newId = null;
    Debug.debug("Syncing RTEs from catalogs to DBs: "+MyUtil.arrayToString(localRuntimeDBs), 2);
    for(int ii=0; ii<localRuntimeDBs.length; ++ii){
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(localRuntimeDBs[ii]);
        DBResult rtes = rteRdfParser.getDBResult(localDBMgr);
        Debug.debug("Checking RTEs "+rtes.values.length, 3);
        for(int i=0; i<rtes.values.length; ++i){
          Debug.debug("Checking RTE "+MyUtil.arrayToString(rtes.getRow(i).values), 3);
          id = null;
          // Check if RTE already exists
          rteNameField = MyUtil.getNameField(localDBMgr.getDBName(), "runtimeEnvironment");
          id = localDBMgr.getRuntimeEnvironmentID(
              (String) rtes.getRow(i).getValue(rteNameField), csName);
          if(id==null || id.equals("-1")){
            if(localDBMgr.createRuntimeEnvironment(rtes.getRow(i).values)){
              Debug.debug("Created RTE "+MyUtil.arrayToString(rtes.getRow(i).values), 2);
              // Tag for deletion
              String name = (String) rtes.getRow(i).getValue(rteNameField);
              newId = localDBMgr.getRuntimeEnvironmentID(name , csName);
              if(newId!=null && !newId.equals("-1")){
                Debug.debug("Tagging for deletion "+name+":"+newId, 3);
                toCleanupRTEs.add(new String [] {name, csName, localDBMgr.getDBName()});
              }
            }
            else{
              Debug.debug("WARNING: Failed creating RTE "+MyUtil.arrayToString(rtes.getRow(i).values), 2);
            }
          }
        }
      }
      catch(Exception e){
        error = "Could not load local runtime DB "+localRuntimeDBs[ii]+"."+e.getMessage();
        Debug.debug(error, 1);
        e.printStackTrace();
      }
    }
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
    if(runtimeDirectory==null || runtimeDirectory.length()==0 ||
        !shell.existsFile(runtimeDirectory)){
      logFile.addMessage("ERROR: could not download RTE to "+runtimeDirectory);
      return false;
    }
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String jobDefId = job.getIdentifier();
    String transID = dbPluginMgr.getJobDefTransformationID(jobDefId);
    DBRecord transformation = dbPluginMgr.getTransformation(transID);
    String rteNamesString = (String) transformation.getValue(
        MyUtil.getTransformationRuntimeReference(job.getDBName())[1]);
    String [] rteNames = MyUtil.split(rteNamesString);
    String rteId = null;
    DBRecord rteRecord = null;
    RteInstaller rteInstaller = null;
    String url = null;
    String [] deps = new String [0];
    String rteName = null;
    for(int i=0; i<rteNames.length; ++i){
      try{
        rteId = dbPluginMgr.getRuntimeEnvironmentID(rteNames[i], job.getCSName());
        if(rteId==null || rteId.equals("") || rteId.equals("-1")){
          logFile.addMessage("runtimeEnvironment "+rteNames[i]+" not found for computing system "+job.getCSName());
          return false;
        }
        rteRecord = dbPluginMgr.getRuntimeEnvironment(rteId);
        String depsStr = (String) rteRecord.getValue("depends");
        if(depsStr!=null && !depsStr.equals("")){
          deps = MyUtil.splitUrls((String) rteRecord.getValue("depends"));
        }
        for(int j=0; j<deps.length+1; ++j){
          // First see if the dependencies are there or will install.
          // Then, install the RTE.
          if(j==deps.length){
            rteName = rteNames[i];
          }
          else{
            rteName = deps[i];
          }
          rteId = dbPluginMgr.getRuntimeEnvironmentID(rteName, job.getCSName());
          if(rteId==null || rteId.equals("") || rteId.equals("-1")){
            logFile.addMessage("runtimeEnvironment "+rteNames[i]+" not found for computing system "+job.getCSName());
            return false;
          }
          if(toCleanupRTEs.contains(rteNames[i])){
            // RTE comes from local RTE dir (DB record written on initialization). If this shell is not local,
            // Copy "Linux" RTE file to remote host. Others are problematic as there is no way to
            // tell where all their files are from the setup script.
            // DROPPED: Better to switch completely to Janitor RTEs.
            /*if(!shellMgr.isLocal() && rteName.equalsIgnoreCase("Linux")){
              try{
                shellMgr.upload(runtimeDirectory+"/"+rteName, runtimeDirectory+"/"+rteName);
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }*/
            continue;
          }
          rteRecord = dbPluginMgr.getRuntimeEnvironment(rteId);
          url = (String) rteRecord.getValue("url");
          if(url!=null && !url.equals("null") && !url.equals("")){
            // Notice that we use the same directory to keep RTEs on both the local host and the (remote)
            // shell host
            rteInstaller = new RteInstaller(url, runtimeDirectory, runtimeDirectory, rteNames[i], shell);
            try{
              rteInstaller.install();
            }
            catch(Exception e){
              e.printStackTrace();
              logFile.addInfo("ERROR: RTE "+rteNames[i]+" could not be installed. " +
                  "Presumably it is already installed. Trying to run job...");
              //return false;
            }
          }
          // If there's no URL the RTE is a local one
          /*else{
            logFile.addMessage("ERROR: RTE "+rteNames[i]+" cannot be installed " +
                  "dynamically; no URL found.");
            return false;
          }*/
        }
      }
      catch(Exception e){
        logFile.addMessage("ERROR: could not install RTE "+rteNames[i], e);
        e.printStackTrace();
        return false;
      }
    }
    return true;
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

    // CONVENTION: if job has already had remote input files downloaded (by PullJobsDaemon),
    // job.getDownloadFiles() will be set (to local files). These files should then be copied to the
    // run directory along with any local input files.
    // Moreover, the remote files from transInputFiles and jobInputFiles should be ignored.
    String [] pullInputFiles = new String [] {};
    boolean ignoreRemoteInputs = false;
    if(job.getDownloadFiles()!=null && job.getDownloadFiles().length>0){
      pullInputFiles = job.getDownloadFiles();
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
                                       pullInputFiles.length];
    for(int i=0; i<transInputFiles.length; ++i){
      inputFiles[i] = transInputFiles[i];
    }
    for(int i=0; i<jobInputFiles.length; ++i){
      inputFiles[i+transInputFiles.length] = jobInputFiles[i];
    }
    for(int i=0; i<pullInputFiles.length; ++i){
      inputFiles[i+transInputFiles.length+jobInputFiles.length] = pullInputFiles[i];
    }
    Vector downloadVector = new Vector();
    String [] downloadFiles = null;
    // TODO: clean up this mess!
    for(int i=0; i<inputFiles.length; ++i){
      Debug.debug("Pre-processing : Getting " + inputFiles[i], 2);
      String fileName = inputFiles[i];
      String urlDir = "/";
      int lastSlash = inputFiles[i].lastIndexOf("/");
      if(lastSlash>-1){
        fileName = inputFiles[i].substring(lastSlash + 1);
        urlDir = inputFiles[i].substring(0, lastSlash + 1);
      }
      if(inputFiles[i]!=null && inputFiles[i].trim().length()!=0){
        // Remote shell
        if(!thisShellMgr.isLocal()){
          // If source starts with file:/, scp the file from local disk.
          if(inputFiles[i].matches("^file:/*[^/]+.*")){
            inputFiles[i] = MyUtil.clearTildeLocally(MyUtil.clearFile(inputFiles[i]));
            ok = thisShellMgr.upload(inputFiles[i], runDir(job)+"/"+fileName);
            if(!ok){
              logFile.addMessage("ERROR: could not put input file "+inputFiles[i]);
            }
          }
          // If source starts with /, just use the remote file.
          else if(inputFiles[i].startsWith("/")){
          }
          // If source is remote and the job is not pulled, have the job script get it
          // (assuming that e.g. the runtime environment ARC has been required)
          else if(!ignoreRemoteInputs && MyUtil.urlIsRemote(inputFiles[i])){
            try{
              Debug.debug("Getting input file "+inputFiles[i]+" --> "+runDir(job), 3);
              TransferControl.copyInputFile(inputFiles[i], runDir(job)+"/"+fileName, thisShellMgr, error, logFile);
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
              TransferControl.download(urlDir + fileName, new File(runDir(job)));
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
        ok = ok && (TransferControl.copyOutputFile(localName, remoteName, shellMgr, error, logFile) ||
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
      if(TransferControl.copyOutputFile(job.getOutTmp(), finalStdOut, shellMgr, error, logFile) ||
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
      if(TransferControl.copyOutputFile(job.getErrTmp(), finalStdErr, shellMgr, error, logFile) ||
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
    return shellMgr;
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