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

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.DBRecord;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.ShellMgr;
import gridpilot.TransferControl;
import gridpilot.Util;

public class ForkComputingSystem implements ComputingSystem{

  protected String [] env = {
    "STATUS_WAIT="+ComputingSystem.STATUS_WAIT,
    "STATUS_RUNNING="+ComputingSystem.STATUS_RUNNING,
    "STATUS_DONE="+ComputingSystem.STATUS_DONE,
    "STATUS_ERROR="+ComputingSystem.STATUS_ERROR,
    "STATUS_FAILED="+ComputingSystem.STATUS_FAILED};

  protected LogFile logFile;
  protected String csName;
  protected ShellMgr shellMgr;
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

  public ForkComputingSystem(String _csName) throws Exception{
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    
    try{
      shellMgr = GridPilot.getClassMgr().getShellMgr(csName);
    }
    catch(Exception e){
      Debug.debug("WARNING: could not get shell manager: "+e.getMessage(), 1);
      if(csName.equalsIgnoreCase("fork")){
        throw e;
      }
    }
    
    defaultUser = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "default user");
    try{
      userName = shellMgr.getUserName();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(csName, "working directory");
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
    
    runtimeDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "runtime directory");   
    if(runtimeDirectory!=null && runtimeDirectory.startsWith("~")){
      // Expand ~. Should work for both local and remote shells...
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

    publicCertificate = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "public certificate");
    remotePullDB = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "remote pull database");
    localRuntimeDBs = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "runtime databases");
    
    Debug.debug("Setting up RTEs for "+csName, 2);
    if(runtimeDirectory!=null){
      if(!shellMgr.existsFile(runtimeDirectory)){
        logFile.addInfo("Runtime directory "+runtimeDirectory+" does not exist, creating.");
        shellMgr.mkdirs(runtimeDirectory);
      }
      setupRuntimeEnvironments(csName);
    }
    transformationDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "transformation directory");   
    //if(shellMgr.isLocal() && transformationDirectory!=null && transformationDirectory.startsWith("~")){
    //  transformationDirectory = System.getProperty("user.home")+transformationDirectory.substring(1);
    //}
  }
  
  protected String runDir(JobInfo job){
    return Util.clearTildeLocally(Util.clearFile(workingDir +"/"+job.getName()));
  }
  
  /**
   * By convention the runtime environments are defined by the
   * scripts in the directory specified in the config file (runtime directory).
   */
  public void setupRuntimeEnvironments(String csName){
    DBPluginMgr remoteDBMgr = null;
    try{
      remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(remotePullDB);
    }
    catch(Exception e){
      Debug.debug("WARNING: Could not load remote pull DB "+
          remoteDBMgr+". Runtime environments must be defined by hand. "+
          e.getMessage(), 1);
    }
    if(remoteDBMgr!=null){
      //Enable the pull button on the monitoring panel
      GridPilot.pullEnabled = true;
      try{
        Debug.debug("Enabling pulling of jobs", 2);
        GridPilot.getClassMgr().getGlobalFrame().monitoringPanel.jobMonitor.setPullEnabled(true);
      }
      catch(Exception e){
      }
    }
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
      if(i>0){
        remoteDBMgr = null;
      }
      try{
        setupRuntimeEnvironments(localDBMgr, remoteDBMgr, csName);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(localRuntimeDBs.length==0 && remoteDBMgr!=null){
      setupRuntimeEnvironments(null, remoteDBMgr, csName);
    }
  }

  /**
   * Scan runtime environment directory for runtime environment setup scripts;
   * register the found RTEs in local database with computing system cs;
   * register them in remote database (if defined) with computing system "GPSS".
   * @param localDBMgr Local DBPluginMgr
   * @param remoteDBMgr Remote DBPluginMgr
   * @param cs Computing system name
   */
  public void setupRuntimeEnvironments(DBPluginMgr localDBMgr, DBPluginMgr remoteDBMgr,
      String cs){

    if(shellMgr.isLocal() &&
        System.getProperty("os.name").toLowerCase().startsWith("linux") ||
        // remote shells always run on Linux
        !shellMgr.isLocal()){
      Debug.debug("Setting up runtime environments...", 3);
      try{
        String filePath = null;
        filePath = runtimeDirectory+"/"+"Linux";
        if(!shellMgr.existsFile(filePath)){
          Debug.debug("Writing "+filePath, 3);
          shellMgr.writeFile(filePath, "# This is a dummy runtime environment" +
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
    String cert = getCertificate(shellMgr);
    String url = getUrl();
    
    toCleanupRTEs = new HashSet();
    HashSet runtimes = shellMgr.listFilesRecursively(runtimeDirectory);
    if(runtimes!=null && runtimes.size()>0){
      String fil = null;      
      for(Iterator it=runtimes.iterator(); it.hasNext();){
        
        name = null;
        fil = (String) it.next();
        
        // Get the name
        Debug.debug("File found: "+runtimeDirectory+":"+fil, 3);
        name = fil.substring(Util.clearTildeLocally(Util.clearFile(runtimeDirectory)).length()+1);
        if(name.toLowerCase().endsWith(".gz") || name.toLowerCase().endsWith(".tar") ||
            name.toLowerCase().endsWith(".tgz") || name.toLowerCase().endsWith(".zip") ||
            shellMgr.isDirectory(name)){
          continue;
        }
        // Read dependencies from the file.
        // The notation is:
        // # ARC_RTE_DEP=<RTE name 1>
        // # ARC_RTE_DEP=<RTE name 2>
        // ...
        try{
          String content = shellMgr.readFile(fil);
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
          // Register with local and remote DB with CS "GPSS"
          if(cert!=null && cert.length()>0 && remoteDBMgr!=null){
            createRTE(localDBMgr, name, "GPSS", deps, cert, null);
            createRTE(remoteDBMgr, name, "GPSS", deps, cert, url);
            createRTE(remoteDBMgr, name, cs, deps, null, null);
          }         
          else{
            logFile.addMessage("WARNING: no certificate or no remote DB. Disabling remote registration of " +
                    "runtime environments.");
          }
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
    return url;
  }
  
  protected String getCertificate(ShellMgr shellMgr){
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
  
  public boolean submit(final JobInfo job){
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    job.setOutputs(stdoutFile, stderrFile);
    try{
      ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(job.getCSName(), runDir(job));
      if(!scriptGenerator.createWrapper(shellMgr, job, job.getName()+commandSuffix)){
        throw new IOException("Could not create wrapper script.");
      }
      String id = shellMgr.submit(Util.clearTildeLocally(Util.clearFile(cmd)),
                                  runDir(job),
                                  Util.clearTildeLocally(Util.clearFile(stdoutFile)),
                                  Util.clearTildeLocally(Util.clearFile(stderrFile)));
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
      updateStatus((JobInfo) jobs.get(i), shellMgr);
  }
  
  protected void updateStatus(JobInfo job, ShellMgr shellMgr){
    
    // Host.
    job.setHost(shellMgr.getHostName());

    if(shellMgr.isRunning(job.getJobId())/*stdOut.length()!=0 &&
        stdOut.indexOf(job.getName())>-1*/
        ){
      job.setJobStatus("Running");
      job.setInternalStatus(ComputingSystem.STATUS_RUNNING);
    }
    else{
      if(shellMgr.existsFile(job.getStdErr())){
        boolean ok = true;
        String stdErr = "";
        try{
          stdErr = shellMgr.readFile(job.getStdErr());
        }
        catch(Exception e){
          ok = false;
        }
        if(stdErr!=null && stdErr.length()>0){
          ok = false;
        }
        if(!ok){
          job.setJobStatus("Done with errors");
        }
        else{
          job.setJobStatus("Done");
        }
        job.setInternalStatus(ComputingSystem.STATUS_DONE);
      }
      else if(shellMgr.existsFile(job.getStdOut())){
        job.setJobStatus("Done");
        job.setInternalStatus(ComputingSystem.STATUS_DONE);
      }
      else{
        // If there is no stdout and no stderr, the job is considered to have failed...
        job.setJobStatus("Error");
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
      }
    }
  }

  public boolean killJobs(Vector jobsToKill){
    Vector errors = new Vector();
    JobInfo job = null;
    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      try{
        job = (JobInfo) en.nextElement();
        shellMgr.killProcess(job.getJobId());
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
      shellMgr.deleteDir(runDir);
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
  }

  public void exit(){
    cleanupRuntimeEnvironments(csName);
  }
  
  public void cleanupRuntimeEnvironments(String csName){
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
  
  public boolean postProcess(JobInfo job){
    Debug.debug("Post processing job " + job.getName(), 2);
    String runDir = runDir(job);
    boolean ok = true;
    if(copyToFinalDest(job, shellMgr)){
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

  public boolean preProcess(JobInfo job){
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
    return setupJobRTEs(job, shellMgr) &&
       setRemoteOutputFiles(job) && getInputFiles(job, shellMgr);
  }
  
  protected void writeUserProxy(ShellMgr shellMgr) throws IOException{
    try{
      StringBuffer stdout = new StringBuffer();
      StringBuffer stderr = new StringBuffer();
      if(shellMgr.exec("id -u", stdout, stderr)!=0 ||
          stderr!=null && stderr.length()!=0){
        //logFile.addMessage("Could not get user id. "+stderr);
        throw new FileNotFoundException(stderr.toString());
      }
      String uid = stdout.toString().trim();
      shellMgr.upload(Util.getProxyFile().getAbsolutePath(), "/tmp/x509up_u"+uid);     
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
  protected boolean setRemoteOutputFiles(JobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] outputFiles = dbPluginMgr.getOutputFiles(job.getJobDefId());
    Vector remoteNamesVector = new Vector();
    String remoteName = null;
    boolean ok = true;
    try{
      for(int i=0; i<outputFiles.length; ++i){
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(), outputFiles[i]);
        // These are considered remote
        if(remoteName!=null && !remoteName.equals("") && !remoteName.startsWith("file:") &&
            !remoteName.startsWith("/") && !remoteName.matches("\\w:.*")){
          remoteNamesVector.add(outputFiles[i]);
        }
      }
      String [][] remoteNames = new String [remoteNamesVector.size()][2];
      for(int i=0; i<remoteNamesVector.size(); ++i){
        remoteNames[i][0] = dbPluginMgr.getJobDefOutLocalName(job.getJobDefId(),
            (String )remoteNamesVector.get(i));
        remoteNames[i][1] = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(),
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
   * If a setup script for an RTE required by job is not present
   * and the corresponding tarball URL is set, the tarball is downloaded
   * and extracted/installed and a setup script is written in the runtime directory.
   * @param job
   * @param shellMgr
   * @return false if a required RTE is not present and could not be downloaded.
   */
  protected boolean setupJobRTEs(JobInfo job, ShellMgr shellMgr){
    if(runtimeDirectory==null || runtimeDirectory.length()==0 ||
        !(new File(Util.clearTildeLocally(Util.clearFile(runtimeDirectory)))).exists()){
      logFile.addMessage("ERROR: could not download RTE to "+runtimeDirectory);
      return false;
    }
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String jobDefId = job.getJobDefId();
    String transID = dbPluginMgr.getJobDefTransformationID(jobDefId);
    DBRecord transformation = dbPluginMgr.getTransformation(transID);
    String rteNamesString = (String) transformation.getValue(
        Util.getTransformationRuntimeReference(job.getDBName())[1]);
    String [] rteNames = Util.split(rteNamesString);
    String rteId = null;
    DBRecord rteRecord = null;
    RteInstaller rteInstaller = null;
    String url = null;
    String [] deps = new String [0];
    String rteName = null;
    for(int i=0; i<rteNames.length; ++i){
      try{
        rteId = dbPluginMgr.getRuntimeEnvironmentID(rteNames[i], job.getCSName());
        rteRecord = dbPluginMgr.getRuntimeEnvironment(rteId);
        String depsStr = (String) rteRecord.getValue("depends");
        if(depsStr!=null && !depsStr.equals("")){
          deps = Util.splitUrls((String) rteRecord.getValue("depends"));
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
          if(rteId==null || rteId.equals("-1")){
            logFile.addMessage("ERROR: RTE "+rteNames[i]+" not found.");
            return false;
          }
          if(toCleanupRTEs.contains(rteNames[i])){
            continue;
          }
          rteRecord = dbPluginMgr.getRuntimeEnvironment(rteId);
          url = (String) rteRecord.getValue("url");
          if(url!=null && !url.equals("null") && !url.equals("")){
            rteInstaller = new RteInstaller(url, runtimeDirectory, rteNames[i], shellMgr);
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
  protected boolean getInputFiles(JobInfo job, ShellMgr shellMgr){
    
    boolean ok = true;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String transID = dbPluginMgr.getJobDefTransformationID(job.getJobDefId());
    Debug.debug("Getting input files for transformation " + transID, 2);
    String [] transInputFiles = dbPluginMgr.getTransformationInputs(transID);

    Debug.debug("Getting input files for job " + job.getName(), 2);
    String [] jobInputFiles = dbPluginMgr.getJobDefInputFiles(job.getJobDefId());

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
        if(!Util.urlIsRemote(jobInputFiles[i])){
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
        if(!shellMgr.isLocal()){
          // If source starts with file:/, scp the file from local disk.
          if(inputFiles[i].matches("^file:/*[^/]+.*")){
            inputFiles[i] = Util.clearTildeLocally(Util.clearFile(inputFiles[i]));
            ok = shellMgr.upload(inputFiles[i], runDir(job)+"/"+fileName);
            if(!ok){
              logFile.addMessage("ERROR: could not put input file "+inputFiles[i]);
            }
          }
          // If source starts with /, just use the remote file.
          else if(inputFiles[i].startsWith("/")){
          }
          // If source is remote and the job is not pulled, have the job script get it
          // (assuming that e.g. the runtime environment ARC has been required)
          else if(!ignoreRemoteInputs && Util.urlIsRemote(inputFiles[i])){
            try{
              Debug.debug("Getting input file "+inputFiles[i]+" --> "+runDir(job), 3);
              TransferControl.copyInputFile(inputFiles[i], runDir(job)+"/"+fileName, shellMgr, error, logFile);
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
            inputFiles[i] = Util.clearFile(inputFiles[i]);
            try{
              if(!shellMgr.existsFile(inputFiles[i])){
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
              if(!shellMgr.copyFile(inputFiles[i], runDir(job)+"/"+fileName)){
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
          else if(!ignoreRemoteInputs && Util.urlIsRemote(inputFiles[i])){
            try{
              TransferControl.download(urlDir + fileName,
                  new File(runDir(job)), GridPilot.getClassMgr().getGlobalFrame().getContentPane());
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
  protected boolean copyToFinalDest(JobInfo job, ShellMgr shellMgr){
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    
    // Output files
    // Try copying file(s) to output destination
    String jobDefID = job.getJobDefId();
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
        localName = Util.clearFile(localName);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(jobDefID, outputNames[i]);
        emptyFile = remoteName.startsWith("https") && (new File(localName)).length()==0;
        ok = ok && (TransferControl.copyOutputFile(localName, remoteName, shellMgr, error, logFile) ||
            emptyFile);
      }
      catch(Exception e){
        // Horrible clutch because Globus gass copy fails on empty files...
        if(!emptyFile){
          job.setJobStatus("Error");
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
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
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    
    /**
     * move temp StdOut -> finalStdOut
     */
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      emptyFile = finalStdOut.startsWith("https") && (new File(job.getStdOut())).length()==0;
      if(TransferControl.copyOutputFile(job.getStdOut(), finalStdOut, shellMgr, error, logFile) ||
          emptyFile){
        job.setStdOut(finalStdOut);
      }
      else{
        ok = false;
      }
    }

    /**
     * move temp StdErr -> finalStdErr
     */
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      emptyFile = finalStdErr.startsWith("https") && (new File(job.getStdErr())).length()==0;
      if(TransferControl.copyOutputFile(job.getStdErr(), finalStdErr, shellMgr, error, logFile) ||
          emptyFile){
        job.setStdErr(finalStdErr);
      }
      else{
        ok = false;
      }
    }

    return ok;
    
  }
  
  public String getError(String csName){
    return error;
  }
}