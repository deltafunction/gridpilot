package gridpilot.csplugins.fork;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.ShellMgr;
import gridpilot.TransferControl;
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
  private ShellMgr shellMgr;
  private String workingDir;
  private String commandSuffix;
  private String defaultUser;
  private String userName;
  private String error = "";
  private String runtimeDirectory = null;
  private String transformationDirectory = null;
  private String publicCertificate = null;
  private String remotePullDB = null;
  private String [] localRuntimeDBs = null;
  private HashSet finalRuntimesLocal = null;
  private HashSet finalRuntimesRemote = null;

  public ForkComputingSystem(String _csName){
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    
    try{
      shellMgr = GridPilot.getClassMgr().getShellMgr(csName);
    }
    catch(Exception e){
      Debug.debug("ERROR getting shell manager: "+e.getMessage(), 1);
      return;
    }
    
    defaultUser = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "default user");
    userName = shellMgr.getUserName();

    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(shellMgr.isLocal() && workingDir!=null && workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir!=null && workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    if(workingDir!=null && !shellMgr.existsFile(workingDir)){
      logFile.addInfo("Working directory "+workingDir+" does not exist, creating.");
      shellMgr.mkdirs(workingDir);
    }
    
    commandSuffix = ".sh";
    if(shellMgr.isLocal() && System.getProperty("os.name").toLowerCase().startsWith("windows")){
      commandSuffix = ".bat";
    }
    
    runtimeDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "runtime directory");   
    if(runtimeDirectory!=null && runtimeDirectory.startsWith("~")){
      // Expand ~. Should work for both local and remote shells...
      if(shellMgr.isLocal()){
        runtimeDirectory = System.getProperty("user.home")+runtimeDirectory.substring(1);
      }
      else{
        // remote shells are always non-Windows, so just discard c: and replace \ -> /
        runtimeDirectory = (new File(shellMgr.listFiles(runtimeDirectory)[0])
            ).getParentFile().getAbsolutePath().replaceAll("\\\\", "/"
                ).replaceFirst("^\\w:", "");
      }
    }

    publicCertificate = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "public certificate");
    remotePullDB = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "remote pull database");
    localRuntimeDBs = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "runtime databases");
    
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
    if(transformationDirectory!=null){
      if(!shellMgr.existsFile(transformationDirectory)){
        try{
          shellMgr.mkdirs(transformationDirectory);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
      for(int i=0; i<localRuntimeDBs.length; ++i){  
        createTestTransformation(transformationDirectory, localRuntimeDBs[i]);
      }
    }
  }

  private void createTestTransformation(String transformationDirectory, String localRuntimeDB){
    // If we are running on Linux and a transformation directory is
    // specified in the config file, copy there a test transformation
    
    if(shellMgr.isLocal() && !System.getProperty("os.name").toLowerCase().startsWith("linux")){
      return;
    }
    
    // Create two dummy input files
    if(!shellMgr.existsFile(transformationDirectory+"/data1.txt")){
      try{
        shellMgr.writeFile(transformationDirectory+"/data1.txt", "test data", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(!shellMgr.existsFile(transformationDirectory+"/data2.txt")){
      try{
        shellMgr.writeFile(transformationDirectory+"/data2.txt", "test data", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    
    String testScriptName = "test.sh";
    StringBuffer fileStr = new StringBuffer("");
    
    if(!shellMgr.existsFile(transformationDirectory+"/"+testScriptName)){
      BufferedReader in = null;
      try{
        URL fileURL = GridPilot.class.getResource(
            GridPilot.resourcesPath+testScriptName);
        in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
        String line = null;
        while((line = in.readLine())!=null){
          fileStr.append(line+"\n");
        }
        in.close();
        shellMgr.writeFile(transformationDirectory+"/"+testScriptName, fileStr.toString(), false);
      }
      catch(IOException e){
        logFile.addMessage("WARNING: Could not write test transformation", e);
        return;
      }
      finally{
        try{
          in.close();
        }
        catch(Exception ee){
        }
      }
    }
    DBPluginMgr localDBMgr = null;
    try{
      localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
        localRuntimeDB);
    }
    catch(Exception e){
      logFile.addMessage("WARNING: Could not create test transformation in DB"+localRuntimeDB,
          e);
      return;
    }
    try{
      StringBuffer stdout = new StringBuffer();
      StringBuffer stderr = new StringBuffer();
      String transDir = transformationDirectory;
      if(shellMgr.isLocal()){
        transDir = Util.clearTildeLocally(transDir);
      }
      /*else{
        transDir = transDir.replaceFirst("~", "\\$HOME");
      }*/
      shellMgr.exec(
          "chmod +x "+transDir+"/"+testScriptName, stdout, stderr);
      if(stderr!=null && stderr.length()!=0){
        logFile.addMessage("Could not set transformation executable. "+stderr);
        throw new FileNotFoundException(stderr.toString());
      }
    }
    catch(Exception e){
      Debug.debug("Warning: NOT setting file executable. " +
          "Probably not on UNIX. "+e.getMessage(), 2);
    }

    try{
      if(localDBMgr.getTransformationID("test", "0.1")==null ||
          localDBMgr.getTransformationID("test", "0.1").equals("-1")){
        String [] fields = localDBMgr.getFieldNames("transformation");
        String [] values = new String [fields.length];
        for(int i=0; i<fields.length; ++i){
          if(fields[i].equalsIgnoreCase("name")){
            values[i] = "test";
          }
          else if(fields[i].equalsIgnoreCase("version")){
            values[i] = "0.1";
          }
          else if(fields[i].equalsIgnoreCase("runtimeEnvironmentName")){
            values[i] = "Linux";
          }
          else if(fields[i].equalsIgnoreCase("arguments")){
            values[i] = "multiplier inputFileNames";
          }
          else if(fields[i].equalsIgnoreCase("inputFiles")){
            values[i] = "file:"+transformationDirectory+"/data1.txt "+
            "file:"+transformationDirectory+"/data2.txt";
          }
          else if(fields[i].equalsIgnoreCase("outputFiles")){
            values[i] = "out.txt";
          }
          else if(fields[i].equalsIgnoreCase("script")){
            values[i] = "file:"+transformationDirectory+"/"+testScriptName;
          }
          else if(fields[i].equalsIgnoreCase("comment")){
            values[i] = "Transformation script to test running local GridPilot jobs on Linux.";
          }
          else{
            values[i] = "";
          }
        }
        localDBMgr.createTransformation(values);
      }
    }
    catch(Exception e){
      logFile.addMessage("WARNING: Could not create test transformation in DB"+localRuntimeDB,
          e);
    }
  }
  
  private String runDir(JobInfo job){
    return workingDir +"/"+job.getName();
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
      Debug.debug("WARNING: Could not load remote runtime DB "+
          remoteDBMgr+". Runtime environments must be defined by hand. "+
          e.getMessage(), 1);
    }
    if(remoteDBMgr!=null){
      //Enable the pull button on the monitoring panel
      GridPilot.pullEnabled = true;
      try{
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
   * register the found RTEs in local database with computing system "Fork";
   * register them in remote database (if defined) with computing system "GPSS".
   * @param localDBMgr Local DBPluginMgr
   * @param remoteDBMgr Remote DBPluginMgr
   * @param csName Computing system name
   */
  public void setupRuntimeEnvironments(DBPluginMgr localDBMgr, DBPluginMgr remoteDBMgr,
      String csName){

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
    
    finalRuntimesLocal = new HashSet();
    finalRuntimesRemote = new HashSet();
    HashSet runtimes = shellMgr.listFilesRecursively(runtimeDirectory);
    if(runtimes!=null && runtimes.size()>0){
      String fil = null;
      String hostName = null;
      
      String name = null;
      String cert = null;
      String url = null;
      
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
      
      if(publicCertificate!=null){
        // get the certificate
        try{
          cert = shellMgr.readFile(publicCertificate);
          // TODO: check if certificate includes private key
          // and discard the key if so
        }
        catch(Exception e){
          //e.printStackTrace();
        }
      }

      String [] runtimeEnvironmentFields = null;
      String [] rtVals = null;
      
      if(localDBMgr!=null){
        runtimeEnvironmentFields =
          localDBMgr.getFieldNames("runtimeEnvironment");
        rtVals = new String [runtimeEnvironmentFields.length];
      }
      
      for(Iterator it=runtimes.iterator(); it.hasNext();){
        
        name = null;
        cert = null;
        url = null;

        fil = (String) it.next();
        
        // Get the name
        Debug.debug("File found: "+runtimeDirectory+":"+fil, 3);
        name = fil.substring(runtimeDirectory.length()+1);
        
        if(name!=null && name.length()>0 &&
            /*url!=null && url.length()>0 && */runtimeEnvironmentFields!=null){
          // Write the entry in the local DB
          Debug.debug("runtimeEnvironmentFields: "+runtimeEnvironmentFields.length, 3);
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
          Debug.debug("localDBMgr: "+localDBMgr, 3);
          if(localDBMgr!=null){
            boolean rteExistsLocally = false;
            if(localDBMgr!=null){
              try{
                String rtId = localDBMgr.getRuntimeEnvironmentID(name, csName);
                if(rtId!=null && !rtId.equals("-1")){
                  rteExistsLocally = true;
                }
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
                    
            // create if not there
            Debug.debug("rteExistsLocally: "+rteExistsLocally, 3);
            if(!rteExistsLocally){
              try{
                if(localDBMgr.createRuntimeEnvironment(rtVals)){
                  finalRuntimesLocal.add(name);
                }
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
            // tag for deletion in any case
            else{
              finalRuntimesLocal.add(name);
            }
          }
          
          // Register with remote DB
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
              boolean rteExistsRemotely = false;
              if(remoteDBMgr!=null){
                try{
                  String rtId = remoteDBMgr.getRuntimeEnvironmentID(name, csName);
                  if(rtId!=null && !rtId.equals("-1")){
                    rteExistsRemotely = true;
                  }
                }
                catch(Exception e){
                  e.printStackTrace();
                }
              }
              // Only create and tag for deletion if not already there.
              if(!rteExistsRemotely && remoteDBMgr.createRuntimeEnvironment(rtVals)){
                finalRuntimesRemote.add(name);
              }
            }
            catch(Exception e){
              Debug.debug("WARNING: could not access "+remotePullDB+". Disabling" +
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
    final String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
    final String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
    final String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
    Debug.debug("Executing "+cmd, 2);
    job.setOutputs(stdoutFile, stderrFile);
    ForkScriptGenerator scriptGenerator =
      new ForkScriptGenerator(job.getCSName(), runDir(job));

    scriptGenerator.createWrapper(shellMgr, job, job.getName()+commandSuffix);
    
    try{
      String id = shellMgr.submit(cmd, runDir(job), stdoutFile, stderrFile);
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
    String runtimeName = null;
    String initText = null;
    String id = "-1";
    boolean ok = true;
    DBPluginMgr localDBMgr = null;
    for(int i=0; i<localRuntimeDBs.length; ++i){  
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
            localRuntimeDBs[i]);
      }
      catch(Exception e){
        Debug.debug("Could not load local runtime DB "+localRuntimeDBs[i]+"."+e.getMessage(), 1);
      }
      if(localDBMgr!=null){
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
      }
    }
    if(remotePullDB!=null){
      DBPluginMgr remoteDBMgr = null;
      try{
        remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
          remotePullDB);
      }
      catch(Exception e){
      }
      if(remoteDBMgr!=null){
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

  public String[] getCurrentOutputs(JobInfo job, boolean resyncFirst){
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
    if(defaultUser!=null){
      return defaultUser;
    }
    else{
      Debug.debug("default user null, using system user", 3);
    }
    String user = null;
    try{
      user = System.getProperty("user.name");
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
    return user;
  }
  
  public boolean postProcess(JobInfo job){
    Debug.debug("Post processing job " + job.getName(), 2);
    String runDir = runDir(job);
    boolean ok = true;
    if(copyToFinalDest(job)){
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
    
    
    
    // TODO: rethink this...
    //if(job.getDownloadedFiles()!=null && job.getDownloadedFiles().length>0){
    //  // job has already had remote input files downloaded (by PullJobsDaemon).
    //  return true;
    //}
    
    
    
    //else{
      if(!shellMgr.isLocal()){
        try{
          writeUserProxy();
        }
        catch(Exception e){
          logFile.addMessage("WARNING: could not cwrite user proxy.", e);
        }
      }
      return setRemoteOutputFiles(job) && getInputFiles(job);
    //}
  }
  
  private void writeUserProxy() throws IOException{
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
  private boolean setRemoteOutputFiles(JobInfo job){
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
   * Copies input files to run directory.
   * Assumes job.stdout points to a file in the run directory.
   */
  private boolean getInputFiles(JobInfo job){
    boolean ok = true;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String transID = dbPluginMgr.getJobDefTransformationID(job.getJobDefId());
    Debug.debug("Getting input files for transformation " + transID, 2);
    String [] transInputFiles = dbPluginMgr.getTransformationInputs(transID);
    Debug.debug("Getting input files for job " + job.getName(), 2);
    String [] jobInputFiles = dbPluginMgr.getJobDefInputFiles(job.getJobDefId());
    String [] inputFiles = new String [transInputFiles.length+jobInputFiles.length];
    for(int i=0; i<transInputFiles.length; ++i){
      inputFiles[i] = transInputFiles[i];
    }
    for(int i=0; i<jobInputFiles.length; ++i){
      inputFiles[i+transInputFiles.length] = jobInputFiles[i];
    }
    Vector downloadVector = new Vector();
    String [] downloadFiles = null;
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
            inputFiles[i] = Util.clearFile(inputFiles[i]);
            ok = shellMgr.upload(inputFiles[i], runDir(job)+"/"+fileName);
            if(!ok){
              logFile.addMessage("ERROR: could not put input file "+inputFiles[i]);
            }
          }
          // If source starts with /, just use the remote file.
          else if(inputFiles[i].startsWith("/")){
          }
          // If source is remote, have the job script get it
          // (assuming that e.g. the runtime environment ARC has been required)
          else if(!inputFiles[i].matches("^file:/*[^/]+.*") &&
              inputFiles[i].matches("^[a-z]+:/*[^/]+.*")){
            downloadVector.add(inputFiles[i]);
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
          else if(!inputFiles[i].matches("^file:/*[^/]+.*") &&
              inputFiles[i].matches("^[a-z]+:/*[^/]+.*")){
            try{
              TransferControl.download(urlDir + fileName,
                  new File(runDir(job)), GridPilot.getClassMgr().getGlobalFrame().getContentPane());
            }
            catch(Exception ioe){
              logFile.addMessage("WARNING: GridPilot could not get input file "+inputFiles[i]+
                  ".", ioe);
              ioe.printStackTrace();
              // If we could not get file natively, have the job script get it
              // (assuming that e.g. the runtime environment ARC has been required)
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
   * (from AtCom1)
   */
  private boolean copyToFinalDest(JobInfo job){
    
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    
    // Output files
    // Try copying file(s) to output destination
    String jobDefID = job.getJobDefId();
    String [] outputNames = dbPluginMgr.getOutputFiles(jobDefID);
    String localName = null;
    String remoteName = null;
    boolean ok = true;
    for(int i=0; i<outputNames.length; ++i){
      try{
        localName = runDir(job) +"/"+dbPluginMgr.getJobDefOutLocalName(jobDefID,
            outputNames[i]);
        localName = Util.clearFile(localName);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(jobDefID, outputNames[i]);
        ok = ok && copyOutputFile(localName, remoteName);
      }
      catch(Exception e){
        job.setJobStatus("Error");
        job.setInternalStatus(ComputingSystem.STATUS_ERROR);
        error = "Exception during copying of output file(s) for job : " + job.getName() + "\n" +
        "\tCommand\t: " + localName + ": -> " + remoteName +"\n" +
        "\tException\t: " + e.getMessage();
        logFile.addMessage(error, e);
        ok = false;
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
    ok = false;   
    if(finalStdOut!=null && finalStdOut.trim().length()>0){
      if(copyOutputFile(job.getStdOut(), finalStdOut)){
        job.setStdOut(finalStdOut);
        ok = true;
      }
    }

    /**
     * move temp StdErr -> finalStdErr
     */
    ok = false;   
    if(finalStdErr!=null && finalStdErr.trim().length()>0){
      if(copyOutputFile(job.getStdErr(), finalStdErr)){
        job.setStdErr(finalStdErr);
        ok = true;
      }
    }

    return ok;
    
  }
  
  private boolean copyOutputFile(String src, String dest){
    if(dest.startsWith("file:") || dest.startsWith("/") ||
        dest.matches("\\w:.*") && shellMgr.isLocal()){
      try{
        if(!shellMgr.existsFile(src)){
          error = "Post processing : File " + src + " doesn't exist";
          logFile.addMessage(error);
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for "+src+": "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
        return false;
      }
      // by convention, if the destination starts with file:, we get the file to the local disk
      if(dest.startsWith("file:") && !shellMgr.isLocal()){
        Debug.debug("Post processing : getting " + src + " -> " + dest, 2);
        try{
          String realDest = Util.clearTildeLocally(Util.clearFile(dest));
          File realParentDir = (new File(realDest)).getParentFile();
          Debug.debug("Post processing : real locations: " + src + " -> " + realDest, 2);
          // Check if parent dir exists, create if not
          if(!realParentDir.exists()){
            try{
              if(!realParentDir.mkdir()){
                throw new Exception("Error creating directory "+realParentDir.getAbsolutePath());
              }
            }
            catch(Exception e){
              error = "ERROR: could not create directory for job outputs.";
              logFile.addMessage(error, e);
              return false;
            }
          }
          if(!shellMgr.download(src, realDest)){
            error = "Post processing : Cannot get \n\t" +
            src + "\n to \n\t" + dest;
            logFile.addMessage(error);
            return false;
          }
        }
        catch(Throwable e){
          error = "ERROR getting "+src+": "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
          return false;
        }
      }
      // if we have a fully qualified name (and for Windows a local shell), we just copy it with the shellMgr
      else{
        Debug.debug("Post processing : renaming " + src + " in " + dest, 2);
        try{
          if(!shellMgr.copyFile(src, dest)){
            error = "Post processing : Cannot move \n\t" + src +
            "\n into \n\t" + dest;
            logFile.addMessage(error);
            return false;
          }
        }
        catch(Throwable e){
          error = "ERROR copying "+src+": "+e.getMessage();
          Debug.debug(error, 2);
          logFile.addMessage(error);
          return false;
        }
      }
    }
    // relative paths or getting files from a Windows server is not supported
    else{
      error = "ERROR copying : unqualified paths or getting files from a " +
          "Windows server is not supported.";
      logFile.addMessage(error);
      return false;
    }
    return true;
  }
  
  public String getError(String csName){
    return error;
  }

}