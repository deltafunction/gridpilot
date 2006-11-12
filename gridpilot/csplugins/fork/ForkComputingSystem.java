package gridpilot.csplugins.fork;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
  private String csName;
  private LocalShellMgr shellMgr;
  private String workingDir;
  private String commandSuffix;
  private String defaultUser;
  private String error = "";
  private String runtimeDirectory = null;
  private String transformationDirectory = null;
  private String publicCertificate = null;
  private String remoteDB = null;
  private String localRuntimeDB = null;
  private HashSet finalRuntimesLocal = null;
  private HashSet finalRuntimesRemote = null;

  public ForkComputingSystem(String _csName){
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    shellMgr = new LocalShellMgr();
    
    workingDir = GridPilot.getClassMgr().getConfigFile().getValue(csName, "working directory");
    if(workingDir==null || workingDir.equals("")){
      workingDir = "~";
    }
    if(workingDir.startsWith("~")){
      workingDir = System.getProperty("user.home")+workingDir.substring(1);
    }
    if(workingDir.endsWith("/") || workingDir.endsWith("\\")){
      workingDir = workingDir.substring(0, workingDir.length()-1);
    }
    if(!shellMgr.existsFile(workingDir)){
      logFile.addInfo("Working directory "+workingDir+" does not exist, creating.");
      shellMgr.mkdirs(workingDir);
    }
    
    commandSuffix = ".sh";
    if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      commandSuffix = ".bat";
    }
    
    runtimeDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "runtime directory");   
    if(runtimeDirectory!=null && runtimeDirectory.startsWith("~")){
      runtimeDirectory = System.getProperty("user.home")+runtimeDirectory.substring(1);
    }

    defaultUser = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "default user");
    publicCertificate = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "public certificate");
    remoteDB = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "remote database");
    localRuntimeDB = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "runtime database");
    
    if(runtimeDirectory!=null){
      if(!shellMgr.existsFile(runtimeDirectory)){
        logFile.addInfo("Runtime directory "+runtimeDirectory+" does not exist, creating.");
        shellMgr.mkdirs(runtimeDirectory);
      }
      setupRuntimeEnvironments(csName);
    }
    transformationDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "transformation directory");   
    if(transformationDirectory!=null && transformationDirectory.startsWith("~")){
      transformationDirectory = System.getProperty("user.home")+transformationDirectory.substring(1);
    }
    if(transformationDirectory!=null){
      if(!shellMgr.existsFile(transformationDirectory)){
        try{
          shellMgr.mkdirs(transformationDirectory);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
      createTestTransformation(transformationDirectory);
    }
  }

  private void createTestTransformation(String transformationDirectory){
    // If we are running on Linux and a transformation directory is
    // specified in the config file, copy there a test transformation
    
    if(shellMgr.isLocal() && !System.getProperty("os.name").toLowerCase().startsWith("linux")){
      return;
    }
    
    // Create two dummy input files
    if(!shellMgr.existsFile("/tmp/file1.root")){
      try{
        shellMgr.writeFile("/tmp/file1.root", "", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(!shellMgr.existsFile("/tmp/file2.root")){
      try{
        shellMgr.writeFile("/tmp/file2.root", "", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    
    String testScriptName = "test.sh";
    File testScript = new File(transformationDirectory, testScriptName);
    
    if(!testScript.exists()){
      BufferedReader in = null;
      PrintWriter out = null;
      try{
        URL fileURL = GridPilot.class.getResource(
            GridPilot.resourcesPath+"/"+testScriptName);
        in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
        out = new PrintWriter(testScript);
        String line = null;
        while((line = in.readLine())!=null){
          out.println(line);
        }
        in.close();
        out.close();
      }
      catch(IOException e){
        logFile.addMessage("WARNING: Could not write test transformation", e);
        return;
      }
      finally{
        try{
          in.close();
          out.close();
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
      shellMgr.exec(
          "chmod +x "+testScript.getAbsolutePath(), stdout, stderr);
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
            values[i] = "file1.root file2.root multiplier";
          }
          else if(fields[i].equalsIgnoreCase("inputFiles")){
            values[i] = "/tmp/file1.root /tmp/file2.root";
          }
          else if(fields[i].equalsIgnoreCase("outputFiles")){
            values[i] = "result.txt";
          }
          else if(fields[i].equalsIgnoreCase("script")){
            values[i] = testScript.getAbsolutePath();
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

    if(shellMgr.isLocal() && System.getProperty("os.name").toLowerCase().startsWith("linux")){
      try{
        File linuxFile = new File(runtimeDirectory, "Linux");
        if(!linuxFile.exists()){
          shellMgr.writeFile(linuxFile.getAbsolutePath(), "# This is a dummy runtime environment" +
                " description file. Its presence just means that we are running on Linux.", false);
        }
        linuxFile = new File("tmp", "data1.root");
        if(!linuxFile.exists()){
          shellMgr.writeFile(linuxFile.getAbsolutePath(), "", false);
        }
        linuxFile = new File("tmp", "data2.root");
        if(!linuxFile.exists()){
          shellMgr.writeFile(linuxFile.getAbsolutePath(), "", false);
        }
      }
      catch(Exception e){
        logFile.addMessage("WARNING: Could not create Linux runtime enviromnment file",
            e);
      }
    }
    
    finalRuntimesLocal = new HashSet();
    finalRuntimesRemote = new HashSet();
    HashSet runtimes = shellMgr.listFilesRecursively(runtimeDirectory);
    if(runtimes!=null && runtimes.size()>0){
      File fil = null;
      String hostName = null;
      
      String name = null;
      String cert = null;
      String url = null;
      
      DBPluginMgr localDBMgr = null;
      
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
          localRuntimeDB);
      }
      catch(Exception e){
        Debug.debug("WARNING: Could not load local runtime DB "+
            localRuntimeDB+". Runtime environments must be defined by hand. "+
            e.getMessage(), 1);
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

        fil = (File) it.next();
        
        // Get the name
        Debug.debug("File found: "+fil.getName()+":"+fil.getAbsolutePath(), 3);
        name = fil.getAbsolutePath().substring(
            (new File(runtimeDirectory)).getAbsolutePath().length()+1).replaceAll(
                "\\\\", "/");
        
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
          cert = shellMgr.readFile(publicCertificate);
          // TODO: check if certificate includes private key
          // and discard the key if so
        }
        catch(Exception e){
          //e.printStackTrace();
        }

        if(name!=null && name.length()>0 &&
            url!=null && url.length()>0 && runtimeEnvironmentFields!=null){
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
          if(localDBMgr!=null){
            // create if not there
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
              DBPluginMgr remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
                  remoteDB);
              boolean rteExistsRemotely = false;
              if(localDBMgr!=null){
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
    if(!shellMgr.existsFile(runDir(job))){
      shellMgr.mkdirs(runDir(job));
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
      String[] outputMapping = dbPluginMgr.getOutputFiles(jobDefID);
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
          job.setInternalStatus(ComputingSystem.STATUS_ERROR);
          error = "Exception during copying of output file(s) for job : " + job.getName() + "\n" +
          "\tCommand\t: " + localName + ": -> " + remoteName +"\n" +
          "\tException\t: " + e.getMessage();
          logFile.addMessage(error, e);
        }
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
    try{
      shellMgr.deleteFile(finalStdOut);
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    try{
      shellMgr.deleteFile(finalStdErr);
    }
    catch(Exception ioe){
      error = "Exception during clearOutputMapping of job " + job.getName()+ "\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
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
    try{
      localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
        localRuntimeDB);
    }
    catch(Exception e){
      Debug.debug("Could not load local runtime DB "+localRuntimeDB+"."+e.getMessage(), 1);
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
    if(remoteDB!=null){
      DBPluginMgr remoteDBMgr = null;
      try{
        remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
          remoteDB);
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
      // Delete the run directory
      try{
        shellMgr.deleteDir(runDir);
      }
      catch(Exception e){
        error = "Exception during postProcess of job " + job.getName()+ "\n" +
        "\tException\t: " + e.getMessage();
        logFile.addMessage(error, e);
        //return false;
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
    boolean ok = true;
    
    // TODO: support sources: gsiftp://, ftp://.
    // TODO: if source is /..., c:\... or file://...,
    // use shellMgr to scp the file from local disk.

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
    for(int i=0; i<inputFiles.length; ++i){
      if(inputFiles[i]!=null && inputFiles[i].trim().length()!=0){
        if(inputFiles[i].matches("^file:/*[^/]+.*")){
          inputFiles[i] = inputFiles[i].replaceFirst("^file:/*", "/");
        }
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
        Debug.debug("Pre-processing : Getting " + inputFiles[i], 2);
        String fileName = inputFiles[i];
        int lastSlash = fileName.lastIndexOf("/");
        if(lastSlash>-1){
          fileName = fileName.substring(lastSlash + 1);
        }
        try{
          if(!shellMgr.copyFile(inputFiles[i], runDir(job)+"/"+fileName)){
            logFile.addMessage("Pre-processing : Cannot get " +
                inputFiles[i]);
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
    }
    return ok;
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
    // TODO: support destinations: gsiftp://, ftp://.
    // TODO: if destination is /..., c:\... or file://...,
    // use shellMgr to scp the file to local disk.
    /**
     * move temp StdOut -> finalStdOut
     */
    if(finalStdOut!=null && finalStdOut.trim().length()!=0){
      try{
        if(!shellMgr.existsFile(job.getStdOut())){
          error = "Post processing : File " + job.getStdOut() + " doesn't exist";
          logFile.addMessage(error);
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for stdout: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
      }
      Debug.debug("Post processing : Renaming " + job.getStdOut() + " in " + finalStdOut, 2);
      try{
        if(!shellMgr.copyFile(job.getStdOut(), finalStdOut)){
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
      }
      job.setStdOut(finalStdOut);
    }

    /**
     * move temp StdErr -> finalStdErr
     */
    if(finalStdErr!=null && finalStdErr.trim().length()!=0){
      try{
        if(!shellMgr.existsFile(job.getStdErr())){
          logFile.addMessage("Post processing : File " + job.getStdErr() + " doesn't exist");
          return false;
        }
      }
      catch(Throwable e){
        error = "ERROR checking for stderr: "+e.getMessage();
        Debug.debug(error, 2);
        logFile.addMessage(error);
      }
      Debug.debug("Post processing : Renaming " + job.getStdErr() + " in " + finalStdErr,2);
      try{
        if(!shellMgr.copyFile(job.getStdErr(), finalStdErr)){
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
      }
      job.setStdErr(finalStdErr);
    }
    return true;
  }
  
  public String getError(String csName){
    return error;
  }

}