package gridpilot.csplugins.glite;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import gridpilot.ComputingSystem;
import gridpilot.ConfigFile;
import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.GridPilot;
import gridpilot.ShellMgr;
import gridpilot.Util;

import org.glite.wms.wmproxy.CredentialException;
import org.glite.wms.wmproxy.ServiceException;
import org.glite.wms.wmproxy.ServiceURLException;
import org.glite.wms.wmproxy.WMProxyAPI;

/**
 * Main class for the LSF plugin.
 */

public class GLiteComputingSystem implements ComputingSystem{

  public static final String LSF_STATUS_PEND = "PEND";
  public static final String LSF_STATUS_RUN = "RUN";
  public static final String LSF_STATUS_WAIT = "WAIT";
  public static final String LSF_STATUS_DONE = "DONE";
  public static final String LSF_STATUS_EXIT = "EXIT";
  public static final String LSF_STATUS_UNKWN = "UNKWN";

  public static final String LSF_STATUS_NOTFOUND = "NOT_FOUND";
  public static final String LSF_STATUS_ERROR = "ERROR";
  public static final String LSF_STATUS_UNAVAILABLE = "UNAVAILABLE";

  private String csName;
  private LogFile logFile;
  private ConfigFile configFile;
  private String error = "";
  private String runtimeDirectory = null;
  private String runtimeDB = null;
  private HashSet finalRuntimes = null;
  private String wmUrl = null;
  private WMProxyAPI vmProxyAPI = null;

  public GLiteComputingSystem(String _csName){
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    try{
      wmUrl = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "WMProxy URL");
    }
    catch(Exception e){
      Debug.debug("ERROR getting runtime directory: "+e.getMessage(), 1);
    }
    try{
      runtimeDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "runtime directory");
    }
    catch(Exception e){
      Debug.debug("ERROR getting runtime directory: "+e.getMessage(), 1);
    }
    try{
      runtimeDB = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "runtime database");
    }
    catch(Exception e){
      Debug.debug("ERROR getting runtime database: "+e.getMessage(), 1);
    }
    if(runtimeDB!=null && !runtimeDB.equals("")){
      setupRuntimeEnvironments(csName);
    }
    
    try{
      vmProxyAPI = new WMProxyAPI(wmUrl,
          Util.getProxyFile().getAbsolutePath(), GridPilot.caCerts);
    }
    catch(Exception e){
      Debug.debug("ERROR initializing VMProxy: "+e.getMessage(), 1);
      e.printStackTrace();
    }
        
  }

  /**
   * By convention the runtime environments are defined by the
   * scripts in the directory specified in the config file (runtime directory).
   */
  public void setupRuntimeEnvironments(String csName){
    finalRuntimes = new HashSet();
    HashSet runtimes = shell.listFilesRecursively(runtimeDirectory);
    if(runtimes!=null && runtimes.size()>0){
      File fil = null;
      
      String name = null;
      DBPluginMgr dbPluginMgr = null;      
      try{
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(
            runtimeDB);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not load runtime DB "+runtimeDB, 1);
        return;
      }
      
      String [] runtimeEnvironmentFields =
        dbPluginMgr.getFieldNames("runtimeEnvironment");
      String [] rtVals = new String [runtimeEnvironmentFields.length];

      for(Iterator it=runtimes.iterator(); it.hasNext();){
        
        name = null;

        fil = (File) it.next();
        
        // Get the name
        Debug.debug("File found: "+fil.getName()+":"+fil.getAbsolutePath()+
            ":"+fil.getAbsolutePath(), 3);
        name = fil.getName();
                
        if(name!=null && name.length()>0){
          // Write the entry in the local DB
          for(int i=0; i<runtimeEnvironmentFields.length; ++i){
            if(runtimeEnvironmentFields[i].equalsIgnoreCase("name")){
              rtVals[i] = name;
            }
            else if(runtimeEnvironmentFields[i].equalsIgnoreCase("computingSystem")){
              rtVals[i] = csName;
            }
            else{
              rtVals[i] = "";
            }
          }
          try{
            if(dbPluginMgr.createRuntimeEnvironment(rtVals)){
              finalRuntimes.add(name);
            }
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    else{
      Debug.debug("WARNING: no runtime environment scripts found", 1);
    }
  }

  public boolean submit(JobInfo job){
    Debug.debug("Job definition ID : " + job.getJobDefId(), 3);
    Debug.debug("Job name : " + job.getName(), 3);
    return false;
  }

  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i){
      ((JobInfo) jobs.get(i)).setInternalStatus(getLocalStatus(((JobInfo) jobs.get(i))));
    }
  }

  /**
   * Matches LSF status in AtCom Status
   */
  private int getLocalStatus(JobInfo job){
    if (job.getJobStatus()==null)
      return ComputingSystem.STATUS_WAIT;

    if (job.getJobStatus().equals(LSF_STATUS_DONE) ||
        job.getJobStatus().equals(LSF_STATUS_EXIT) ||
        job.getJobStatus().equals(LSF_STATUS_NOTFOUND))
      return ComputingSystem.STATUS_DONE;

    if (job.getJobStatus().equals(LSF_STATUS_PEND))
      return ComputingSystem.STATUS_WAIT;

    if (job.getJobStatus().equals(LSF_STATUS_UNKWN))
      return ComputingSystem.STATUS_FAILED;

    if (job.getJobStatus().equals(LSF_STATUS_ERROR))
      return ComputingSystem.STATUS_ERROR;

    return ComputingSystem.STATUS_RUNNING;
  }

  public boolean killJobs(Vector jobsToKill){

    String killCommand = configFile.getValue(csName, "Kill command");
    if(killCommand==null){
      logFile.addMessage("Could not kill job : " +
                         configFile.getMissingMessage(csName,
                         "kill command"));
      return false;
    }

    String cmd = "";
    cmd += killCommand;

    for(Enumeration en=jobsToKill.elements(); en.hasMoreElements();){
      cmd += " " + ((JobInfo) en.nextElement()).getJobId();
    }

    /**
     * Command execution
     */
    try{
      StringBuffer stdErr = new StringBuffer();

      shell.exec(cmd, null, stdErr);

      /**
       * Error processing
       */
      if(stdErr.length()!=0){
        logFile.addMessage("Error during killing of job " +
                           " on " + csName + " : \n" +
                           "\tCommand\t: " + cmd + "\n" +
                           "\tMessage\t: " + stdErr);
      }

    }
    catch(IOException ioe){
      logFile.addMessage("IOException during job " +
                         " killing :\n" +
                         "\tCommand\t: " + cmd + "\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      return false;
    }
    return true;
  }

  public void clearOutputMapping(JobInfo job){

    /** Gets the name of the "rfrm" command */
    String rmCmd = configFile.getValue(csName, "removeCommand");
    if(rmCmd==null){
      logFile.addMessage("Cannot clear output mapping : \n" +
                         configFile.getMissingMessage(csName,
          "remove command"));
      return;
    }

    /**
     * Gets the names of all output mapping for this job transformation.
     */
    String[] outputMapping = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()
        ).getOutputFiles(job.getJobDefId());

    /**
     * For each job transformation output, gets the file to be removed
     */
    for(int i=0; i<outputMapping.length; ++i){
      /**
       * Command creation
       */
      String remoteName = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName()
          ).getJobDefOutLocalName(job.getJobDefId(), outputMapping[i]);
      String cmd = rmCmd+" "+remoteName;

      Debug.debug("clearOutputMapping : " + cmd, 3);

      try{
        /**
         * Command execution
         */
        StringBuffer stdOut = new StringBuffer();
        StringBuffer stdErr = new StringBuffer();
        shell.exec(cmd, stdOut, stdErr);
        if (stdOut.length() != 0)
          Debug.debug("clearOutputMapping : " + stdOut, 3);

          /**
           * Errors processing
           */
        if (stdErr.length() != 0)
          logFile.addMessage("Error during clearOutputMapping command : \n" +
                             "\tCommand : " + cmd + "\n" +
                             "\tError : " + stdErr);

      }
      catch(IOException ioe){
        logFile.addMessage("IOException during outputMappingClearing of job " +
                           job.getName() +
                           " on " + csName + " : \n" +
                           "\tCommand\t: " + cmd + "\n" +
                           "\tException\t: " + ioe.getMessage(), ioe);
      }
    }
  }

  public void exit(){
    String runtimeName = null;
    String initText = null;
    String id = "-1";
    boolean ok = true;
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(
        runtimeDB);
    for(Iterator it=finalRuntimes.iterator(); it.hasNext();){
      ok = true;
      runtimeName = (String )it.next();
      // Don't delete records with a non-empty initText.
      // These can only have been created by hand.
      initText = dbPluginMgr.getRuntimeInitText(runtimeName, csName);
      if(initText!=null && !initText.equals("")){
        continue;
      }
      id = dbPluginMgr.getRuntimeEnvironmentID(runtimeName, csName);
      if(!id.equals("-1")){
        ok = dbPluginMgr.deleteRuntimeEnvironment(id);
      }
      else{
        ok = false;
      }
      if(!ok){
        Debug.debug("WARNING: could not delete runtime environment " +
            runtimeName+
            " from database "+
            dbPluginMgr.getDBName(), 1);
      }
    }
  }

  public String getFullStatus(JobInfo job){
    /**
     * Command creation
     */
    String bjobs = configFile.getValue(csName, "Checking command");
    if (bjobs == null || job.getJobId() == null || job.getJobId().equals(""))
      return "";

    String cmd = bjobs+" "+"-w"+" "+job.getJobId();
    try{
      /**
       * Command execution
       */
      StringBuffer stdOut = new StringBuffer();
      StringBuffer stdErr = new StringBuffer();

      shell.exec(cmd, stdOut, stdErr);
      return stdOut + (stdErr.length() == 0 ? "" : "\n(" + stdErr + ")");

    }
    catch(IOException ioe){
      logFile.addMessage("IOException during getFullStatus of job " +
                         job.getName() +
                         "on " + csName + " : \n" +
                         "\tCommand\t: " + cmd + "\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
      return ioe.getMessage();
    }
  }

  public String[] getCurrentOutputs(JobInfo job){
    Debug.debug("getCurrentOutputs", 3);

    String[] res = new String[2];
    
    String cmd = "bpeek "+job.getJobId();

    StringBuffer stdOut = new StringBuffer();
    StringBuffer stdErr = new StringBuffer();

    try{
      shell.exec(cmd, stdOut, stdErr);
      res[0] = stdOut.toString();
      if (stdErr.length() != 0){
        logFile.addMessage("Error during bpeek command : \n" +
                           "\tCommand : " + cmd + "\n" +
                           "\tError : " + stdErr);
        res[0] = (res[0] == null ? "" : res[0]) + "Error : " + stdErr;
      }

    }
    catch(IOException ioe){
      logFile.addMessage("IOException during bpeek command of job " +
                         job.getName() + " : \n" +
                         "\tCommand\t: " + cmd + "\n" +
                         "\tException\t: " + ioe.getMessage(), ioe);
    }
    return res;
  }

  public String[] getScripts(JobInfo job){
    String wrapperPath;
    if(job.getStdOut()!= null){
      wrapperPath = job.getStdOut().substring(0, job.getStdOut().lastIndexOf("/")+1);
    }
    else{
      wrapperPath = "./";
    }
    String jobScriptFile = wrapperPath + job.getName() + ".job";
    try{
      ShellMgr shellMgr = GridPilot.getClassMgr().getShellMgr(
          csName);
      String jobScriptText = "No file "+jobScriptFile;
      if(shellMgr.existsFile(jobScriptFile)){
        jobScriptText = shellMgr.readFile(jobScriptFile);
      }
      return new String [] {jobScriptText};
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during getScripts of job " + job.getName()+ "\n" +
                                  "\tException\t: " + ioe.getMessage(), ioe);
      return null;
    }
  }
  public boolean copyFile(String csName, String src, String dest){
    try{
      return shell.copyFile(src, dest);
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during copying of file " +
          csName + " : \n" +
          "\tSource\t: " + src + "\n" +
          "\tDestination\t: " + dest + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("Exception during copying of file " +ioe.getMessage(), 3);
      return false;
    }
  }
  
  public boolean deleteFile(String csName, String src){
    try{
      return shell.deleteFile(src);
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during deleting of file " +
          csName + " : \n" +
          "\tSource\t: " + src + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("Exception during deleting of file " +ioe.getMessage(), 3);
      return false;
    }
  }
  
  public boolean existsFile(String csName, String src){
    try{
      return shell.existsFile(src);
    }
    catch(Exception ioe){
      logFile.addMessage("Exception during checking of file " +
          csName + " : \n" +
          "\tSource\t: " + src + "\n" +
          "\tException\t: " + ioe.getMessage(), ioe);
      Debug.debug("Exception during checking of file " +ioe.getMessage(), 3);
      return false;
    }
  }
  
  public String getUserInfo(String csName){
    String user = null;
    String cmd ="whoami";
    try{
      StringBuffer stdErr = new StringBuffer();
      StringBuffer stdOut = new StringBuffer();
      shell.exec(cmd, stdOut, stdErr);
      user = stdOut.toString();
    }
    catch(IOException ioe){
      logFile.addMessage("IOException during getUserInfo\n" +
                                  "\tCommands\t: " + cmd +"\n" +
                                 "\tException\t: " + ioe.getMessage(), ioe);
    }
    if(user==null){
      Debug.debug("Job user null, getting from config file", 3);
       user = GridPilot.getClassMgr().getConfigFile().getValue("GridPilot", "user");
    }
    return user;
  }
  
  /**
   * Operations done after a job is Validated. <br>
   * Theses operations contain emcompasses two stages :
   * <ul>
   * <li>Moving of outputs in their final destination
   * <li>Extraction of some informations from outputs
   * </ul> <p>
   *
   * @return <code>true</code> if postprocessing went ok, <code>false</code> otherwise
   * 
   * (from AtCom1)
   */
  public boolean postProcess(JobInfo job){
    return copyToFinalDest(job);
  }

  /**
   * Operations done (by GridPilot) before a job is run. <br>
   *
   * @return <code>true</code> if postprocessing went ok, <code>false</code> otherwise
   * 
   * (from AtCom1)
   */
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
      shell = GridPilot.getClassMgr().getShellMgr(job);
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
          String wrapperPath;
          if(job.getStdOut()!=null){
            wrapperPath = job.getStdOut().substring(0, job.getStdOut().lastIndexOf("/")+1);
          }
          else{
            wrapperPath = "./";
          }
          if(!shell.copyFile(inputFiles[i], wrapperPath+"/"+fileName)){
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
    Debug.debug("PostProcessing for job " + job.getName(), 2);
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getJobDefId());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getJobDefId());
    // TODO: should we support destinations like gsiftp:// and http://?
    // For grid systems they should already have been taken care of by
    // the job description.
    ShellMgr shell = null;
    try{
      shell = GridPilot.getClassMgr().getShellMgr(job);
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

  public String getError(String csName){
    return error;
  }

}