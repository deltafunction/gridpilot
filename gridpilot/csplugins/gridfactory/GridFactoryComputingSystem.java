package gridpilot.csplugins.gridfactory;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Vector;

import javax.swing.Timer;


import org.globus.gsi.GlobusCredentialException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSException;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.lrms.LRMS;

import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyJobInfo;
import gridpilot.MySSL;
import gridpilot.MyUtil;
import gridpilot.RteRdfParser;
import gridpilot.csplugins.fork.ForkComputingSystem;
import gridpilot.csplugins.fork.ForkScriptGenerator;

public class GridFactoryComputingSystem extends ForkComputingSystem implements MyComputingSystem{

  private String submitURL = null;
  private String submitHost = null;
  private String user = null;
  private FileTransfer fileTransfer = null;
  private LRMS myLrms = null;
  // Map of id -> DBPluginMgr.
  // This is to be able to clean up RTEs from catalogs on exit.
  private HashMap<String, String> toDeleteRtes = new HashMap<String, String>();
  // Whether or not to request virtualization of jobs.
  private boolean virtualize = false;
  // VOs allowed to run my jobs.
  private String [] allowedVOs = null;
  // RTEs are refreshed from entries written by pull nodes in remote database
  // every RTE_SYNC_DELAY milliseconds.
  private static int RTE_SYNC_DELAY = 60000;
  
  private Timer timerSyncRTEs = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      Debug.debug("Syncing RTEs", 2);
      cleanupRuntimeEnvironments(csName);
      MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, runtimeDBs, toDeleteRtes,
          false, true,  new String [] {"Linux", "Windows"}, true);
    }
  });
  private boolean onWindows;

  public GridFactoryComputingSystem(String _csName) throws Exception{
    super(_csName);
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    submitURL = configFile.getValue(csName, "submission url");
    submitHost = (new GlobusURL(submitURL)).getHost();
    rteCatalogUrls = configFile.getValues(GridPilot.TOP_CONFIG_SECTION, "runtime catalog URLs");
    timerSyncRTEs.setDelay(RTE_SYNC_DELAY);
    String virtualizeStr = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "virtualize");
    virtualize = virtualizeStr!=null && (virtualizeStr.equalsIgnoreCase("yes") || virtualizeStr.equalsIgnoreCase("true"));
    allowedVOs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "allowed subjects");
    String onWindowsStr = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "On windows");
    onWindows = onWindowsStr!=null && (onWindowsStr.equalsIgnoreCase("yes") ||
        onWindowsStr.equalsIgnoreCase("true"));
    try{
      // Set user
      user = GridPilot.getClassMgr().getSSL().getGridSubject();            
    }
    catch(Exception ioe){
      error = "ERROR during initialization of GridFactory plugin\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    fileTransfer = GridPilot.getClassMgr().getFTPlugin("https");
  }
  
  /**
   * Add the executable and the input files of the executable to job.getInputFiles().
   * @param job the job in question
   */
  private void setInputFiles(JobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] jobInputFiles = dbPluginMgr.getJobDefInputFiles(job.getIdentifier());
    job.setInputFileUrls(jobInputFiles);
    String executableID = dbPluginMgr.getJobDefExecutableID(job.getIdentifier());
    String [] executableInputs = dbPluginMgr.getExecutableInputs(executableID);
    // Input files -
    // Skip local files that are not present
    // - they are expected to be present on the worker node
    int initialLen = job.getInputFileUrls()!=null&&job.getInputFileUrls().length>0?job.getInputFileUrls().length:0;
    Vector<String> newInputs = new Vector<String>();
    for(int i=0; i<initialLen; ++i){
      if(fileIsRemoteOrPresent(job.getInputFileUrls()[i])){
        newInputs.add(job.getInputFileUrls()[i]);
        Debug.debug("Setting input file "+job.getInputFileUrls()[i], 3);
      }
      else{
        Debug.debug("Input file "+job.getInputFileUrls()[i]+" not found - continuing anyway...", 3);
      }
    }
    for(int i=initialLen; i<initialLen+executableInputs.length; ++i){
      if(fileIsRemoteOrPresent(executableInputs[i])){
        newInputs.add(executableInputs[i]);
      }
    }
    String exeScript = dbPluginMgr.getExecutableFile(job.getIdentifier());
    if(fileIsRemoteOrPresent(exeScript)){
      newInputs.add(exeScript);
    }
    Debug.debug("Job has input files: "+newInputs, 3);
    job.setInputFileUrls(newInputs.toArray(new String[newInputs.size()]));
    // Executable
    String exeScriptName = (new File(exeScript)).getName();
    job.setExecutables(new String [] {exeScriptName});
  }
  
  private void setOutputFiles(JobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String [] outputDestinations = new String [outputFileNames.length];
    for(int i=0; i<outputDestinations.length; ++i){
      outputDestinations[i] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
    }
    job.setOutputFileDestinations(outputDestinations);
    job.setOutputFileNames(outputFileNames);
    Debug.debug("Output files: "+MyUtil.arrayToString(job.getOutputFileNames())+"-->"+
        MyUtil.arrayToString(job.getOutputFileDestinations()), 2);
  }
  
  /**
   * Add the requested RTEs to job.getRTEs() or job.getOpsys().
   * @param job the job in question
   */
  private void setRTEs(JobInfo job) {
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] rtes0 = dbPluginMgr.getRuntimeEnvironments(job.getIdentifier());
    Debug.debug("The job "+job.getIdentifier()+" requires RTEs: "+MyUtil.arrayToString(rtes0), 2);
    if(rtes0==null || rtes0.length==0){
      return;
    }
    Vector<String> rtesVec = new Vector<String>();
    for(int i=0; i<rtes0.length; ++i){
      // Check if any of the RTEs is a BaseSystem - if so, set job.getOpsys() instead of job.getRTEs().
      // TODO: consider using RTEMgr.isVM() instead of relying on people starting their
      //       VM RTE names with VM/
      if(MyUtil.checkOS(rtes0[i])){
        if(job.getOpSys()==null || job.getOpSys().equals("")){
          job.setOpSys(rtes0[i]);
        }
      }
      else if(MyUtil.checkOS(rtes0[i]) || rtes0[i].startsWith(RteRdfParser.VM_PREFIX)){
        if(job.getOpSys()==null || job.getOpSys().equals("")){
          job.setOpSys(rtes0[i]);
        }
      }
      else{
        rtesVec.add(rtes0[i]);
      }
    }
    if(rtesVec.size()>0){
      job.setRTEs(rtesVec.toArray(new String[rtesVec.size()]));
    }
  }

  private boolean fileIsRemoteOrPresent(String file) {
    if(!MyUtil.urlIsRemote(file)){
      String localFile = MyUtil.clearTildeLocally(MyUtil.clearFile(file));
      return LocalStaticShell.existsFile(localFile);
    }
    return true;
  }
  
  protected String getCommandSuffix(MyJobInfo job){
    String commandSuffix = ".sh";
    if(onWindows){
      commandSuffix = ".bat";
    }
    return commandSuffix;
  }

  /**
   * Copies jobDefinition plus the associated dataset to the (remote) database,
   * where it will be picked up by pull clients. Sets csState to 'ready'.
   * Sets finalStdOut and finalStdErr and any local output files 
   * to files in the remote gridftp directory.
   */
  public boolean submit(JobInfo job){
    /*
     * Flags that can be set in the job script.
     *  -n [job name]<br>
     *  -i [input file 1] [input file 2] ...<br>
     *  -e [executable 1] [executable 2] ...<br>
     *  -t [running time on a 2.8 GHz Intel Pentium 4 processor]<br>
     *  -m [required memory in megabytes]<br>
     *  -z whether or not to require that the job should run in its own virtual
           machine<br>
     *  -o [output files]<br>
     *  -r [runtime environment 1] [runtime environment 2] ...<br>
     *  -y [operating system]<br>
     *  -s [distinguished name] : DN identifying the submitter<br><br>
     *  -v [allowed virtual organization 1] [allowed virtual organization 2] ...<br>
     *  -u [id] : unique identifier of the form https://server/job/dir/hash
     */
    try{
      Debug.debug("Submitting", 3);
      job.setAllowedVOs(allowedVOs);
      String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
      String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
      ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
      // Create a standard shell script.
      ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job),
          ignoreBaseSystemAndVMRTEs, onWindows);
      if(!scriptGenerator.createWrapper(shell, (MyJobInfo) job, job.getName()+getCommandSuffix((MyJobInfo) job))){
        throw new IOException("Could not create wrapper script.");
      }
      String [] values = new String []{
          job.getName(),
          MyUtil.arrayToString(job.getInputFileUrls()),
          MyUtil.arrayToString(job.getExecutables()),
          Integer.toString(job.getGridTime()),
          Integer.toString(job.getMemory()),
          virtualize?"1":"0",
          constructOutputFilesString(job),
          MyUtil.arrayToString(job.getRTEs()),
          job.getOpSys(),
          job.getUserInfo(),
          MyUtil.arrayToString(job.getAllowedVOs(), job.getJobId()),
          null
      };

      // If this is a resubmit, first delete the old remote jobDefinition
      try{
        if(job.getJobId()!=null && job.getJobId().length()>0){
          getLRMS().clean(new String [] {job.getJobId()}, null);
        }
      }
      catch(Exception e){
      }
      
      String cmd = runDir(job)+"/"+job.getName()+getCommandSuffix((MyJobInfo) job);
      String id = getLRMS().submit(cmd, values, submitURL, true, GridPilot.getClassMgr().getSSL().getCertFile());
      job.setJobId(id);
    }
    catch(Exception e){
      error = "ERROR: could not submit job. "+e.getMessage();
      logFile.addMessage(error, e);
      e.printStackTrace();
      return false;
    }
    return true;
  }
  
  // We're using an undocumented feature of GridFactory (Util.parseJobScript, QueueMgr):
  // output files specified as
  // file1 https://some.server/some/dir/file1 file2 https://some.server/some/dir/file2 ...
  // are delivered.
  private String constructOutputFilesString(JobInfo job) {
    StringBuffer ret = new StringBuffer();
    for(int i=0; i<job.getOutputFileNames().length; ++i){
      ret.append(" " + job.getOutputFileNames()[i]);
      if(MyUtil.urlIsRemote(job.getOutputFileDestinations()[i])){
        ret.append(" " + job.getOutputFileDestinations()[i]);
      }
      else{
        ret.append(" " + job.getOutputFileNames()[i]);
      }
    }
    return ret.toString().trim();
  }

  private void mkRemoteDir(String url) throws Exception{
    if(!url.endsWith("/")){
      throw new IOException("Directory URL: "+url+" does not end with a slash.");
    }
    GlobusURL globusUrl= new GlobusURL(url);
    // First, check if directory already exists.
    try{
      fileTransfer.list(globusUrl, null);
      return;
    }
    catch(Exception e){
    }
    // If not, create it.
    Debug.debug("Creating directory "+globusUrl.getURL(), 2);
    fileTransfer.write(globusUrl, "");
  }
  
  /**
   * Sets csState to 'requestKill' in remote jobDefinition record.
   * @throws GeneralSecurityException 
   * @throws IOException 
   */
  public boolean killJobs(Vector<JobInfo> jobs) {
    boolean ok = true;
    Enumeration<JobInfo> en = jobs.elements();
    MyJobInfo job = null;
    while(en.hasMoreElements()){
      job = (MyJobInfo) en.nextElement();
      try{
        getLRMS().kill(submitHost, new String [] {job.getJobId()});
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("WARNING: could not kill job "+job.getIdentifier(), e);
        ok = false;
      }
    }
    return ok;
  }

  /**
   * If any input or output files or finalStdout, finalStderr are local:
   * - Creates temporary directory on the remote server.
   */
  public boolean preProcess(JobInfo job){
    // Iff the job has remote output files, check if the remote directory exists,
    // otherwise see if we can create it.
    Debug.debug("Preprocessing", 3);
    try{
      if(job.getOutputFileDestinations()!=null){
        String rDir = null;
        for(int i=0; i<job.getOutputFileDestinations().length; ++i){
          if(MyUtil.urlIsRemote(job.getOutputFileDestinations()[i])){
            rDir = job.getOutputFileDestinations()[i].replaceFirst("(^(.*/)[^/]+$", "$1");
            mkRemoteDir(rDir);
          }
        }
      }
    }
    catch(Exception e){
      error = "ERROR: could not upload requested files. "+e.getMessage();
      logFile.addMessage(error, e);
      return false;
    }
    try{
      // Do this only if not a resubmit
      setInputFiles(job);
      setOutputFiles(job);
      setRTEs(job);
    }
    catch(Exception e){
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean postProcess(JobInfo job){
    boolean ok = true;
    // super.postProcess assumes the output files are available to the shell, so
    // we copy them over locally to make this true. Stdout/err were taken care of
    // by getCurrentOutput called from updateStatus before validation.
    // TODO: Once GridFactory supports upload of output files for https, this should
    // be done only for output destinations with other protocols than https.
    try{
      getOutputs((MyJobInfo) job);
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("ERROR: could not download output files from job "+job.getIdentifier()+" : "+job.getJobId(), e);
      ok = false;
    }
    if(!super.postProcess(job)){
      ok = false;
    }
    return ok;
  }
  
  private void getOutputs(MyJobInfo job) throws MalformedURLException, Exception{
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] outputFiles = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String [] outputDestinations = dbPluginMgr.getOutputFiles(job.getIdentifier());
    Vector<String> outNamesVec = new Vector();
    Vector<String> outDestsVec = new Vector();
    if(outputFiles!=null && outputFiles.length>0){
      for(int i=0; i<outputFiles.length; ++i){
        // Files that have remote destinations will have been uploaded by GridFactory.
        if(!MyUtil.urlIsRemote(outputDestinations[i])){
          fileTransfer.getFile(
              new GlobusURL(job.getJobId()+"/"+outputFiles[i]),
              new File(MyUtil.clearTildeLocally(MyUtil.clearFile(runDir(job)))));
        }
        else{
          outNamesVec.add(outputFiles[i]);
          outDestsVec.add(outputDestinations[i]);
        }
      }
      if(!outDestsVec.isEmpty()){
        String [][] uploadFiles = new String [2][outDestsVec.size()];
        for(int i=0; i<outDestsVec.size(); ++i){
          uploadFiles[0][i] = outNamesVec.get(i);
          uploadFiles[1][i] = outDestsVec.get(i);
        }
        // This will prevent super.postProcess to look for these files.
        job.setUploadFiles(uploadFiles);
      }
    }
  }

  /**
   * Returns csStatus of remote jobDefinition record.
   */
  public String getFullStatus(JobInfo job){
    String ret = "";
    ret += "Submit host: "+submitHost+"\n";
    //ret += "DB location: "+job.getDBLocation()+"\n";
    //ret += "Job DB URL: "+job.getDBURL()+"\n";
    try{
      ret += "Job status: "+JobInfo.getStatusName(getLRMS().getJobStatus(submitHost, job.getJobId()));
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("WARNING: could not get status of job "+job.getIdentifier(), e);
      return null;
    }
    return ret;
  }

  /**
   * Downloads stdout/stderr to local run dir and reads them.
   */
  public String[] getCurrentOutput(JobInfo job) {
    File tmpStdout = new File(job.getOutTmp());
    File tmpStderr = new File(job.getErrTmp());   
    String [] res = new String[2];
    try{
      int st = getLRMS().getJobStatus(submitHost, job.getJobId());
      if(st==JobInfo.STATUS_DONE || st==JobInfo.STATUS_FAILED || st==JobInfo.STATUS_UPLOADED){
        // stdout
        fileTransfer.getFile(new GlobusURL(job.getJobId()+"/stdout"), tmpStdout);
        res[0] = LocalStaticShell.readFile(tmpStdout.getAbsolutePath());
        // stderr
        boolean ok = true;
        try{
          fileTransfer.getFile(new GlobusURL(job.getJobId()+"/stderr"), tmpStderr);
        }
        catch(Exception e){
          ok = false;
          e.printStackTrace();
        }
        if(ok){
          res[1] = LocalStaticShell.readFile(tmpStderr.getAbsolutePath());
        }
      }
      else if(st==JobInfo.STATUS_RUNNING){
        res = getLRMS().getOutput(submitHost, job.getJobId(), GridPilot.getClassMgr().getCSPluginMgr().currentOutputTimeOut);
      }
      else{
        throw new IOException("Job is not in a state that allows getting output - "+JobInfo.getStatusName(st));
      }
    }
    catch(Exception ee){
      ee.printStackTrace();
      logFile.addMessage("WARNING: could not get current output of job "+job.getIdentifier(), ee);
    }
    return res;
  }

  /**
   * Finds requested jobs, checks if provider is allowed to run them;
   * if so, sets permissions on input files accordingly.
   */
  public void updateStatus(Vector<JobInfo> jobs){
    for(int i=0; i<jobs.size(); ++i)
      try{
        updateStatus((MyJobInfo) jobs.get(i));
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("WARNING: could not update status of job "+((MyJobInfo) jobs.get(i)).getIdentifier()+
            " : "+((MyJobInfo) jobs.get(i)).getJobId(), e);
      }
  }

  private void updateStatus(MyJobInfo job) throws IOException, GeneralSecurityException,
     GlobusCredentialException, GSSException{
    int st = getLRMS().getJobStatus(submitHost, job.getJobId());
    String csStatus = JobInfo.getStatusName(st);
    job.setCSStatus(csStatus);
    job.setStatus(st);
    if(st==JobInfo.STATUS_DONE){
      // This is to make sure we have something to validate
      getCurrentOutput(job);
      DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
      String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
      String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
      // TODO: Now upload to final destination. - first check if this is not done somewhere else
    }
    Debug.debug("Updated status of job "+job.getName()+" : "+job.getCSStatus()+" : "+csStatus, 2);
    if(csStatus==null || csStatus.equals("")){
      logFile.addMessage("ERROR: no csStatus for job "+job.getIdentifier()+" : "+job.getJobId());
      return;
    }
  }
  
  private LRMS getLRMS() throws IOException, GeneralSecurityException, GlobusCredentialException,
     GSSException{
    if(myLrms==null){
      MySSL ssl = GridPilot.getClassMgr().getSSL();
      if(ssl.getKeyPassword()==null){
        ssl.activateSSL();
      }
      myLrms = LRMS.constructLrms(true, GridPilot.getClassMgr().getSSL(), null, null);
    }
    return myLrms;
  }
  
  public String getUserInfo(String csName){
    return user;
  }

  public void setupRuntimeEnvironments(String csName){
    MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, runtimeDBs, toDeleteRtes,
        false, true, new String [] {"Linux", "Windows"}, true);
  }

  public void exit(){
  }
  
  /**
   * Clean up runtime environment records copied from runtime catalog URLs.
   */
  public void cleanupRuntimeEnvironments(String csName){
    MyUtil.cleanupRuntimeEnvironments(csName, runtimeDBs, toDeleteRtes);
  }
  
  public boolean cleanup(JobInfo job){
    boolean ret = super.cleanup(job);
    try{
      getLRMS().clean(new String [] {job.getJobId()}, null);
    }
    catch(Exception e){
      ret = false;
      e.printStackTrace();
    }    
    return ret;
  }


}
