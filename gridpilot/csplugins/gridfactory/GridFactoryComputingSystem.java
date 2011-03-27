package gridpilot.csplugins.gridfactory;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
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
import gridfactory.common.MyLinkedHashSet;
import gridfactory.common.jobrun.RTEMgr;
import gridfactory.lrms.LRMS;

import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyJobInfo;
import gridpilot.MySSL;
import gridpilot.MyUtil;
import gridpilot.csplugins.fork.ForkComputingSystem;
import gridpilot.csplugins.fork.ForkScriptGenerator;

public class GridFactoryComputingSystem extends ForkComputingSystem implements MyComputingSystem{

  private String [] submitURLs = null;
  private String [] submitHosts = null;
  private String user = null;
  private FileTransfer fileTransfer = null;
  private LRMS myLrms = null;
  // Map of id -> DBPluginMgr.
  // This is to be able to clean up RTEs from catalogs on exit.
  private HashMap<String, String> toDeleteRtes = new HashMap<String, String>();
  // Whether or not to request virtualization of jobs.
  private int virtualize = -1;
  private int runningSeconds = -1;
  private int ramMB = -1;
  // VOs allowed to run my jobs.
  private String [] allowedVOs = null;
  private String [] requiredRuntimeEnvs = null;
  // RTEs are refreshed from entries written by pull nodes in remote database
  // every RTE_SYNC_DELAY milliseconds.
  private static int RTE_SYNC_DELAY = 60000;

  private String [] basicOSRTES = {"Linux", "Windows", "Mac OS X"};
  
  private static HashMap<String, String> remoteCopyCommands = null;
  
  private Timer timerSyncRTEs = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      Debug.debug("Syncing RTEs", 2);
      cleanupRuntimeEnvironments(csName);
      MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, runtimeDBs, toDeleteRtes,
          false, true,  basicOSRTES, false);
    }
  });
  private boolean onWindows;

  public GridFactoryComputingSystem(String _csName) throws Exception{
    super(_csName);
    String [] rtCpCmds = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "Remote copy commands");
    if(rtCpCmds!=null && rtCpCmds.length>1){
      remoteCopyCommands = new HashMap<String, String>();
      for(int i=0; i<rtCpCmds.length/2; ++i){
        remoteCopyCommands.put(rtCpCmds[2*i], rtCpCmds[2*i+1]);
      }
    }
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    submitURLs = configFile.getValues(csName, "Submission urls");
    submitHosts = new String [submitURLs.length];
    for(int i=0; i<submitURLs.length; ++i){
      submitHosts[i] = (new GlobusURL(submitURLs[i])).getHost();
    }
    rteCatalogUrls = configFile.getValues(csName, "Runtime catalog URLs");
    requiredRuntimeEnvs = configFile.getValues(csName, "Required runtime environments");
    String cpuTime = configFile.getValue(csName, "CPU time");
    if(cpuTime!=null && !cpuTime.trim().equals("")){
      runningSeconds = 60*Integer.parseInt(cpuTime);
    }
    String memory = configFile.getValue(csName, "Memory");
    if(memory!=null && !memory.trim().equals("")){
      ramMB = Integer.parseInt(memory);
    }
    timerSyncRTEs.setDelay(RTE_SYNC_DELAY);
    String virtualizeStr = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "virtualize");
    if(virtualizeStr!=null && !virtualizeStr.trim().equals("")){
      virtualize = virtualizeStr.equalsIgnoreCase("yes") || virtualizeStr.equalsIgnoreCase("true")?1:0;
    }
    allowedVOs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "Allowed subjects");
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
    MyLinkedHashSet<String> newInputs = new MyLinkedHashSet<String>();
    for(int i=0; i<initialLen; ++i){
      if(fileIsRemoteOrPresent(job.getInputFileUrls()[i])){
        newInputs.add(job.getInputFileUrls()[i]);
        Debug.debug("Setting input file "+job.getInputFileUrls()[i], 3);
      }
      else{
        logFile.addMessage("Input file "+job.getInputFileUrls()[i]+" not found - continuing anyway...");
      }
    }
    for(int i=0; i<executableInputs.length; ++i){
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
    MyLinkedHashSet<String> allRtes = new MyLinkedHashSet<String>();
    if(rtes0!=null){
      Collections.addAll(allRtes, rtes0);
    }
    if(requiredRuntimeEnvs!=null){
      Collections.addAll(allRtes, requiredRuntimeEnvs);
    }
    MyLinkedHashSet<String> rtes = new MyLinkedHashSet<String>();
    String rte;
    for(Iterator<String> it=allRtes.iterator(); it.hasNext();){
      rte = it.next();
      // Check if any of the RTEs is a BaseSystem - if so, set job.getOpsys() instead of job.getRTEs().
      // TODO: consider using RTEMgr.isVM() instead of relying on people starting their
      //       VM RTE names with VM/
      if(MyUtil.checkOS(rte)){
        if(job.getOpSys()==null || job.getOpSys().equals("")){
          job.setOpSys(rte);
        }
      }
      else if(MyUtil.checkOS(rte) || rte.startsWith(RTEMgr.VM_PREFIX)){
        if(job.getOpSys()==null || job.getOpSys().equals("")){
          job.setOpSys(rte);
        }
      }
      else{
        rtes.add(rte);
      }
    }
    if(rtes.size()>0){
      job.setRTEs(rtes.toArray(new String[rtes.size()]));
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
      Debug.debug("Submitting job with output files "+MyUtil.arrayToString(job.getOutputFileNames()), 3);
      job.setAllowedVOs(allowedVOs);
      String stdoutFile = runDir(job) +"/"+job.getName()+ ".stdout";
      String stderrFile = runDir(job) +"/"+job.getName()+ ".stderr";
      ((MyJobInfo) job).setOutputs(stdoutFile, stderrFile);
      // Create a standard shell script.
      ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job),
          ignoreBaseSystemAndVMRTEs, onWindows, false);
      if(!scriptGenerator.createWrapper(shell, (MyJobInfo) job, job.getName()+getCommandSuffix((MyJobInfo) job))){
        throw new IOException("Could not create wrapper script.");
      }
      String[] nonDownloadInputFileUrls = removeDownloadFiles((MyJobInfo) job);
      String [] values = new String []{
          job.getName(),
          MyUtil.arrayToString(nonDownloadInputFileUrls),
          MyUtil.arrayToString(job.getExecutables()),
          Integer.toString(job.getRunningSeconds()),
          Integer.toString(job.getRamMB()),
          Integer.toString(virtualize),
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
          getLRMS().clean(new String [] {job.getJobId()}, null, null);
        }
      }
      catch(Exception e){
      }
      
      String cmd = runDir(job)+"/"+job.getName()+getCommandSuffix((MyJobInfo) job);
      String id = getLRMS().submit(cmd, values, submitURLs, true, GridPilot.getClassMgr().getSSL().getCertFile());
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
  
  private String[] removeDownloadFiles(MyJobInfo job) {
    if(job.getDownloadFiles()==null){
      return job.getInputFileUrls();
    }
    Vector<String> nonDlUrls = new Vector<String>();
    for(int i=0; i<job.getInputFileUrls().length; ++i){
      if(MyUtil.arrayContainsMatch(job.getDownloadFiles(), job.getInputFileUrls()[i]) ||
          MyUtil.arrayContainsMatch(job.getDownloadFiles(), ".*/"+job.getInputFileUrls()[i])){
        continue;
      }
      nonDlUrls.add(job.getInputFileUrls()[i]);
    }
    return nonDlUrls.toArray(new String[nonDlUrls.size()]);
  }

  // We're using an undocumented feature of GridFactory (Util.parseJobScript, QueueMgr):
  // output files specified as
  // file1 https://some.server/some/dir/file1 file2 https://some.server/some/dir/file2 ...
  // are delivered.
  private String constructOutputFilesString(JobInfo job) {
    StringBuffer ret = new StringBuffer();
    // Don't inform GridFactory about files that will be uploaded by the script itself.
    String[] uploadFileNames = new String[0];
    if(((MyJobInfo) job).getUploadFiles()!=null && ((MyJobInfo) job).getUploadFiles().length>0){
      uploadFileNames = ((MyJobInfo) job).getUploadFiles()[0];
    }
    Debug.debug("Checking output files "+MyUtil.arrayToString(job.getOutputFileNames())+" : "+
        MyUtil.arrayToString(uploadFileNames), 2);
    for(int i=0; i<job.getOutputFileNames().length; ++i){
      if(uploadFileNames!=null && MyUtil.arrayContainsMatch(uploadFileNames, job.getOutputFileNames()[i])){
        continue;
      }
      ret.append(" " + job.getOutputFileNames()[i]);
      if(MyUtil.urlIsRemote(job.getOutputFileDestinations()[i])){
        ret.append(" " + job.getOutputFileDestinations()[i]);
      }
      else{
        ret.append(" " + job.getOutputFileNames()[i]);
      }
    }
    String res = ret.toString().trim();
    Debug.debug("Returning output files string "+res, 2);
    return res;
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
  public boolean killJobs(Set<JobInfo> jobs) {
    boolean ok = true;
    MyJobInfo job = null;
    for(Iterator<JobInfo> it=jobs.iterator(); it.hasNext();){
      job = (MyJobInfo) it.next();
      try{
        getLRMS().kill(MyUtil.getHostFromID(job.getJobId()), new String [] {job.getJobId()}, null);
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
            rDir = job.getOutputFileDestinations()[i].replaceFirst("^(.*/)[^/]+$", "$1");
            mkRemoteDir(rDir);
          }
        }
      }
    }
    catch(Exception e){
      error = "ERROR: will not be able to upload output files. "+e.getMessage();
      logFile.addMessage(error, e);
      return false;
    }
    try{
      setInputFiles(job);
      setOutputFiles(job);
      setRTEs(job);
      if(!MyUtil.setRemoteOutputFiles((MyJobInfo) job, remoteCopyCommands)){
        throw new IOException("Problem with remote output files.");
      }
      if(!MyUtil.setRemoteInputFiles((MyJobInfo) job, remoteCopyCommands)){
        throw new IOException("Problem with remote input files.");
      }
      if(ramMB>0){
        job.setRamMB(ramMB);
      }
      if(runningSeconds>0){
        job.setRunningSeconds(runningSeconds);
      }
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
    try{
      getOutputs((MyJobInfo) job);
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("ERROR: could not download output files from job "+job.getIdentifier()+" : "+job.getJobId(), e);
      ok = false;
    }
    /*try{
      if(ok){
        deliverStdoutErr(job);
      }
    }
    catch(Exception e){
      logFile.addMessage("WARNING: could not deliver stdout/stderr of job "+
          job.getIdentifier()+" / "+job.getName(), e);
    }*/
    ok = ok && super.postProcess(job);
    return ok;
  }
  
  private void deliverStdoutErr(JobInfo job) throws MalformedURLException, Exception{
    File tmpStdout = new File(job.getOutTmp());
    File tmpStderr = new File(job.getErrTmp());
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String finalStdOut = dbPluginMgr.getStdOutFinalDest(job.getIdentifier());
    String finalStdErr = dbPluginMgr.getStdErrFinalDest(job.getIdentifier());
    // Now upload to final destination.
    String protocol = null;
    try{
      protocol = (new GlobusURL(finalStdOut)).getProtocol();
    }
    catch(Exception e){
      //e.printStackTrace();
    }
    if(!MyUtil.isLocalFileName(finalStdOut)){
      if(!protocol.equals("https")){
        transferControl.upload(tmpStdout, finalStdOut);
        transferControl.upload(tmpStderr, finalStdErr);
      }
    }
    else{
      LocalStaticShell.copyFile(tmpStdout.getAbsolutePath(), finalStdOut);
      LocalStaticShell.copyFile(tmpStderr.getAbsolutePath(), finalStdErr);
    }
  }
  
  private void getOutputs(MyJobInfo job) throws MalformedURLException, Exception{
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(((MyJobInfo) job).getDBName());
    String [] outputFiles = dbPluginMgr.getOutputFiles(job.getIdentifier());
    String [] outputDestinations = outputFiles==null?null:new String[outputFiles.length];
    for(int i=0; i<(outputFiles==null?0:outputFiles.length); ++i){
      outputDestinations[i] = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFiles[i]);
    }
    Vector<String> outNamesVec = new Vector<String>();
    Vector<String> outDestsVec = new Vector<String>();
    if(outputFiles!=null && outputFiles.length>0){
      String protocol;
      for(int i=0; i<outputFiles.length; ++i){
        protocol = null;
        try{
          protocol = (new GlobusURL(outputDestinations[i])).getProtocol();
        }
        catch(Exception e){
          //e.printStackTrace();
        }
        if(MyUtil.urlIsRemote(outputDestinations[i])){
          // Files that have been delivered by the script itself
          if(remoteCopyCommands!=null && remoteCopyCommands.containsKey(protocol)){
            outNamesVec.add(outputFiles[i]);
            outDestsVec.add(outputDestinations[i]);
          }
          // Files that have remote https destinations will have been uploaded by GridFactory.
          if(protocol.equalsIgnoreCase("https")){
            outNamesVec.add(outputFiles[i]);
            outDestsVec.add(outputDestinations[i]);
          }
        }
        else{
          //fileTransfer.getFile(
          //   new GlobusURL(job.getJobId()+"/"+outputFiles[i]),
          //   new File(MyUtil.clearTildeLocally(MyUtil.clearFile(runDir(job)))));
          GridPilot.getClassMgr().getTransferStatusUpdateControl().localDownload(
              job.getJobId()+"/"+outputFiles[i], outputFiles[i],
              new File(MyUtil.clearFile(runDir(job))),
              GridPilot.getClassMgr().getCSPluginMgr().copyFileTimeOut);
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
    ret += "Submit host: "+MyUtil.getHostFromID(job.getJobId())+"\n";
    //ret += "DB location: "+job.getDBLocation()+"\n";
    //ret += "Job DB URL: "+job.getDBURL()+"\n";
    try{
      ret += getLRMS().getLongJobStatus(
          MyUtil.getHostFromID(job.getJobId()), job.getJobId(), null);
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
      int st = getLRMS().getJobStatus(MyUtil.getHostFromID(job.getJobId()), job.getJobId(), null);
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
        res = getLRMS().getOutput(MyUtil.getHostFromID(job.getJobId()), job.getJobId(),
            (int) GridPilot.getClassMgr().getCSPluginMgr().currentOutputTimeOut, null);
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
   * @throws Exception 
   */
  public void updateStatus(Vector<JobInfo> jobs) throws Exception{
    Exception ee = null;
    for(int i=0; i<jobs.size(); ++i)
      try{
        updateStatus((MyJobInfo) jobs.get(i));
      }
      catch(Exception e){
        ee = e;
        /*e.printStackTrace();
        logFile.addMessage("WARNING: could not update status of job "+((MyJobInfo) jobs.get(i)).getIdentifier()+
            " : "+((MyJobInfo) jobs.get(i)).getJobId(), e);*/
      }
      if(ee!=null){
        throw ee;
      }
  }

  private void updateStatus(MyJobInfo job) throws IOException, GeneralSecurityException,
     GlobusCredentialException, GSSException{
    int st = getLRMS().getJobStatus(MyUtil.getHostFromID(job.getJobId()), job.getJobId(), null);
    String csStatus = JobInfo.getStatusName(st);
    job.setCSStatus(csStatus);
    job.setStatus(st);
    if(st==JobInfo.STATUS_DONE){
      // This is to make sure we have something to validate
      getCurrentOutput(job);
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
        false, true, new String [] {"Linux", "Windows"}, false);
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
      getLRMS().clean(new String [] {job.getJobId()}, null, null);
    }
    catch(Exception e){
      ret = false;
      e.printStackTrace();
    }    
    return ret;
  }


}
