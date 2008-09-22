package gridpilot.csplugins.gridfactory;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.Timer;


import org.globus.util.GlobusURL;

import gridfactory.common.ConfigFile;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.JobInfo;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.Shell;
import gridfactory.common.Util;
import gridfactory.lrms.LRMS;
import gridpilot.MyComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.MyJobInfo;
import gridpilot.RteRdfParser;

import gridpilot.MyUtil;
import gridpilot.csplugins.fork.ForkComputingSystem;
import gridpilot.csplugins.fork.ForkScriptGenerator;

public class GridFactoryComputingSystem extends ForkComputingSystem implements MyComputingSystem{

  private String submitURL = null;
  private String submitHost = null;
  private String user = null;
  private FileTransfer fileTransfer = null;
  private LRMS lrms = null;
  // Map of id -> DBPluginMgr.
  // This is to be able to clean up RTEs from catalogs on exit.
  private HashMap toDeleteRtes = new HashMap();
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
      MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, localRuntimeDBs, toDeleteRtes);
    }
  });

  public GridFactoryComputingSystem(String _csName) throws Exception{
    super(_csName);
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    submitURL = configFile.getValue(csName, "submission url");
    submitHost = (new GlobusURL(submitURL)).getHost();
    rteCatalogUrls = configFile.getValues("GridPilot", "runtime catalog URLs");
    timerSyncRTEs.setDelay(RTE_SYNC_DELAY);
    String virtualizeStr = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "virtualize");
    virtualize = virtualizeStr!=null && (virtualizeStr.equalsIgnoreCase("yes") || virtualizeStr.equalsIgnoreCase("true"));
    allowedVOs = GridPilot.getClassMgr().getConfigFile().getValues(csName, "allowed subjects");
    try{
      // Set user
      user = GridPilot.getClassMgr().getSSL().getGridSubject();            
    }
    catch(Exception ioe){
      error = "ERROR during initialization of GridFactory plugin\n" +
      "\tException\t: " + ioe.getMessage();
      logFile.addMessage(error, ioe);
    }
    lrms = LRMS.constructLrms(true, GridPilot.getClassMgr().getSSL(), null, null);
  }
  
  /**
   * Add the input files of the transformation to job.getInputFiles().
   * @param job the job in question
   */
  private void setExtraInputFiles(MyJobInfo job){
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String transformationID = dbPluginMgr.getJobDefTransformationID(job.getIdentifier());
    String [] transformationInputs = dbPluginMgr.getTransformationInputs(transformationID);
    if(transformationInputs==null || transformationInputs.length==0){
      return;
    }
    int initialLen = job.getInputFileUrls()!=null&&job.getInputFileUrls().length>0?job.getInputFileUrls().length:0;
    String [] newInputs = new String [initialLen+transformationInputs.length];
    for(int i=0; i<initialLen; ++i){
      newInputs[i] = job.getInputFileUrls()[i];
    }
    for(int i=initialLen; i<initialLen+transformationInputs.length; ++i){
      newInputs[i] = transformationInputs[i];
    }
    job.setInputFileUrls(newInputs);
  }

  /**
   * Copies jobDefinition plus the associated dataset to the (remote) database,
   * where it will be picked up by pull clients. Sets csState to 'ready'.
   * Sets finalStdOut and finalStdErr and any local output files 
   * to files in the remote gridftp directory.
   */
  public boolean submit(MyJobInfo job){
    /*
     * Flags that can be set in the job script.
     * <br><br>
     *  -n [job name]<br>
     *  -i [input file 1] [input file 2] ...<br>
     *  -e [executable 1] [executable 2] ...<br>
     *  -t [running time on a 2.8 GHz Intel Pentium 4 processor]<br>
     *  -m [required memory in megabytes]<br>
     *  -z whether or not to require that the job should run in its own virtual
           machine<br>
     *  -o [output files]<br>
     *  -r [runtime environment 1] [runtime environment 2] ...<br>
     *  -s [distinguished name] : DN identifying the submitter<br><br>
     *  -v [allowed virtual organization 1] [allowed virtual organization 2] ...<br>
     *  -u [id] : unique identifier of the form https://server/job/dir/hash
     */
    try{
      setExtraInputFiles(job);
      job.setAllowedVOs(allowedVOs);
      // Create a standard shell script.
      ForkScriptGenerator scriptGenerator = new ForkScriptGenerator(((MyJobInfo) job).getCSName(), runDir(job));
      if(!scriptGenerator.createWrapper(shellMgr, (MyJobInfo) job, job.getName()+commandSuffix)){
        throw new IOException("Could not create wrapper script.");
      }
      String [] values = new String []{job.getName(), Util.arrayToString(job.getInputFileUrls()),
          Util.arrayToString(job.getExecutables()), Integer.toString(job.getGridTime()), Integer.toString(job.getMemory()),
          virtualize?"1":"0", Util.arrayToString(job.getOutputFileNames()), Util.arrayToString(job.getRTEs()),
              job.getUserInfo(), Util.arrayToString(job.getAllowedVOs(), job.getJobId())};

      // If this is a resubmit, first delete the old remote jobDefinition
      try{
        if(job.getJobId()!=null && job.getJobId().length()>0){
          lrms.clean(new String [] {job.getJobId()}, null);
        }
      }
      catch(Exception e){
      }
      
      String cmd = runDir(job)+"/"+job.getName()+commandSuffix;
      String id = lrms.submit(cmd, values, submitURL, true, GridPilot.getClassMgr().getSSL().getGridSubject());
      job.setJobId(id);
    }
    catch(Exception e){
      error = "ERROR: could not submit job. "+e.getMessage();
      return false;
    }
    return true;
  }
  
  private void mkRemoteDir(String url) throws Exception{
    if(!url.endsWith("/")){
      throw new IOException("Directory URL: "+url+" does not end with a slash.");
    }
    GlobusURL globusUrl= new GlobusURL(url);
    if(fileTransfer==null){
      fileTransfer = GridPilot.getClassMgr().getFTPlugin(globusUrl.getProtocol());
    }
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
  public boolean killJobs(Vector jobs) {
    boolean ok = true;
    Enumeration en = jobs.elements();
    MyJobInfo job = null;
    while(en.hasMoreElements()){
      job = (MyJobInfo) en.nextElement();
      try{
        lrms.kill(submitHost, new String [] {job.getJobId()});
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
   * - Creates temporary directory on the configured gridftp server.
   * - Uploads any local input files and sets inputFiles accordingly.
   */
  public boolean preProcess(JobInfo job){
    // Iff the job has remote output files, check if the remote directory exists,
    // otherwise see if we can create it.
    try{
      if(job.getOutputFileDestinations()!=null){
        String rDir = null;
        for(int i=0; i<job.getOutputFileDestinations().length; ++i){
          if(Util.urlIsRemote(job.getOutputFileDestinations()[i])){
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
    return true;
  }

  /**
   * - Downloads any files specified as local in the original jobDefinition
   *   from the remote temporary location.
   * - Deletes the remote temporary location.
   * - Cleans up temporary transformation and dataset if any such was written.
   * - Deletes remote jobDefinition.
   */
  public boolean postProcess(JobInfo job){
    boolean ok = true;
    if(((MyJobInfo) job).getCSStatus().equals(JobInfo.getStatusName(JobInfo.STATUS_DONE))){
      // Delete the run directory
      try{
        ok = shellMgr.deleteDir(runDir(job));
      }
      catch(Exception e){
        e.printStackTrace();
        ok = false;
      }
    }
    return ok;
  }

  /**
   * Returns csStatus of remote jobDefinition record.
   */
  public String getFullStatus(JobInfo job){
    try{
      return JobInfo.getStatusName(lrms.getJobStatus(submitHost, job.getJobId()));
    }
    catch(Exception e){
      e.printStackTrace();
      logFile.addMessage("WARNING: could not get status of job "+job.getIdentifier(), e);
      return null;
    }
  }

  /**
   * If job is running: deletes stdout and stderr on gridftp server,
   * sets csStatus to 'requestStdout', waits for files to reappear,
   * then resets csStatus to its previous value.
   * If job is done, just reads finalStdout and finalStderr.
   */
  public String[] getCurrentOutput(JobInfo job) {
   String [] res = new String[2];
    File tmpStdout = null;
    File tmpStderr = null;   
    try{
      int st = lrms.getJobStatus(submitHost, job.getJobId());
      if(st==JobInfo.STATUS_DONE || st==JobInfo.STATUS_FAILED || st==JobInfo.STATUS_UPLOADED){
        tmpStdout = File.createTempFile(/*prefix*/"GridPilot-stdout", /*suffix*/"");
        tmpStderr = File.createTempFile(/*prefix*/"GridPilot-stderr", /*suffix*/"");
        String finalStdOut = job.getStdoutDest();
        String finalStdErr = job.getStderrDest();
        // stdout
        if(finalStdOut.startsWith("file:")){
          res[0] = LocalStaticShell.readFile(finalStdOut);
        }
        else if(MyUtil.urlIsRemote(finalStdOut)){
          try{
            fileTransfer.getFile(new GlobusURL(finalStdOut), tmpStdout.getParentFile());
          }
          catch(Exception e){
            e.printStackTrace();
            throw new IOException("ERROR: could not download stdout. "+e.getMessage());
          }
          res[0] = LocalStaticShell.readFile(tmpStdout.getAbsolutePath());
        }
        else{
          throw new IOException("Cannot access local files on remote system");
        }
        // stderr
        if(finalStdErr.startsWith("file:")){
          res[1] = LocalStaticShell.readFile(finalStdErr);
        }
        else if(MyUtil.urlIsRemote(finalStdErr)){
          boolean ok = true;
          try{
            fileTransfer.getFile(new GlobusURL(finalStdErr), tmpStderr.getParentFile());
          }
          catch(Exception e){
            ok = false;
            e.printStackTrace();
            throw new IOException("ERROR: could not download stderr. "+e.getMessage());
          }
          if(ok){
            res[1] = LocalStaticShell.readFile(tmpStderr.getAbsolutePath());
          }
        }
        else{
          throw new IOException("Cannot access local files on remote system");
        }
      }
      else if(st==JobInfo.STATUS_RUNNING){
        res = lrms.getOutput(submitHost, job.getJobId(), GridPilot.getClassMgr().getCSPluginMgr().currentOutputTimeOut);
      }
      else{
        throw new IOException("Job is not in a state that allows getting output - "+JobInfo.getStatusName(st));
      }
    }
    catch(Exception ee){
      ee.printStackTrace();
      logFile.addMessage("WARNING: could not get current output of job "+job.getIdentifier(), ee);
    }
    finally{
      try{
        if(!LocalStaticShell.deleteFile(tmpStdout.getAbsolutePath()) ||
            !LocalStaticShell.deleteFile(tmpStderr.getAbsolutePath())){
           error = "WARNING: could not delete stdout or stderr temp file.";
           logFile.addMessage(error);
         }
      }
      catch(Exception eee){
        eee.printStackTrace();
      }
    }
    return res;
  }

  /**
   * Finds requested jobs, checks if provider is allowed to run them;
   * if so, sets permissions on input files accordingly.
   */
  public void updateStatus(Vector jobs){
    for(int i=0; i<jobs.size(); ++i)
      try{
        updateStatus((MyJobInfo) jobs.get(i));
      }
      catch(Exception e){
        e.printStackTrace();
        logFile.addMessage("WARNING: could not update status of job "+((MyJobInfo) jobs.get(i)).getIdentifier(), e);
      }
  }

  private void updateStatus(MyJobInfo job) throws IOException, GeneralSecurityException{
    int st = lrms.getJobStatus(submitHost, job.getJobId());
    String csStatus = JobInfo.getStatusName(st);
    job.setCSStatus(csStatus);
    job.setStatus(st);
    Debug.debug("Updating status of job "+job.getName()+" : "+job.getCSStatus()+" : "+csStatus, 2);
    if(csStatus==null || csStatus.equals("")){
      logFile.addMessage("ERROR: no csStatus for job "+job.getIdentifier());
      return;
    }
  }
  
  public String getUserInfo(String csName){
    return user;
  }

  public void setupRuntimeEnvironments(String csName){
    MyUtil.syncRTEsFromCatalogs(csName, rteCatalogUrls, localRuntimeDBs, toDeleteRtes);
  }

  public void exit(){
  }
  
  /**
   * Clean up runtime environment records copied from runtime catalog URLs.
   */
  public void cleanupRuntimeEnvironments(String csName){
    MyUtil.cleanupRuntimeEnvironments(csName, localRuntimeDBs, toDeleteRtes);
  }

  public Shell getShell(JobInfo job){
    return null;
  }

}
