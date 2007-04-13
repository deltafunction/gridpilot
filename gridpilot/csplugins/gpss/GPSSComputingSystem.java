package gridpilot.csplugins.gpss;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.Timer;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;

import gridpilot.ComputingSystem;
import gridpilot.DBPluginMgr;
import gridpilot.DBRecord;
import gridpilot.DBResult;
import gridpilot.Debug;
import gridpilot.GridPilot;
import gridpilot.JobInfo;
import gridpilot.LogFile;
import gridpilot.Util;

public class GPSSComputingSystem implements ComputingSystem{

  private String error = "";
  private LogFile logFile = null;
  private String [] localRuntimeDBs = null;
  private String remoteDB = null;
  private String csName;
  private HashSet finalRuntimesLocal = null;
  
  // RTEs are refreshed from entries written by pull nodes in remote database
  // every RTE_SYNC_DELAY milliseconds.
  private static int RTE_SYNC_DELAY = 60000;
  
  private Timer timerSyncRTEs = new Timer(0, new ActionListener(){
    public void actionPerformed(ActionEvent e){
      Debug.debug("Syncing RTEs", 2);
      cleanupRuntimeEnvironments();
      setupRuntimeEnvironments();
    }
  });

  public GPSSComputingSystem(String _csName){
    csName = _csName;
    logFile = GridPilot.getClassMgr().getLogFile();
    localRuntimeDBs = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "runtime databases");
    remoteDB = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "remote database");
    timerSyncRTEs.setDelay(RTE_SYNC_DELAY);
  }

  /**
   * Copies local copy of the corresponding jobDefinition plus
   * the associated dataset to the (remote) database, where it
   * will be picked up by pull clients. Sets csState to 'ready'.
   */
  public boolean submit(JobInfo job){
    DBRecord jobRecord = null;
    DBRecord dataset = null;
    String datasetID = null;
    DBRecord transformation = null;
    String transformationID = null;
    DBPluginMgr jobMgr = GridPilot.getClassMgr().getDBPluginMgr(
        job.getDBName());
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(
        remoteDB);
    // First, read the jobDefinition, dataset and transformation.
    try{
      String [] jobDefDatasetReference = Util.getJobDefDatasetReference(job.getDBName());  
      jobRecord = jobMgr.getJobDefinition(job.getJobDefId());
      datasetID = (String) jobRecord.getValue(jobDefDatasetReference[1]);
      dataset = jobMgr.getDataset(datasetID);
    }
    catch(Exception e){
      error = "ERROR: could not read dataset or jobDefinition. "+e.getMessage();
      return false;
    }
    // Now, create the transformation if necessary
    try{
      remoteMgr.createDataset("dataset", dataset.fields, dataset.values);
    }
    catch(Exception e){
      error = "ERROR: could not write dataset or jobDefinition. "+e.getMessage();
      return false;
    }
    
    return true;
  }

  /**
   * Sets csState to 'requestKill' in remote jobDefinition record.
   */
  public boolean killJobs(Vector jobs){
    // TODO
    return false;
  }

  /**
   * Clean up on remote gridftp server.
   */
  public void clearOutputMapping(JobInfo job){
    // TODO
  }

  /**
   * Returns csStatus of remote jobDefinition record.
   */
  public String getFullStatus(JobInfo job){
    // TODO
    return null;
  }

  /**
   * If job is running: deletes stdout and stderr on gridftp server,
   * sets csStatus to 'requestStdout', waits for files to reappear,
   * then resets csStatus to its previous value.
   * If job is done, just reads finalStdout and finalStderr.
   */
  public String[] getCurrentOutputs(JobInfo job, boolean resyncFirst)
      throws IOException{
    // TODO
    return null;
  }

  /**
   * Creates 'local' copy of corresponding jobDefinition. For this copy:
   * If finalStdout, finalStderr or any output files are local:
   * - Creates temporary directory on the configured gridftp server.
   * - Uploads any local input files and sets inputFiles accordingly.
   * - Sets finalStdOut and finalStdErr and any local output files 
   *   to files in this directory.
   */
  public boolean preProcess(JobInfo job){
    // TODO
    return false;
  }

  /**
   * - Downloads any files specified as local in the original jobDefinition
   *   from the remote temporary location.
   * - Deletes the remote temporary location.
   */
  public boolean postProcess(JobInfo job){
    // TODO
    return false;
  }

  public void updateStatus(Vector jobs){
    // TODO
  }

  public String[] getScripts(JobInfo job){
    // Nothing to do: we cannot get the scripts produced by the
    // remote GridPilot. - we could introduce another csStatus trigger
    // to have the remote GridPilot call its getScripts and upload the
    // scripts to the remote gridftp directory. But I don't really think
    // it's worth it...
    return null;
  }

  public String getUserInfo(String csName){
    String user = null;
    try{
      Debug.debug("getting credential", 3);
      GSSCredential credential = GridPilot.getClassMgr().getGridCredential();
      GlobusCredential globusCred = null;
      if(credential instanceof GlobusGSSCredentialImpl){
        globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
      }
      Debug.debug("getting identity", 3);
      user = globusCred.getIdentity();
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

  public void setupRuntimeEnvironments(String csName){
    setupRuntimeEnvironments();
  }
  
  /**
   * Simply copies over runtime environment records from the defined
   * "remote database" to the defined "runtime databases".
   */
  public void setupRuntimeEnvironments(){
    String certificate = null;
    DBResult rtes = null;
    DBPluginMgr remoteDBMgr = null;
    DBPluginMgr localDBMgr = null;
    DBRecord rte = null;
    try{
      remoteDBMgr = GridPilot.getClassMgr().getDBPluginMgr(remoteDB);
    }
    catch(Exception e){
      error = "Could not load remote runtime DB "+remoteDB+"."+e.getMessage();
      Debug.debug(error, 1);
      return;
    }
    rtes = remoteDBMgr.getRuntimeEnvironments();
    // Write records in the 'local' runtime DBs,
    // clearing the certificate.
    for(int ii=0; ii<localRuntimeDBs.length; ++ii){
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(localRuntimeDBs[ii]);
      }
      catch(Exception e){
        error = "Could not load local runtime DB "+localRuntimeDBs[ii]+"."+e.getMessage();
        Debug.debug(error, 1);
      }
      for(int i=0; i<rtes.values.length; ++i){
        rte = rtes.getRow(i);
        // Only copy records put there by pull clients.
        certificate = (String) rte.getValue("certificate");
        if(certificate==null || certificate.equals("")){
          continue;
        }
        try{
          // Clear the certificate,
          // in order to avoid confusion and have the record delete on exit.
          rte.setValue("certificate", "");
          localDBMgr.createRuntimeEnv(rte.fields, rte.values);
        }
        catch(Exception e1){
          e1.printStackTrace();
          error = "WARNING: could not create runtime environment " +
          rtes.getRow(i).getValue("name")+" in database "+localDBMgr.getDBName();
          Debug.debug(error, 1);
          continue;
        }
        if(ii==0){
          try{
            finalRuntimesLocal.add(localRuntimeDBs[ii]);
          }
          catch(Exception e){
            e.printStackTrace();
          }
        }
      }
    }
  }

  public void exit(){
    cleanupRuntimeEnvironments();
  }
  
  /**
   * Clean up runtime environment records copied from "remote database".
   */
  public void cleanupRuntimeEnvironments(){
    String runtimeName = null;
    String certificate = null;
    String id = "-1";
    boolean ok = true;
    DBPluginMgr localDBMgr = null;
    for(int i=0; i<localRuntimeDBs.length; ++i){
      localDBMgr = null;
      try{
        localDBMgr = GridPilot.getClassMgr().getDBPluginMgr(
            localRuntimeDBs[i]);
      }
      catch(Exception e){
        error = "Could not load local runtime DB "+localRuntimeDBs[i]+"."+e.getMessage();
        Debug.debug(error, 1);
      }
      if(localDBMgr!=null){
        for(Iterator it=finalRuntimesLocal.iterator(); it.hasNext();){
          ok = true;
          runtimeName = (String) it.next();
          id = localDBMgr.getRuntimeEnvironmentID(runtimeName, csName);
          if(!id.equals("-1")){
            // Don't delete records with a non-empty certificate.
            // These were put there by pull clients.
            certificate = (String) localDBMgr.getRuntimeEnvironment(id).getValue("certificate");
            if(certificate!=null && !certificate.equals("")){
              continue;
            }
            ok = localDBMgr.deleteRuntimeEnvironment(id);
          }
          else{
            ok = false;
          }
          if(!ok){
            error = "WARNING: could not delete runtime environment " +
            runtimeName+" from database "+localDBMgr.getDBName();
            Debug.debug(error, 1);
          }
        }
      }
    }
  }

  public String getError(String csName){
    return error;
  }

}
