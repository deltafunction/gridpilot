package gridpilot.csplugins.gpss;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
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
import gridpilot.PullJobsDaemon;

public class GPSSComputingSystem implements ComputingSystem{

  private String error = "";
  private LogFile logFile = null;
  private String [] localRuntimeDBs = null;
  private String remoteDB = null;
  private String csName;
  private HashSet finalRuntimesLocal = null;
  private String user = null;

  
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
    // Set user
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
  }

  /**
   * Copies jobDefinition plus the associated dataset to the (remote) database,
   * where it will be picked up by pull clients. Sets csState to 'ready'.
   * Sets finalStdOut and finalStdErr and any local output files 
   * to files in the remote gridftp directory.
   */
  public boolean submit(JobInfo job){
    DBRecord jobDefinition = null;
    DBRecord dataset = null;
    String datasetID = null;
    DBRecord transformation = null;
    String transformationName = null;
    String transformationVersion = null;
    String transformationID = null;
    DBPluginMgr jobDbMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(
        remoteDB);
    // First, read the jobDefinition, dataset and transformation.
    try{
      jobDefinition = jobDbMgr.getJobDefinition(job.getJobDefId());
      datasetID = jobDbMgr.getJobDefDatasetID(job.getJobDefId());
      dataset = jobDbMgr.getDataset(datasetID);
      transformationName = jobDbMgr.getDatasetTransformationName(datasetID);
      transformationVersion = jobDbMgr.getDatasetTransformationVersion(datasetID);
      transformationID = jobDbMgr.getTransformationID(transformationName, transformationVersion);
      transformation = jobDbMgr.getTransformation(transformationID);
      // Hmm, 7 database lookups. Perhaps we should reconsider this...
    }
    catch(Exception e){
      error = "ERROR: could not read jobDefinition, dataset or transformation. "+e.getMessage();
      return false;
    }
    try{
      // Create the transformation if necessary
      transformationID = remoteMgr.getTransformationID(transformationName, transformationVersion);
      if(transformationID==null || transformationID.equals("-1")){
        remoteMgr.createTrans(transformation.fields, transformation.values);
      }
      // Tag it for deletion
      tagDeleteTransformation(remoteMgr, transformationID);
      // Create the dataset if necessary
      String datasetName = null;
      try{
        datasetName = remoteMgr.getDatasetName(datasetID);
      }
      catch(Exception ee){
      }
      if(datasetName==null || datasetName.equals("")){
        remoteMgr.createDataset("dataset", dataset.fields, dataset.values);
      }
      // Tag it for deletion
      tagDeleteDataset(remoteMgr, datasetID);
      // Modify and write the jobDefinition
      jobDefinition.setValue("csStatus", PullJobsDaemon.STATUS_READY);
      jobDefinition = updateURLs(jobDefinition);
      remoteMgr.createJobDef(jobDefinition.fields, jobDefinition.values);
    }
    catch(Exception e){
      error = "ERROR: could not write to 'remote' database. "+e.getMessage();
      return false;
    }
    return true;
  }
  
  private void getRemoteDir(DBRecord jobDefinition){
    // TODO Auto-generated method stub
    
  }
  
  private DBRecord updateURLs(DBRecord jobDefinition){
    // TODO
    try{
      getRemoteDir(jobDefinition);
    }
    catch(Exception e){
      error = "ERROR: could not set remote output files. "+e.getMessage();
     }
    return jobDefinition;
  }

  /**
   * Tags a transformation record, copied to a remote database only to be
   * able to run a job, for deletion once the job has finished.
   * If the transformation happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void tagDeleteTransformation(DBPluginMgr remoteMgr, String transformationID){
    String comment = (String) remoteMgr.getTransformation(transformationID).getValue("comment");
    if(comment!=null && comment.startsWith("volatile:")){
      if(!comment.matches("volatile::'"+user+"'.*") &&
          !comment.matches("volatile::.*::'"+user+"'.*")){
        comment = comment+"::'"+user+"'";
      }
    }
    else{
      comment = "volatile::'"+user+"'";
    }
  }

  /**
   * Deletes transformation tagged for deletion.
   * If a transformation happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void deleteTaggedTransformation(DBPluginMgr remoteMgr, String transformationID){
    String comment = (String) remoteMgr.getTransformation(transformationID).getValue("comment");
    if(comment!=null && comment.startsWith("volatile:")){
      if(!comment.matches("volatile::'"+user+"'")){
        remoteMgr.deleteTransformation(transformationID);
      }
      else{
        comment = comment.replaceFirst("::'"+user+"'", "");
        remoteMgr.updateTransformation(transformationID, new String [] {"comment"}, new String [] {comment});
      }
    }
  }

  /**
   * Tags a dataset record, copied to a remote database only to be
   * able to run a job, for deletion once the job has finished.
   * If the dataset happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void tagDeleteDataset(DBPluginMgr remoteMgr, String datasetID){
    String metaData = (String) remoteMgr.getDataset(datasetID).getValue("metaData");
    if(metaData!=null && metaData.startsWith("volatile:")){
      if(!metaData.matches("volatile::'"+user+"'.*") &&
          !metaData.matches("volatile::.*::'"+user+"'.*")){
        metaData = metaData+"::'"+user+"'";
      }
    }
    else{
      metaData = "volatile::'"+user+"'";
    }
  }

  /**
   * Deletes transformation tagged for deletion.
   * If a transformation happens to be used by other GPSS jobs, this is
   * taken into account - i.e. it is not deleted until the last one has finished.
   */
  private void deleteTaggedDataset(DBPluginMgr remoteMgr, String datasetID){
    String metaData = (String) remoteMgr.getDataset(datasetID).getValue("metaData");
    if(metaData!=null && metaData.startsWith("volatile:")){
      if(!metaData.matches("volatile::'"+user+"'")){
        remoteMgr.deleteDataset(datasetID, false);
      }
      else{
        metaData = metaData.replaceFirst("::'"+user+"'", "");
        remoteMgr.updateDataset(datasetID, new String [] {"metaData"}, new String [] {metaData});
      }
    }
  }

  /**
   * Sets csState to 'requestKill' in remote jobDefinition record.
   */
  public boolean killJobs(Vector jobs){
    boolean ok = true;
    // Group the jobs by DB
    HashMap jobsMap = new HashMap();
    Enumeration en = jobs.elements();
    JobInfo job = null;
    while(en.hasMoreElements()){
      job = (JobInfo) en.nextElement();
      if(!jobsMap.containsKey(job.getDBName())){
        jobsMap.put(job.getDBName(), new Vector());
      }
      ((Vector) jobsMap.get(job.getDBName())).add(job);
    }
    Vector dbJobs = null;
    DBPluginMgr mgr = null;
    String dbName = null;
    // Update each job
    for(Iterator it=jobsMap.keySet().iterator(); it.hasNext();){
      dbName = (String) it.next();
      mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
      dbJobs = (Vector) jobsMap.get(dbName);
      en = dbJobs.elements();
      while(en.hasMoreElements()){
        job = (JobInfo) en.nextElement();
        ok = ok && mgr.updateJobDefinition(job.getJobDefId(),
            new String [] {"csState"}, new String [] {PullJobsDaemon.STATUS_REQUESTED_KILLED});
      }
    }
    return ok;
  }

  /**
   * Creates 'local' copy of corresponding jobDefinition. For this copy:
   * If finalStdout, finalStderr or any output files are local:
   * - Creates temporary directory on the configured gridftp server.
   * - Uploads any local input files and sets inputFiles accordingly.
   */
  public boolean preProcess(JobInfo job){
    
    
    
    
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
   * - Downloads any files specified as local in the original jobDefinition
   *   from the remote temporary location.
   * - Deletes the remote temporary location.
   * - Cleans up temporary transformation and dataset if any such was written.
   */
  public boolean postProcess(JobInfo job){
    // TODO
    
    // Clean up temporary transformation and dataset
    DBPluginMgr remoteMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String datasetID = remoteMgr.getJobDefDatasetID(job.getJobDefId());
    String transformationName = remoteMgr.getDatasetTransformationName(datasetID);
    String transformationVersion = remoteMgr.getDatasetTransformationVersion(datasetID);
    String transformationID = remoteMgr.getTransformationID(transformationName, transformationVersion);
    deleteTaggedDataset(remoteMgr, datasetID);
    deleteTaggedTransformation(remoteMgr, transformationID);
    return false;
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
