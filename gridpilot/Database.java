package gridpilot;

import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;

import java.sql.SQLException;

public interface Database{
    
  public static final int DEFINED = 1;
  public static final int SUBMITTED = 2;
  public static final int VALIDATED = 3;
  public static final int UNDECIDED = 4;
  public static final int FAILED = 5;
  public static final int ABORTED = 6;
  public static final int UNEXPECTED = 7;
  
  public void disconnect();
  // TODO: implement in plugins and make menu point active.
  public void clearCaches();
  
  /**
   * This is the parser of the select request from SelectPanel.
   * The last returned column must be the identifier.
   * The parameter findAll is used only to choose whether or not
   * to query all file catalogs for PFNs.
   * 
   * @param selectRequest
   * @param idField
   * @param findAll
   * @return DBResult object
   */
  public DBResult select(String selectRequest, String idField, boolean findAll);
  
  /**
   * This is used by {@link MyUtil#importToDB()}.
   * @param sql
   */
  public void executeUpdate(String sql) throws Exception;
  
  // ####### RuntimeEnvironment table
  public String [] getRuntimeEnvironments(String jobDefID) throws InterruptedException;
  public DBResult getRuntimeEnvironments() throws InterruptedException;
  public String [] getRuntimeEnvironmentIDs(String name, String cs) throws InterruptedException;
  public DBRecord getRuntimeEnvironment(String runtimeEnvironmentID) throws InterruptedException;
  public String getRuntimeInitText(String pack, String cluster) throws InterruptedException;
  public boolean createRuntimeEnvironment(Object [] values) throws InterruptedException;
  public boolean updateRuntimeEnvironment(String runtimeEnvironmentID, String [] fields, String [] values) throws InterruptedException;
  public boolean deleteRuntimeEnvironment(String runtimeEnvironmentID) throws InterruptedException;

  // ####### Transformation table
  public DBResult getTransformations() throws InterruptedException;
  public DBRecord getTransformation(String transformationID) throws InterruptedException;
  public String getTransformationID(String transName, String transVersion) throws InterruptedException;
  public boolean createTransformation(Object [] values) throws InterruptedException;
  public boolean updateTransformation(String transformatinID, String [] fields, String [] values) throws InterruptedException;
  public boolean deleteTransformation(String transformationID) throws InterruptedException;
  public String [] getVersions(String transformationName) throws InterruptedException;
  public String getTransformationRuntimeEnvironment(String transformationID) throws InterruptedException;
  public String [] getTransformationJobParameters(String transformationID) throws InterruptedException;
  /**
   * the input file of a job script are defined by the fields
   * transformation.inputFiles (fully qualified names),
   * jobDefinition.inputFileURLs (fully qualified names)
   * - generated using dataset.inputDataset, dataset.inputDB).
   * 
   * the output files of a job script are defined by the fields
   * jobDefinition.outFileMapping,
   * - generated using transformation.outputFiles, dataset.outputLocation
   */
  public String [] getTransformationOutputs(String transformationID) throws InterruptedException;
  public String [] getTransformationInputs(String transformationID) throws InterruptedException;

  // ####### Dataset table
  public String getDatasetName(String datasetID) throws InterruptedException;
  public String getDatasetID(String datasetName) throws InterruptedException;
  public String getRunNumber(String datasetID) throws InterruptedException;
  public boolean createDataset(String targetTable, String[] fields, Object [] values) throws InterruptedException;
  // the parameter datasetName is redundant and is only used by AtlasDatabase - because DQ2 cannot lookup dsn name by vuid...
  public boolean updateDataset(String datasetID, String datasetName, String [] fields, String [] values) throws InterruptedException;
  public boolean deleteDataset(String datasetID, boolean cleanup) throws InterruptedException;
  public DBRecord getDataset(String datasetID) throws InterruptedException;
  public String getDatasetTransformationName(String datasetID) throws InterruptedException;
  public String getDatasetTransformationVersion(String datasetID) throws InterruptedException;

  // ####### Job definition table
  public boolean isJobRepository() throws InterruptedException;
  // the convention here is that when datasetID is set to -1,
  // all jobDefinitions are returned
  public DBResult getJobDefinitions(String datasetID, String [] fieldNames, String [] statusList, String [] csStatusList) throws InterruptedException;
  public DBRecord getJobDefinition(String jobDefID) throws InterruptedException;
  public boolean createJobDefinition(String [] values) throws InterruptedException;
  public boolean createJobDefinition(String datasetName, String [] cstAttrNames,
      String [] resCstAttr, String [] trpars, String [] [] ofmap, String odest,
      String edest) throws InterruptedException;
  public boolean deleteJobDefinition(String jobDefID, boolean cleanup) throws InterruptedException;
  public boolean updateJobDefinition(String jobDefID, String [] fields, String [] values) throws InterruptedException;
  // Here the following fields are assumed:
  // "user", "jobDefID", "jobName", "stdOut", "stdErr"
  public boolean updateJobDefinition(String jobDefID, String [] values) throws InterruptedException;
  public String getJobDefStatus(String jobDefID) throws InterruptedException;
  public String getJobDefUserInfo(String jobDefID) throws InterruptedException;
  public String getJobDefName(String jobDefID) throws InterruptedException;
  public String getJobDefDatasetID(String jobDefID) throws InterruptedException;
  public String getJobDefTransformationID(String jobDefID) throws InterruptedException;
  public String getTransformationScript(String jobDefID) throws InterruptedException;
  
  // ####### File table
  public boolean isFileCatalog() throws InterruptedException;
  // lookupPFNs must be 0, 1 or 2: 0=no lookup, 1=lookup one pfn, 2=lookup all pfns
  public DBRecord getFile(String datasetName, String fileID, int lookupPFNs) throws InterruptedException;
  public String getFileID(String datasetName, String fileID) throws InterruptedException;
  // return a list of catalog servers and a list of URLs
  public String [][] getFileURLs(String datasetName, String fileID, boolean findAll) throws InterruptedException;
  // For file catalogs only: register an lfn/pfn pair.
  // TODO: if fileID/lfn do not exist, create.
  // datasetComplete is ignored by other than ATLAS
  public DBResult getFiles(String datasetID) throws InterruptedException;
  public void registerFileLocation(String datasetID, String datasetName,
      String fileID, String lfn, String url, String size, String checksum, boolean datasetComplete) throws Exception;
  public boolean deleteFiles(String datasetID, String [] fileIDs, boolean cleanup) throws InterruptedException;
  
  // ####### Job execution
  public String getRunInfo(String jobDefID, String key) throws InterruptedException;
  public boolean cleanRunInfo(String jobDefID) throws InterruptedException;
  public boolean reserveJobDefinition(String jobDefID, String UserName, String cs) throws InterruptedException;
  public String [] getOutputFiles(String jobDefID) throws Exception;
  public String [] getJobDefInputFiles(String jobDefID) throws Exception;
  public String getJobDefOutRemoteName(String jobDefID, String par) throws Exception;
  public String getJobDefOutLocalName(String jobDefID, String par) throws Exception;
  public String getStdOutFinalDest(String jobDefID) throws InterruptedException;
  public String getStdErrFinalDest(String jobDefID) throws InterruptedException;
  public String [] getTransformationArguments(String jobDefID) throws InterruptedException;
  public String [] getJobDefTransPars(String jobDefID) throws Exception;
  
  // ####### Misc
  public boolean setJobDefsField(String [] identifiers, String field, String value) throws InterruptedException;
  public String [] getFieldNames(String table) throws SQLException, InterruptedException;
  // The last database error reported
  public String getError() throws InterruptedException;
  public void appendError(String error);
  public void clearError();
 // method used by DBPluginMgr to request stopping all queries
  public void requestStop();
  public void clearRequestStop();
  // Method used for DBs (ATLAS) that have time consuming PFN lookups.
  public void requestStopLookup();
  public void clearRequestStopLookup();
 
}