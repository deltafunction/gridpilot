package gridpilot;

import java.sql.SQLException;

public interface Database{
    
  public static final int DEFINED = 1;
  public static final int SUBMITTED = 2;
  public static final int VALIDATED = 3;
  public static final int UNDECIDED = 4;
  public static final int FAILED = 5;
  public static final int ABORTED = 6;
  public static final int UNEXPECTED = 7;
  
  public String connect() throws SQLException;
  public void disconnect();
  // TODO: implement in plugins and make menu point active.
  public void clearCaches();
  
  // This is the parser of the select request from SelectPanel.
  // The last returned column must be the identifier.
  // The parameter findAll is used only to choose whether or not
  // to query all file catalogs for PFNs.
  public DBResult select(String selectRequest, String identifier,
      boolean findAll);
  
  // ####### RuntimeEnvironment table
  public String [] getRuntimeEnvironments(String jobDefID);
  public DBResult getRuntimeEnvironments();
  public String getRuntimeEnvironmentID(String name, String cs);
  public DBRecord getRuntimeEnvironment(String runtimeEnvironmentID);
  public String getRuntimeInitText(String pack, String cluster);
  public boolean createRuntimeEnvironment(Object [] values);
  public boolean updateRuntimeEnvironment(String runtimeEnvironmentID, String [] fields, String [] values);
  public boolean deleteRuntimeEnvironment(String runtimeEnvironmentID);

  // ####### Transformation table
  public DBResult getTransformations();
  public DBRecord getTransformation(String transformationID);
  public String getTransformationID(String transName, String transVersion);
  public boolean createTransformation(Object [] values);
  public boolean updateTransformation(String transformatinID, String [] fields, String [] values);
  public boolean deleteTransformation(String transformationID);
  public String [] getVersions(String transformationName);
  public String getTransformationRuntimeEnvironment(String transformationID);
  public String [] getTransformationJobParameters(String transformationID);
  /**
   * the input file of a job script are defined by the fields
   * transformation.inputFiles (fully qualified names),
   * jobDefinition.inputFileNames (fully qualified names)
   * - generated using dataset.inputDataset, dataset.inputDB).
   * 
   * the output files of a job script are defined by the fields
   * jobDefinition.outFileMapping,
   * - generated using transformation.outputFiles, dataset.outputLocation
   */
  public String [] getTransformationOutputs(String transformationID);
  public String [] getTransformationInputs(String transformationID);

  // ####### Dataset table
  public String getDatasetName(String datasetID);
  public String getDatasetID(String datasetName);
  public String getRunNumber(String datasetID);
  public boolean createDataset(String targetTable, String[] fields, Object [] values);
  public boolean updateDataset(String datasetID, String [] fields, String [] values);
  public boolean deleteDataset(String datasetID, boolean cleanup);
  public DBRecord getDataset(String datasetID);
  public String getDatasetTransformationName(String datasetID);
  public String getDatasetTransformationVersion(String datasetID);

  // ####### Job definition table
  // the convention here is that when datasetID is set to -1,
  // all jobDefinitions are returned
  public DBResult getJobDefinitions(String datasetID, String [] fieldNames);
  public DBRecord getJobDefinition(String jobDefID);
  public boolean createJobDefinition(String [] values);
  public boolean createJobDefinition(String datasetName, String [] cstAttrNames,
      String [] resCstAttr, String [] trpars, String [] [] ofmap, String odest,
      String edest);
  public boolean deleteJobDefinition(String jobDefID);
  public boolean updateJobDefinition(String jobDefID, String [] fields, String [] values);
  // Here the following fields are assumed:
  // "user", "jobDefID", "jobName", "stdOut", "stdErr"
  public boolean updateJobDefinition(String jobDefID, String [] values);
  public String getJobDefStatus(String jobDefID);
  public String getJobDefUserInfo(String jobDefID);
  public String getJobDefName(String jobDefID);
  public String getJobDefDatasetID(String jobDefID);
  public String getJobDefTransformationID(String jobDefID);
  public String getTransformationScript(String jobDefID);
  
  // ####### File table
  public boolean isFileCatalog();
  public DBRecord getFile(String datasetName, String fileID);
  public String getFileID(String datasetName, String fileID);
  public String [] getFileURLs(String datasetName, String fileID);
  public String getFileDatasetID(String datasetName, String fileID);
  // For file catalogs only: register an lfn/pfn pair.
  // TODO: if fileID/lfn do not exist, create.
  // datasetComplete is ignored by other than ATLAS
  public void registerFileLocation(String datasetID, String datasetName,
      String fileID, String lfn, String url, boolean datasetComplete);
  public boolean deleteFiles(String datasetID, String [] fileIDs, boolean cleanup);
  
  // ####### Job execution
  public String getRunInfo(String jobDefID, String key);
  public boolean cleanRunInfo(String jobDefID);
  public boolean reserveJobDefinition(String jobDefID, String UserName, String cs);
  public String [] getOutputMapping(String jobDefID);
  public String [] getJobDefInputFiles(String jobDefID);
  public String getJobDefOutRemoteName(String jobDefID, String par);
  public String getJobDefOutLocalName(String jobDefID, String par);
  public String getStdOutFinalDest(String jobDefID);
  public String getStdErrFinalDest(String jobDefID);
  public String [] getTransformationArguments(String jobDefID);
  public String [] getJobDefTransPars(String jobDefID);
  
  // ####### Misc
  public boolean setJobDefsField(String [] identifiers, String field, String value);
  public String [] getFieldNames(String table) throws SQLException;
  // The last database error reported
  public String getError();
  
}