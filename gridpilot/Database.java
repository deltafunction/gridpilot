package gridpilot;

public interface Database{
    
  public static final int DEFINED = 1;
  public static final int SUBMITTED = 2;
  public static final int VALIDATED = 3;
  public static final int UNDECIDED = 4;
  public static final int FAILED = 5;
  public static final int ABORTED = 6;
  public static final int UNEXPECTED = 7;
  
  public String connect();
  public void disconnect();
  // TODO: implement in plugins and make menu point active.
  public void clearCaches();
  
  // This is the parser of the select request from SelectPanel.
  // The last returned column must be the identifier.
  public DBResult select(String selectRequest, String identifier);
  
  // ####### RuntimeEnvironment table
  public String [] getRuntimeEnvironments(int jobDefID);
  public DBResult getRuntimeEnvironments();
  public int getRuntimeEnvironmentID(String name, String cs);
  public DBRecord getRuntimeEnvironment(int runtimeEnvironmentID);
  public String getRuntimeInitText(String pack, String cluster);
  public boolean createRuntimeEnvironment(Object [] values);
  public boolean updateRuntimeEnvironment(int runtimeEnvironmentID, String [] fields, String [] values);
  public boolean deleteRuntimeEnvironment(int runtimeEnvironmentID);

  // ####### Transformation table
  public DBResult getTransformations();
  public DBRecord getTransformation(int transformationID);
  public int getTransformationID(String transName, String transVersion);
  public boolean createTransformation(Object [] values);
  public boolean updateTransformation(int transformatinID, String [] fields, String [] values);
  public boolean deleteTransformation(int transformationID);
  public String [] getVersions(String transformationName);
  public String getTransformationRuntimeEnvironment(int transformationID);
  public String [] getTransformationJobParameters(int transformationID);
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
  public String [] getTransformationOutputs(int transformationID);
  public String [] getTransformationInputs(int transformationID);

  // ####### Dataset table
  public String getDatasetName(int datasetID);
  public int getDatasetID(String datasetName);
  public String getRunNumber(int datasetID);
  public boolean createDataset(String targetTable, String[] fields, Object [] values);
  public boolean updateDataset(int datasetID, String [] fields, String [] values);
  public boolean deleteDataset(int datasetID, boolean cleanup);
  public DBRecord getDataset(int datasetID);
  public String getDatasetTransformationName(int datasetID);
  public String getDatasetTransformationVersion(int datasetID);

  // ####### Job definition table
  // the convention here is that when datasetID is set to -1,
  // all jobDefinitions are returned
  public DBResult getJobDefinitions(int datasetID, String [] fieldNames);
  public DBRecord getJobDefinition(int jobDefID);
  public boolean createJobDefinition(String [] values);
  public boolean createJobDefinition(String datasetName, String [] cstAttrNames,
      String [] resCstAttr, String [] trpars, String [] [] ofmap, String odest,
      String edest);
  public boolean deleteJobDefinition(int jobDefID);
  public boolean updateJobDefinition(int jobDefID, String [] fields, String [] values);
  // Here the following fields are assumed:
  // "user", "jobDefID", "jobName", "stdOut", "stdErr"
  public boolean updateJobDefinition(int jobDefID, String [] values);
  public String getJobDefStatus(int jobDefID);
  public String getJobDefUserInfo(int jobDefID);
  public String getJobDefName(int jobDefID);
  public int getJobDefDatasetID(int jobDefID);
  public int getJobDefTransformationID(int jobDefID);
  public String getTransformationScript(int jobDefID);
  
  // ####### Job execution
  public String getRunInfo(int jobDefID, String key);
  public boolean cleanRunInfo(int jobDefID);
  public boolean reserveJobDefinition(int jobDefID, String UserName, String cs);
  public String [] getOutputMapping(int jobDefID);
  public String [] getJobDefInputFiles(int jobDefID);
  public String getJobDefOutRemoteName(int jobDefID, String par);
  public String getJobDefOutLocalName(int jobDefID, String par);
  public String getStdOutFinalDest(int jobDefID);
  public String getStdErrFinalDest(int jobDefID);
  public String [] getTransformationArguments(int jobDefID);
  public String [] getJobDefTransPars(int jobDefID);
  
  // ####### Misc
  public boolean setJobDefsField(int [] identifiers, String field, String value);
  public String [] getFieldNames(String table);
  // The last database error reported
  public String getError();
  
}