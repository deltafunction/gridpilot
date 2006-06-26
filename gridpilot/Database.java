package gridpilot;

//import java.util.HashMap;

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
  public DBResult getRuntimeEnvironments();
  public DBRecord getRuntimeEnvironment(int runtimeEnvironmentID);
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
  public String [] getTransJobParameters(int transformationID);
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
  public String [] getTransOutputs(int transformationID);
  public String [] getTransInputs(int transformationID);

  // ####### Dataset table
  public DBResult getJobDefinitions(int datasetID, String [] fieldNames);
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
  public DBRecord getJobDefinition(int jobDefID);
  public boolean createJobDefinition(String [] values);
  public boolean createJobDefinition(String datasetName, String [] cstAttrNames,
      String [] resCstAttr, String [] trpars, String [] [] ofmap, String odest,
      String edest);
  public boolean deleteJobDefinition(int jobDefID);
  public boolean updateJobDefinition(int jobDefID, String [] fields, String [] values);
  // Here the following fields are assumed:
  // "jobDefID", "jobName", "stdOut", "stdErr"
  public boolean updateJobDefinition(int jobDefID, String [] values);
  //public boolean updateJobDefStatus(int jobDefID, String status);
  public String getJobStatus(int jobDefID);
  public String getJobDefUserInfo(int jobDefID);
  public String getJobDefName(int jobDefID);
  public int getJobDefDatasetID(int jobDefID);
  public int getJobDefTransformationID(int jobDefID);
  public String getExtractScript(int jobDefID);
  public String getValidationScript(int jobDefID);
  public String getTransformationScript(int jobDefID);
  
  // ####### Job execution
  public boolean reserveJobDefinition(int jobDefID, String UserName, String cs);
  public boolean cleanRunInfo(int jobDefID);
  public String [] getRuntimeEnvironments(int jobDefID);
  public int getRuntimeEnvironmentID(String name, String cs);
  public String [] getTransformationArguments(int jobDefID);
  public String [] getJobDefTransPars(int jobDefID);
  public String [] getOutputMapping(int jobDefID);
  public String [] getInputs(int jobDefID);
  public String getJobDefInRemoteName(int jobDefID, String par);
  public String getJobDefInLocalName(int jobDefID, String par);
  public String getJobDefOutRemoteName(int jobDefID, String par);
  public String getJobDefOutLocalName(int jobDefID, String par);
  public String getStdOutFinalDest(int jobDefID);
  public String getStdErrFinalDest(int jobDefID);
  public DBRecord getRunInfo(int jobDefID);
  
  // ####### Misc
  public boolean createRunInfo(JobInfo jobInfo);
  public boolean updateRunInfo(JobInfo jobInfo);
  public boolean setJobDefsField(int [] identifiers, String field, String value);
  public String getUserLabel();
  public boolean saveDefVals(int datasetID, String[] defvals, String user);
  public String [] getDefVals(int datasetID, String user);
  public String [] getFieldNames(String table);
  public String getPackInitText (String pack, String cluster);
  // The class providing the panel for job creation
  public String getJobDefCreationPanelClass();
  // The last database error reported
  public String getError();

  
  public class DBRecord{
    public String [] fields = null;
    public Object [] values = null;
    public static String identifier = null;
    public DBRecord(){
      fields = new String [] {""};
    }
    
    public DBRecord(String [] _fields, Object [] _values){
      fields = _fields;
      values = _values;
    }
    public Object getAt(int i){
      return values[i];  
    }
    
    public Object getValue(String col){
      for (int i = 0; i < fields.length; i++){
        if (col.equalsIgnoreCase(fields[i])){
          return values[i];
        }
      }
      //return "no such field "+col;
      return "";
    }
    
    public void setValue(String col, String val) throws Exception{
       for (int i=0; i<fields.length; i++){
        if (col.equalsIgnoreCase(fields[i])){
          values[i] = val;
          //Debug.debug("Set field "+fields[i]+" to value "+values[i],3);
          // TODO: Should set field to value. Seems not to work
          //DBRecord.class.getField(col).set(this,val);
          return;
        }
      }
      throw new Exception("no such field "+col);
    }
  }
  
  
  public static class DBResult{
  
    public String[]    fields;
    public Object[][]  values;
  
    public DBResult(int nrFields, int nrValues){
      fields = new String [nrFields];
      values = new String [nrValues][nrFields];
    }
  
    public DBResult(String[] _fields, String[][] _values) {
      fields = _fields;
      values = _values;
    }
  
    public DBResult(){
      String [] f = {};
      String [] [] v = {};
      fields = f;
      values = v;
    }
  
    public Object getAt(int row, int column){
      if (row>values.length-1){
        return "no such row";
      }
      if(column>values[0].length-1){
        return "no such column";
      }
      return values[row][column];
    }

    public Object getValue(int row, String col){
      if(row>values.length-1){
        return "no such row";
      }
      Debug.debug("fields: "+Util.arrayToString(fields), 3);
      for(int i=0; i<fields.length; i++){
        Debug.debug("checking value "+values[row][i], 3);
        if(col.equalsIgnoreCase(fields[i])){
          return values[row][i];
        }
      }
      return "no such field";
    }

    public DBRecord getRow(int row){
      DBRecord ret = new DBRecord();
      if(row>values.length-1){
        return ret;
      }
      ret.fields = this.fields;
      ret.values = this.values[row];
      return ret;
    }

    public boolean setValue(int row, String col, String value){
      if(row>values.length-1){
        return false;
      }
      for(int i=0; i<fields.length; i++){
        if(col.equalsIgnoreCase(fields[i])){
          values[row][i] = value;
          return true;
        }
      }
      return false;
    }
  }
}