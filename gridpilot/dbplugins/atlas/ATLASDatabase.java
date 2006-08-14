package gridpilot.dbplugins.atlas;

import gridpilot.DBRecord;
import gridpilot.DBResult;
import gridpilot.Database;

public class ATLASDatabase implements Database {

  public ATLASDatabase(String _dbName,
      String _driver, String _database,
      String _user, String _passwd){
    // TODO
  }

    public String connect(){
    // TODO Auto-generated method stub
    return null;
  }

  public void disconnect(){
    // TODO Auto-generated method stub

  }

  public void clearCaches(){
    // TODO Auto-generated method stub

  }

  public DBResult select(String selectRequest, String identifier){
    // TODO Auto-generated method stub
    return null;
  }
  
  public String getDatasetName(int datasetID){
    // TODO Auto-generated method stub
    return null;
  }

  public int getDatasetID(String datasetName){
    // TODO Auto-generated method stub
    return 0;
  }

  public String getRunNumber(int datasetID){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean createDataset(String targetTable, String[] fields,
      Object[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateDataset(int datasetID, String[] fields, String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteDataset(int datasetID, boolean cleanup){
    // TODO Auto-generated method stub
    return false;
  }

  public DBRecord getDataset(int datasetID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getDatasetTransformationName(int datasetID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getDatasetTransformationVersion(int datasetID){
    // TODO Auto-generated method stub
    return null;
  }
  
  
  

  // -------------------------------------------------------------------
  // What's below here can wait
  // -------------------------------------------------------------------


  public String[] getRuntimeEnvironments(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public DBResult getRuntimeEnvironments(){
    // TODO Auto-generated method stub
    return null;
  }

  public int getRuntimeEnvironmentID(String name, String cs){
    // TODO Auto-generated method stub
    return 0;
  }

  public DBRecord getRuntimeEnvironment(int runtimeEnvironmentID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getRuntimeInitText(String pack, String cluster){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean createRuntimeEnvironment(Object[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateRuntimeEnvironment(int runtimeEnvironmentID,
      String[] fields, String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteRuntimeEnvironment(int runtimeEnvironmentID){
    // TODO Auto-generated method stub
    return false;
  }

  public DBResult getTransformations(){
    // TODO Auto-generated method stub
    return null;
  }

  public DBRecord getTransformation(int transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public int getTransformationID(String transName, String transVersion){
    // TODO Auto-generated method stub
    return 0;
  }

  public boolean createTransformation(Object[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateTransformation(int transformatinID, String[] fields,
      String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteTransformation(int transformationID){
    // TODO Auto-generated method stub
    return false;
  }

  public String[] getVersions(String transformationName){
    // TODO Auto-generated method stub
    return null;
  }

  public String getTransformationRuntimeEnvironment(int transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationJobParameters(int transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationOutputs(int transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationInputs(int transformationID){
    // TODO Auto-generated method stub
    return null;
  }

  public DBResult getJobDefinitions(int datasetID, String[] fieldNames){
    // TODO Auto-generated method stub
    return null;
  }

  public DBRecord getJobDefinition(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean createJobDefinition(String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean createJobDefinition(String datasetName, String[] cstAttrNames,
      String[] resCstAttr, String[] trpars, String[][] ofmap, String odest,
      String edest){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean deleteJobDefinition(int jobDefID){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateJobDefinition(int jobDefID, String[] fields,
      String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean updateJobDefinition(int jobDefID, String[] values){
    // TODO Auto-generated method stub
    return false;
  }

  public String getJobDefStatus(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefUserInfo(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefName(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public int getJobDefDatasetID(int jobDefID){
    // TODO Auto-generated method stub
    return 0;
  }

  public int getJobDefTransformationID(int jobDefID){
    // TODO Auto-generated method stub
    return 0;
  }

  public String getTransformationScript(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getRunInfo(int jobDefID, String key){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean cleanRunInfo(int jobDefID){
    // TODO Auto-generated method stub
    return false;
  }

  public boolean reserveJobDefinition(int jobDefID, String UserName, String cs){
    // TODO Auto-generated method stub
    return false;
  }

  public String[] getOutputMapping(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getJobDefInputFiles(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefOutRemoteName(int jobDefID, String par){
    // TODO Auto-generated method stub
    return null;
  }

  public String getJobDefOutLocalName(int jobDefID, String par){
    // TODO Auto-generated method stub
    return null;
  }

  public String getStdOutFinalDest(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String getStdErrFinalDest(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getTransformationArguments(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public String[] getJobDefTransPars(int jobDefID){
    // TODO Auto-generated method stub
    return null;
  }

  public boolean setJobDefsField(int[] identifiers, String field, String value){
    // TODO Auto-generated method stub
    return false;
  }

  public String[] getFieldNames(String table){
    // TODO Auto-generated method stub
    return null;
  }

  public String getError(){
    // TODO Auto-generated method stub
    return null;
  }

}
