package gridpilot;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.LocalShell;
import gridfactory.common.LogFile;
import gridfactory.common.Shell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class TestDatasets {
  
  private LogFile logFile = null;
  private ConfigFile configFile = null;
  private String transformationDirectory = null;
  
  private static String myDatasetName = "my_dataset";
  private static String myTransformationName = "my_transformation";
  private static String myTransformationVersion = "0.1";
  private static String testTransformationName = "test";
  private static String testTransformationVersion = "0.1";

  public TestDatasets() {
    logFile = GridPilot.getClassMgr().getLogFile();
    configFile = GridPilot.getClassMgr().getConfigFile();
    transformationDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        "Fork", "transformation directory");
    if(transformationDirectory==null){
      transformationDirectory = "file:~/GridPilot/transformations/";
    }
    if(!transformationDirectory.endsWith("/")){
      transformationDirectory = transformationDirectory+"/";
    }
  }

  /**
   * Create test transformations and datasets if they don't exist.
   */
  protected void createAll(){
    try{
      if(!GridPilot.IS_FIRST_RUN){
        GridPilot.splashShow("Checking test transformations");
      }
    }
    catch(Exception e){
      // if we cannot show text on splash, just silently ignore
    }
    String isEnabled = "no";
    Shell shellMgr = null;
    DBPluginMgr dbPluginMgr = null;
    for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
      isEnabled = configFile.getValue(GridPilot.CS_NAMES[i], "Enabled");
      if(isEnabled==null || !isEnabled.equalsIgnoreCase("yes") && !isEnabled.equalsIgnoreCase("true")){
        continue;
      }
      try{
        shellMgr = GridPilot.getClassMgr().getShell(GridPilot.CS_NAMES[i]);
      }
      catch(Exception e){
        shellMgr = null;
      }
      if(shellMgr==null){
        shellMgr = new LocalShell();
      }
      Debug.debug("createAll --> "+shellMgr+" : "+transformationDirectory, 3);
      // Make sure transformation directory exists
      if(!shellMgr.existsFile(transformationDirectory)){
        try{
          shellMgr.mkdirs(transformationDirectory);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
      // Make sure scripts exist
      createMyTransformationScript(shellMgr);
      createTestTransformationScript(shellMgr);
    }
    try{
      if(dbPluginMgr==null){
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr("My_DB_Local");
      }
      if(dbPluginMgr==null){
        logFile.addMessage("WARNING: could not get local DBPluginMgr My_DB_Local. Test transformations and dataset not created.");
      }
      // Make sure transformations exist in local database
      createMyTransformation(dbPluginMgr);
      createTestTransformation(dbPluginMgr);
      // Make sure dataset exists
      createMyDataset(dbPluginMgr);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
   * If the transformation script does not exist, create it.
   */
  protected void createMyTransformationScript(Shell shellMgr){
    String transformationScriptName = myTransformationName+".sh";
    try{
      if(!shellMgr.existsFile(transformationDirectory+transformationScriptName)){
        shellMgr.writeFile(transformationDirectory+transformationScriptName,
            "#!/bin/bash\n#\n# Sample transformation.\n# Write any commands below.\n#", false);
      }
    }
    catch(Exception e){
      logFile.addMessage("WARNING: could not create transformation script "+
          transformationDirectory+transformationScriptName);
    }
  }

  /**
   * If the transformation script does not exist, create it.
   */
  protected void createTestTransformationScript(Shell shellMgr){
    String testScriptName = testTransformationName+".sh";
    // Create two dummy input files
    if(!shellMgr.existsFile(transformationDirectory+"data1.txt")){
      try{
        shellMgr.writeFile(transformationDirectory+"data1.txt", "test data", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(!shellMgr.existsFile(transformationDirectory+"data2.txt")){
      try{
        shellMgr.writeFile(transformationDirectory+"data2.txt", "test data", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    StringBuffer fileStr = new StringBuffer("");
    if(!shellMgr.existsFile(transformationDirectory+testScriptName)){
      BufferedReader in = null;
      try{
        URL fileURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH+testScriptName);
        in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
        String line = null;
        while((line=in.readLine())!=null){
          fileStr.append(line+"\n");
        }
        in.close();
        shellMgr.writeFile(transformationDirectory+testScriptName, fileStr.toString(), false);
      }
      catch(IOException e){
        logFile.addMessage("WARNING: Could not write test transformation", e);
        return;
      }
      finally{
        try{
          in.close();
        }
        catch(Exception ee){
        }
      }
    }
  }
  
  /**
   * If the transformation does not exist, create it.
   */
  protected void createMyTransformation(DBPluginMgr dbPluginMgr) throws Exception{
    String transformationScriptName = myTransformationName+".sh";
    String id = dbPluginMgr.getTransformationID(myTransformationName, myTransformationVersion);
    if(id==null || id.equals("") || id.equals("-1")){
      String [] fields = new String [] {
          /*identifier cannot be null*/MyUtil.getIdentifierField("My_DB_Local", "transformation"),
          /*name*/MyUtil.getNameField("My_DB_Local", "transformation"),
          /*version*/MyUtil.getDatasetTransformationVersionReference("My_DB_Local")[0],
          /*runtimeenvironmentname*/MyUtil.getTransformationRuntimeReference("My_DB_Local")[1],
          "script"};
      String [] values = new String [] {
          "",
          myTransformationName,
          myTransformationVersion,
          "Linux",
          "file:"+transformationDirectory+transformationScriptName};
      dbPluginMgr.createTrans(fields, values);
    }
  }

  /**
   * If the transformation does not exist, create it.
   */
  protected void createTestTransformation(DBPluginMgr dbPluginMgr){
    String testScriptName = testTransformationName+".sh";
    try{
      if(dbPluginMgr.getTransformationID(testTransformationName, testTransformationVersion)==null ||
          dbPluginMgr.getTransformationID(testTransformationName, testTransformationVersion).equals("-1")){
        String [] fields = dbPluginMgr.getFieldNames("transformation");
        String [] values = new String [fields.length];
        for(int i=0; i<fields.length; ++i){
          if(fields[i].equalsIgnoreCase("name")){
            values[i] = "test";
          }
          else if(fields[i].equalsIgnoreCase("version")){
            values[i] = "0.1";
          }
          else if(fields[i].equalsIgnoreCase("runtimeEnvironmentName")){
            values[i] = "Linux";
          }
          else if(fields[i].equalsIgnoreCase("arguments")){
            values[i] = "multiplier inputFileURLs";
          }
          else if(fields[i].equalsIgnoreCase("inputFiles")){
            values[i] = "file:"+transformationDirectory+"data1.txt "+
            "file:"+transformationDirectory+"data2.txt";
          }
          else if(fields[i].equalsIgnoreCase("outputFiles")){
            values[i] = "out.txt";
          }
          else if(fields[i].equalsIgnoreCase("script")){
            values[i] = "file:"+transformationDirectory+testScriptName;
          }
          else if(fields[i].equalsIgnoreCase("comment")){
            values[i] = "Transformation script to test running local GridPilot jobs on Linux.";
          }
          else{
            values[i] = "";
          }
        }
        dbPluginMgr.createTransformation(values);
      }
    }
    catch(Exception e){
      logFile.addMessage("WARNING: Could not create test transformation in DB "+dbPluginMgr.getDBName(),
          e);
    }
  }
  
  /**
   * If the dataset does not exist, create it.
   */
  public static void createMyDataset(DBPluginMgr dbPluginMgr){
    Debug.debug("Creating my_dataset with output location "+GridPilot.GRID_HOME_URL, 1);
    String id = dbPluginMgr.getDatasetID(myDatasetName);
    if(id==null || id.equals("") || id.equals("-1")){
      String [] fields = new String [] {
          /*identifier cannot be null*/MyUtil.getIdentifierField("My_DB_Local", "dataset"),
          /*name*/MyUtil.getNameField("My_DB_Local", "dataset"),
          /*transformationname*/MyUtil.getDatasetTransformationReference("My_DB_Local")[1],
          /*transformationversion*/MyUtil.getDatasetTransformationVersionReference("My_DB_Local")[1],
          "totalFiles",
          "outputLocation"};
      String [] values = new String [] {
          "",
          myDatasetName,
          myTransformationName,
          myTransformationVersion,
          "1",
          GridPilot.GRID_HOME_URL};
      dbPluginMgr.createDataset(null, fields, values);
    }
  }

}
