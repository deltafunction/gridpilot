package gridpilot;

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
  private String executableDirectory = null;
  
  private static String myDatasetName = "my_dataset";
  private static String myExecutableName = "my_executable";
  private static String myExecutableVersion = "0.1";
  private static String testExecutableName = "test";
  private static String testExecutableVersion = "0.1";

  public TestDatasets() {
    logFile = GridPilot.getClassMgr().getLogFile();
    executableDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        "Fork", "Executable directory");
    if(executableDirectory==null){
      executableDirectory = "file:~/GridPilot/executables/";
    }
    if(!executableDirectory.endsWith("/")){
      executableDirectory = executableDirectory+"/";
    }
  }

  /**
   * Create test executables and datasets if they don't exist.
   */
  protected void createAll(){
    try{
      if(!GridPilot.IS_FIRST_RUN){
        GridPilot.splashShow("Checking test executables");
      }
    }
    catch(Exception e){
      // if we cannot show text on splash, just silently ignore
    }
    Shell shellMgr = null;
    DBPluginMgr dbPluginMgr = null;
    for(int i=0; i<GridPilot.CS_NAMES.length; ++i){
      if(!MyUtil.checkCSEnabled(GridPilot.CS_NAMES[i])){
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
      Debug.debug("createAll --> "+shellMgr+" : "+executableDirectory, 3);
      // Make sure executable directory exists
      if(!shellMgr.existsFile(executableDirectory)){
        try{
          shellMgr.mkdirs(executableDirectory);
        }
        catch(Exception e){
          e.printStackTrace();
        }
      }
      // Make sure scripts exist
      createMyExecutableScript(shellMgr);
      //createTestExecutableScript(shellMgr);
    }
    try{
      if(dbPluginMgr==null){
        dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr("My_DB_Local");
      }
      if(dbPluginMgr==null){
        logFile.addMessage("WARNING: could not get local DBPluginMgr My_DB_Local. Test executables and dataset not created.");
      }
      // Make sure executables exist in local database
      createMyExecutable(dbPluginMgr);
      //createTestExecutable(dbPluginMgr);
      // Make sure dataset exists
      createMyDataset(dbPluginMgr);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }

  /**
   * If the executable script does not exist, create it.
   */
  protected void createMyExecutableScript(Shell shell){
    String executableScriptName = myExecutableName+".sh";
    try{
      if(!shell.existsFile(executableDirectory+executableScriptName)){
        shell.writeFile(executableDirectory+executableScriptName,
            "#!/bin/bash\n#\n# Sample executable.\n# Write any commands below.\n#", false);
      }
    }
    catch(Exception e){
      logFile.addMessage("WARNING: could not create executable script "+
          executableDirectory+executableScriptName);
    }
  }

  /**
   * If the executable script does not exist, create it.
   */
  protected void createTestExecutableScript(Shell shellMgr){
    String testScriptName = testExecutableName+".sh";
    // Create two dummy input files
    if(!shellMgr.existsFile(executableDirectory+"data1.txt")){
      try{
        shellMgr.writeFile(executableDirectory+"data1.txt", "test data", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(!shellMgr.existsFile(executableDirectory+"data2.txt")){
      try{
        shellMgr.writeFile(executableDirectory+"data2.txt", "test data", false);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    StringBuffer fileStr = new StringBuffer("");
    if(!shellMgr.existsFile(executableDirectory+testScriptName)){
      BufferedReader in = null;
      try{
        URL fileURL = GridPilot.class.getResource(GridPilot.RESOURCES_PATH+testScriptName);
        in = new BufferedReader(new InputStreamReader(fileURL.openStream()));
        String line = null;
        while((line=in.readLine())!=null){
          fileStr.append(line+"\n");
        }
        in.close();
        shellMgr.writeFile(executableDirectory+testScriptName, fileStr.toString(), false);
      }
      catch(IOException e){
        logFile.addMessage("WARNING: Could not write test executable", e);
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
   * If the executable does not exist, create it.
   */
  protected void createMyExecutable(DBPluginMgr dbPluginMgr) throws Exception{
    String executableScriptName = myExecutableName+".sh";
    String id = dbPluginMgr.getExecutableID(myExecutableName, myExecutableVersion);
    if(id==null || id.equals("") || id.equals("-1")){
      String [] fields = new String [] {
          /*identifier cannot be null*/MyUtil.getIdentifierField("My_DB_Local", "executable"),
          /*name*/MyUtil.getNameField("My_DB_Local", "executable"),
          /*version*/MyUtil.getDatasetExecutableVersionReference("My_DB_Local")[0],
          /*runtimeenvironmentname*/MyUtil.getExecutableRuntimeReference("My_DB_Local")[1],
          "executableFile"};
      String [] values = new String [] {
          "",
          myExecutableName,
          myExecutableVersion,
          "Linux",
          "file:"+executableDirectory+executableScriptName};
      dbPluginMgr.createExecutable(fields, values);
    }
  }

  /**
   * If the executable does not exist, create it.
   */
  protected void createTestExecutable(DBPluginMgr dbPluginMgr){
    String testScriptName = testExecutableName+".sh";
    try{
      if(dbPluginMgr.getExecutableID(testExecutableName, testExecutableVersion)==null ||
          dbPluginMgr.getExecutableID(testExecutableName, testExecutableVersion).equals("-1")){
        String [] fields = dbPluginMgr.getFieldNames("executable");
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
            values[i] = "file:"+executableDirectory+"data1.txt "+
            "file:"+executableDirectory+"data2.txt";
          }
          else if(fields[i].equalsIgnoreCase("outputFiles")){
            values[i] = "out.txt";
          }
          else if(fields[i].equalsIgnoreCase("script")){
            values[i] = "file:"+executableDirectory+testScriptName;
          }
          else if(fields[i].equalsIgnoreCase("comment")){
            values[i] = "Executable script to test running local GridPilot jobs on Linux.";
          }
          else{
            values[i] = "";
          }
        }
        dbPluginMgr.createExecutable(values);
      }
    }
    catch(Exception e){
      logFile.addMessage("WARNING: Could not create test executable in DB "+dbPluginMgr.getDBName(),
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
          /*executablename*/MyUtil.getDatasetExecutableReference("My_DB_Local")[1],
          /*executableversion*/MyUtil.getDatasetExecutableVersionReference("My_DB_Local")[1],
          "totalFiles",
          "outputLocation"};
      String [] values = new String [] {
          "",
          myDatasetName,
          myExecutableName,
          myExecutableVersion,
          "1",
          GridPilot.GRID_HOME_URL};
      dbPluginMgr.createDataset(null, fields, values);
    }
  }

}
