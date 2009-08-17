package gridpilot;

import gridfactory.common.ConfirmBox;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;

import java.io.File;
import java.sql.SQLException;

import javax.swing.JOptionPane;

public class ExportImport {
  
  /**
   * Placeholder for the import directory, used in database records produced by
   * {@link #exportDB()}.
   */
  private static final String IMPORT_DIR = "GRIDPILOT_IMPORT_DIR";
  private static final String [] EXE_FILE_FIELDS = new String [] {"executableFile", "inputFiles"};
  private static final String EXE_FILES_DIR = "executableFiles";

  /**
   * Exports dataset and corresponding executable information from the chosen
   * database. If 'dbName' is null, a popup is displayed asking to choose a database.
   * If 'datasetId' is null, the whole "dataset" and "executable" tables are exported.
   * Input files for the tranformation(s) are bundled in the exported tarball.
   * @param exportDir
   * @param datasetId
   * @param _dbName
   * @throws Exception
   */
  public static void exportDB(String exportDir, String _dbName, String datasetId) throws Exception{
    String dbName = _dbName;
    String executableId = null;
    String datasetName = null;
    if(datasetId!=null){
      DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
      String exeName = mgr.getDatasetExecutableName(datasetId);
      String exeVersion = mgr.getDatasetExecutableVersion(datasetId);
      executableId = mgr.getExecutableID(exeName, exeVersion);
      datasetName = mgr.getDatasetName(datasetId);
    }
    String exportFileName;
    String [] choices;
    if(dbName==null){
      choices = new String[GridPilot.DB_NAMES.length+1];
      System.arraycopy(GridPilot.DB_NAMES, 0, choices, 0, GridPilot.DB_NAMES.length);
    }
    else{
      choices = new String[] {"Continue", "Cancel"};
    }
    choices[choices.length-1] = datasetId==null?"none (cancel)":"cancel";
    if(datasetName!=null){
      exportFileName = datasetName+".gpa";
    }
    else{
      exportFileName = "GridPilot_EXPORT_"+MyUtil.getDateInMilliSeconds()+".gpa";
    }
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String message = datasetId==null?
         "This will export all datasets and executables of the chosen database\n" +
              "plus any files associated with the executables. Non-local files will\n" +
              "be downloded first.\n\n" +
              "Choose database to export from or choose none to cancel.\n" :
         "This will export the dataset \n\n" +
               "\"" + datasetName + "\"\n\n" +
               "and its associated executable plus any files associated\n" +
               "with this executable. Non-local files will be downloded\n" +
               "first.\n\n" +
               "Notice: if the file \n\n" +exportFileName + "\n\n" +
               "already exists in the chosen directory, it will be overwritten.\n";
    int choice = confirmBox.getConfirm("Export from database", message, choices);
    if(choice<0 || choice>=choices.length-1){
      return;
    }
    if(dbName==null){
      dbName = GridPilot.DB_NAMES[choice];
    }
    // Work in a tmp dir
    File tmpDir = File.createTempFile(MyUtil.getTmpFilePrefix(), "");
    tmpDir.delete();
    tmpDir.mkdirs();
    GridPilot.getClassMgr().getLogFile().addInfo("Exporting from database "+dbName+
        " to "+tmpDir.getAbsolutePath());
    File tarFile = File.createTempFile(MyUtil.getTmpFilePrefix(), ".tar");
    // have the tmp directory and file deleted on exit
    GridPilot.addTmpFile(tmpDir.getAbsolutePath(), tmpDir);
    GridPilot.addTmpFile(tarFile.getAbsolutePath(), tarFile);
    // Save everything to the tmp dir
    saveTableAndFiles(tmpDir, dbName, "dataset", new String [] {},
        datasetId);
    saveTableAndFiles(tmpDir, dbName, "executable", EXE_FILE_FIELDS,
        executableId);
    // Tar up the tmp dir
    MyUtil.tar(tarFile, tmpDir);
    String gzipFile = tarFile.getAbsolutePath()+".gz";
    Debug.debug("Created temporary archive: "+gzipFile, 1);
    MyUtil.gzip(tarFile.getAbsolutePath(), gzipFile);
    // Clean up
    LocalStaticShell.deleteDir(tmpDir.getAbsolutePath());
    LocalStaticShell.deleteFile(tarFile.getAbsolutePath());
    if(MyUtil.urlIsRemote(exportDir)){
      GridPilot.getClassMgr().getTransferControl().upload(
          new File(tarFile.getAbsolutePath()+".gz"), exportDir+"/"+exportFileName);
      LocalStaticShell.deleteFile(tarFile.getAbsolutePath()+".gz");
    }
    else{
      LocalStaticShell.moveFile(tarFile.getAbsolutePath()+".gz",
          (new File(exportDir, exportFileName)).getAbsolutePath());
    }
  }

  /**
   * This will fill a tmp directory with an SQL file, 'table'.sql plus a subdirectory
   * 'table'Files, containing the files found in 'fileFields'.
   * The URLs found in 'fileFields' will be changed to 'exportImportDir'/'table'Files/[record name]/[file name].
   * <br><br>
   * E.g.
   * 
   * /tmp/GridPilot-12121/                                        <br>&nbsp;
   *                     executable.sql                      <br>&nbsp;
   *                     my_executable/                      <br>&nbsp;
   *                                         jobOptions.py       <br>&nbsp;
   *                                         input1.root         <br>&nbsp;
   *                  
   *      
   * 
   * @param dbDir
   * @param dbName
   * @param table
   * @param fileFields
   * @param sqlExtension
   * @param id the identifier of the row to be exported. If this is null, the whole table is exported.
   * @throws Exception
   */
  private static void saveTableAndFiles(File dbDir, String dbName, String table,
      String [] fileFields, String id) throws Exception{
    String idField = MyUtil.getIdentifierField(dbName, table);
    String query = "SELECT * FROM "+table;
    if(id!=null){
      Debug.debug("Exporting row "+id, 2);
      query += " WHERE "+idField+" = "+id;
    }
    DBResult dbResult =
      GridPilot.getClassMgr().getDBPluginMgr(dbName).select(query, idField, true);
    File dir = new File(dbDir, table+"Files");
    dir.mkdir();
    saveTableFiles(dbResult, dir, dbName, table, fileFields);
    String sql = toSql(dbResult, dbName, table, fileFields);
    File tableFile = new File(dbDir, table+".sql");
    LocalStaticShell.writeFile(tableFile.getAbsolutePath(), sql, true);
  }
  
  private static void saveTableFiles(DBResult dbResult, File dir, String dbName, String table,
      String [] fileFields) throws Exception {
    String urlsStr;
    String [] urls;
    String nameField = MyUtil.getNameField(dbName, table);
    String name;
    File dlDir;
    StringBuffer failedDLs = new StringBuffer();
    while(dbResult.moveCursor()){
      for(int i=0; i<fileFields.length; ++i){
        if(MyUtil.arrayContainsIgnoreCase(dbResult.fields, fileFields[i])){
          name = dbResult.getString(nameField);
          urlsStr = dbResult.getString(fileFields[i]);
          if(urlsStr==null){
            continue;
          }
          urls = MyUtil.split(urlsStr);
          for(int j=0; j<urls.length; ++j){
            // Download url to 'dir'/[record name]
            dlDir = new File(dir, name);
            dlDir.mkdir();
            try{
              Debug.debug("Downloading file "+urls[j]+" to "+dlDir, 2);
              GridPilot.getClassMgr().getTransferControl().download(urls[j], dlDir);
            }
            catch(Exception e){
              failedDLs.append(" "+urls[j]);
            }
          }
        }
      }
      if(failedDLs.length()>0){
        GridPilot.getClassMgr().getLogFile().addMessage("WARNING: the following file(s) could not be downloaded:"+
            failedDLs + ". The export may not be complete.");
      }
    }
  }

  private static String toSql(DBResult dbResult, String dbName, String table, String [] fileFields){
    Debug.debug("Converting DBResult with "+dbResult.values.length+" rows", 2);
    String nameField = MyUtil.getNameField(dbName, table);
    StringBuffer res = new StringBuffer();
    StringBuffer insFields = new StringBuffer();
    insFields.append("INSERT INTO "+table+" (");
    StringBuffer insValues = new StringBuffer();
    insValues.append(") VALUES (");
    Object [][] newValues = dbResult.values.clone();
    String name;
    boolean oneInserted = false;
    for(int i=0; i<dbResult.values.length; ++i){
      name = (String) dbResult.getValue(i, nameField);
      for(int j=0; j<dbResult.fields.length; ++j){
        if(MyUtil.arrayContainsIgnoreCase(fileFields, dbResult.fields[j]) &&
            newValues[i][j]!=null){
          // Replace URLs with file names in exportImportDir
          newValues[i][j] = ((String) newValues[i][j]).trim().replaceAll("\\\\", "/"
              ).replaceAll("^.*/([^/]+)$", IMPORT_DIR+"/"+name+"/$1");
        }
      }
      oneInserted = false;
      for(int j=0; j<dbResult.fields.length; ++j){
        if(newValues[i][j]!=null && !newValues[i][j].equals("")){
          insFields.append((oneInserted?", ":"")+dbResult.fields[j]);
          insValues.append((oneInserted?", ":"")+"'"+newValues[i][j]+"'");
          oneInserted = true;
        }
      }
      res.append(insFields.toString()+insValues.toString()+");\n");
    }
    return res.toString();
  }

  /**
   * Returns import report text.
   */
  public static String importToDB(String importFile) throws Exception{
    String executableDir = GridPilot.getClassMgr().getConfigFile().getValue(
        "Fork", "executable directory");
    File executableDirectory = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(
        executableDir)));
    String [] choices = new String[GridPilot.DB_NAMES.length+1];
    System.arraycopy(GridPilot.DB_NAMES, 0, choices, 0, GridPilot.DB_NAMES.length);
    choices[choices.length-1] = "none (cancel)";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    int choice = confirmBox.getConfirm("Import in database",
  "<html>This will import datasets and executables in the chosen database.<br>" +
        "Any files associated with the executables will be copied to<br>" +
        executableDirectory + "/.<br><br>" +
        "Choose database to use.<br></html>", choices);
    if(choice<0 || choice>=choices.length-1){
      return null;
    }
    String ret = "Successfully imported:\n\n";
    String dbName = GridPilot.DB_NAMES[choice];
    DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    // Download the import file, unpack it in a tmp dir
    File tmpDir = downloadAndUnpack(importFile);
    // have the tmp dir deleted on exit
    GridPilot.addTmpFile(tmpDir.getAbsolutePath(), tmpDir);
    // Insert the SQL
    String sqlFile = (new File(tmpDir, "executable.sql")).getAbsolutePath();
    String sql = LocalStaticShell.readFile(sqlFile);
    mgr.executeUpdate(sql);
    Debug.debug("mgr reports: "+mgr.getError(), 3);
    if(mgr.getError()!=null && !mgr.getError().equals("")){
      throw new SQLException(mgr.getError());
    }
    ret += (sql!=null&&!sql.equals("")?
        " - "+((sql.length()-sql.toLowerCase().replaceAll("(?i)(?s)insert ", "").length())/7)+
           " executable record(s)\n":
        "");
    sqlFile = (new File(tmpDir, "dataset.sql")).getAbsolutePath();
    sql = LocalStaticShell.readFile(sqlFile);
    mgr.executeUpdate(sql);
    if(mgr.getError()!=null && !mgr.getError().equals("")){
      throw new SQLException(mgr.getError());
    }
    ret += (sql!=null&&!sql.equals("")?
        " - "+((sql.length()-sql.toLowerCase().replaceAll("(?i)(?s)insert ", "").length())/7)+
           " application record(s)\n":
        "");
    // Move executable input files to executable dir
    int numFiles = moveTransInputs((new File(tmpDir, EXE_FILES_DIR)).getAbsolutePath(), executableDirectory);
    // Read back any rows containing IMPORT_DIR and modify IMPORT_DIR to the GridPilot directory
    fixImportedFileLocations(dbName, executableDirectory);
    // Clean up
    tmpDir.delete();
    ret += "\ninto database "+dbName+"";
    ret +=(numFiles>0? "\n\nand\n\n - "+numFiles+ " application file(s)\n\ninto directory "+executableDirectory+".":
        ".");
    return ret;
   }
  
  private static File downloadAndUnpack(String importFile) throws Exception{
    // Work in a tmp dir
    File tmpDir = File.createTempFile(MyUtil.getTmpFilePrefix(), "");
    tmpDir.delete();
    tmpDir.mkdirs();
    GridPilot.getClassMgr().getLogFile().addInfo("Importing in directory "+tmpDir.getAbsolutePath());
    // have the tmp directory and file deleted on exit
    GridPilot.addTmpFile(tmpDir.getAbsolutePath(), tmpDir);
    // Download to the tmp dir
    GridPilot.getClassMgr().getTransferControl().download(importFile, tmpDir);
    // Give the file system a few seconds...
    Thread.sleep(10000);
    // Unpack
    String gzipFileName = importFile.replaceAll("\\\\", "/").replaceFirst("^.*/([^/]+)$", "$1");
    String tarFileName = gzipFileName.replaceAll("\\\\", "/").replaceFirst("\\.gpa$", "")+".tar";
    MyUtil.gunzip(new File(tmpDir, gzipFileName), new File(tmpDir, tarFileName));
    MyUtil.unTar(new File(tmpDir, tarFileName), tmpDir);
    return tmpDir;
  }

  private static void fixImportedFileLocations(String dbName, File dir) {
    DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    String sql = "SELECT * FROM executable WHERE ";
    for(int i=0; i<EXE_FILE_FIELDS.length; ++i){
      sql += (i>0?" OR ":"")+EXE_FILE_FIELDS[i]+" CONTAINS "+IMPORT_DIR+"/";
    }
    String idField = MyUtil.getIdentifierField(dbName, "executable");
    DBResult dbResult = mgr.select(sql, idField, true);
    String id;
    String [] urls = null;
    String urlsStr;
    String [] newUrlsStrs = new String [EXE_FILE_FIELDS.length];
    int recNr = 0;
    while(dbResult.moveCursor()){
      id = dbResult.getString(idField);
      for(int i=0; i<EXE_FILE_FIELDS.length; ++i){
        urlsStr = dbResult.getString(EXE_FILE_FIELDS[i]);
        newUrlsStrs[i] = "";
        if(urlsStr!=null){
          urls = MyUtil.split(urlsStr);
          for(int j=0; j<urls.length; ++j){
            // replace IMPORT_DIR/ with 'dir'/
            urls[j] = urls[j].replaceFirst("^"+IMPORT_DIR, "file:"+MyUtil.replaceWithTildeLocally(
                dir.getAbsolutePath()));
          }
          newUrlsStrs[i] = MyUtil.arrayToString(urls);
        }
      }
      if(newUrlsStrs!=null && newUrlsStrs.length>0){
        // write back the record
        mgr.updateExecutable(id, EXE_FILE_FIELDS, newUrlsStrs);
      }
      ++recNr;
    }
  }

  private static int moveTransInputs(String tmpDir, File executableDirectory) {
    String [] files = LocalStaticShell.listFiles(tmpDir);
    if(files==null){
      return 0;
    }
    String fileName;
    int ret = files.length;
    for(int i=0; i<ret; ++i){
      if(!files[i].endsWith(".sql")){
        fileName = (new File(files[i])).getName();
        LocalStaticShell.moveFile(files[i],
            (new File(executableDirectory, fileName)).getAbsolutePath());
      }
    }
    return ret;
  }

}
