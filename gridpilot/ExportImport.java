package gridpilot;

import gridfactory.common.ConfirmBox;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;

import java.io.File;

import javax.swing.JOptionPane;

public class ExportImport {
  
  /**
   * Placeholder for the import directory, used in database records produced by
   * {@link #exportDB()}.
   */
  private static final String IMPORT_DIR = "GRIDPILOT_IMPORT_DIR";
  private static final String [] TRANSFORMATIOM_FILE_FIELDS = new String [] {"script", "inputFiles"};

  /**
   * Exports dataset and corresponding transformation information from the chosen
   * database. If 'dbName' is null, a popup is displayed asking to choose a database.
   * If 'datasetId' is null, the whole "dataset" and "transformation" tables are exported.
   * Input files for the tranformation(s) are bundled in the exported tarball.
   * @param exportDir
   * @param datasetId
   * @param _dbName
   * @throws Exception
   */
  public static void exportDB(String exportDir, String _dbName, String datasetId) throws Exception{
    String dbName = _dbName;
    if(dbName==null){
      String [] choices = new String[GridPilot.dbNames.length+1];
      System.arraycopy(GridPilot.dbNames, 0, choices, 0, GridPilot.dbNames.length);
      choices[choices.length-1] = "none (cancel)";
      ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
      int choice = confirmBox.getConfirm("Export database",
    "<html>This will export all datasets and transformations of the chosen database<br>" +
          "plus any files associated with the transformations. Non-local files will<br>" +
          "be downloded first.<br><br>" +
          "Please choose a database to export.</html>", choices);
      if(choice<0 || choice>=choices.length-1){
        return;
      }
      dbName = GridPilot.dbNames[choice];
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
    String transformationId = null;
    String datasetName = null;
    if(datasetId!=null){
      DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
      String transName = mgr.getDatasetTransformationName(datasetId);
      String transVersion = mgr.getDatasetTransformationVersion(datasetId);
      transformationId = mgr.getTransformationID(transName, transVersion);
      datasetName = mgr.getDatasetName(datasetId);
    }
    saveTableAndFiles(tmpDir, dbName, "transformation" ,TRANSFORMATIOM_FILE_FIELDS,
        transformationId);
    // Tar up the tmp dir
    MyUtil.tar(tarFile, tmpDir);
    String gzipFile = tarFile.getAbsolutePath()+".gz";
    Debug.debug("Created temporary archive: "+gzipFile, 1);
    MyUtil.gzip(tarFile.getAbsolutePath(), gzipFile);
    // Clean up
    LocalStaticShell.deleteDir(tmpDir.getAbsolutePath());
    LocalStaticShell.deleteFile(tarFile.getAbsolutePath());
    String exportFileName;
    if(datasetName!=null){
      exportFileName = datasetName+".tar.gz";
    }
    else{
      exportFileName = "GridPilot_EXPORT_"+MyUtil.getDateInMilliSeconds()+".tar.gz";
    }
    LocalStaticShell.moveFile(tarFile.getAbsolutePath()+".gz",
        (new File(exportDir, exportFileName)).getAbsolutePath());
  }

  /**
   * This will fill a tmp directory with an SQL file, 'table'.sql plus a subdirectory
   * 'table'Files, containing the files found in 'fileFields'.
   * The URLs found in 'fileFields' will be changed to 'exportImportDir'/'table'Files/[record name]/[file name].
   * <br><br>
   * E.g.
   * 
   * /tmp/GridPilot-12121/                                        <br>&nbsp;
   *                     transformation.sql                      <br>&nbsp;
   *                     transformationFiles/                    <br>&nbsp;
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
    String sql = toSql(dbResult, table, fileFields);
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
    while(dbResult.next()){
      for(int i=0; i<fileFields.length; ++i){
        if(MyUtil.arrayContainsIgnoreCase(fileFields, dbResult.fields[i]) &&
            dbResult.values[i]!=null){
          name = dbResult.getString(nameField);
          urlsStr = dbResult.getString(fileFields[i]);
          urls = MyUtil.split(urlsStr);
          for(int j=0; j<urls.length; ++j){
            // Download url to 'dir'/[record name]
            dlDir = new File(dir, name);
            dlDir.mkdir();
            try{
              GridPilot.getClassMgr().getTransferControl().download(urls[i], dlDir);
            }
            catch(Exception e){
              failedDLs.append(" "+urls[i]);
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

  private static String toSql(DBResult dbResult, String table, String [] fileFields){
    Debug.debug("Converting DBResult with "+dbResult.values.length+" rows", 2);
    String res = "";
    Object [][] newValues = dbResult.values.clone();
    for(int i=0; i<dbResult.values.length; ++i){
      for(int j=0; j<fileFields.length; ++j){
        if(MyUtil.arrayContainsIgnoreCase(fileFields, dbResult.fields[j]) &&
            newValues[i][j]!=null){
          // Replace URLs with file names in exportImportDir
          newValues[i][j] = ((String) newValues[i][j]).trim().replaceAll("\\\\", "/"
              ).replaceAll("^.*/([^/]+)$", IMPORT_DIR+"/"+table+"/$1");
        }
      }
      res += "INSERT INTO "+table+" ("+MyUtil.arrayToString(dbResult.fields, ", ")+
      ") VALUES ("+MyUtil.arrayToString(newValues[i], ", ")+");\n";
    }
    return res;
  }

  public static void importToDB(String importFile) throws Exception{
    String [] choices = new String[GridPilot.dbNames.length+1];
    System.arraycopy(GridPilot.dbNames, 0, choices, 0, GridPilot.dbNames.length);
    choices[choices.length-1] = "none (cancel)";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    int choice = confirmBox.getConfirm("Export database",
  "<html>This will export all datasets and transformations of the chosen database<br>" +
        "plus any files associated with the transformations. Non-local files will<br>" +
        "be downloded first.<br><br>" +
        "Please choose a database to export.</html>", choices);
    if(choice<0 || choice>=choices.length-1){
      return;
    }
    String dbName = GridPilot.dbNames[choice];
    DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    File tmpDir = downloadAndUnpack(importFile);
    // Insert the SQL
    String sqlFile = (new File(tmpDir, "transformation")).getAbsolutePath();
    String sql = LocalStaticShell.readFile(sqlFile);
    mgr.executeUpdate(sql);
    sqlFile = (new File(tmpDir, "dataset")).getAbsolutePath();
    sql = LocalStaticShell.readFile(sqlFile);
    mgr.executeUpdate(sql);
    // Now read back any rows containing IMPORT_DIR and modify IMPORT_DIR to the GridPilot directory
    sql = "SELECT * FROM transformation WHERE ";
    for(int i=0; i<TRANSFORMATIOM_FILE_FIELDS.length; ++i){
      sql += TRANSFORMATIOM_FILE_FIELDS[i]+" LIKE "+IMPORT_DIR+"/%";
    }
    DBResult dbResult = mgr.select(selectQuery, identifier, findAll);
      fixImportedFileLocations();
   }
  
  private static void fixImportedFileLocations() {
    while(dbResult.next()){
      for(int i=0; i<fileFields.length; ++i){
        if(MyUtil.arrayContainsIgnoreCase(fileFields, dbResult.fields[i]) &&
            dbResult.values[i]!=null){
          name = dbResult.getString(nameField);
          urlsStr = dbResult.getString(fileFields[i]);
          urls = MyUtil.split(urlsStr);
          for(int j=0; j<urls.length; ++j){
            // Download url to 'dir'/[record name]
            dlDir = new File(dir, name);
            dlDir.mkdir();
            try{
              GridPilot.getClassMgr().getTransferControl().download(urls[i], dlDir);
            }
            catch(Exception e){
              failedDLs.append(" "+urls[i]);
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

  /**
   * Support method for importToDB.
   */
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
    // Unpack
    String gzipFileName = importFile.replaceFirst("^.*/([^/]+)$", "$1");
    String tarFileName = gzipFileName.replaceFirst("\\.gz$", "");
    String dirName = tarFileName.replaceFirst("\\.tar$", "");
    MyUtil.gunzip(new File(tmpDir, gzipFileName), new File(tmpDir, tarFileName));
    File unpackDir = new File(tmpDir, dirName);
    MyUtil.unTar(new File(tmpDir, dirName), unpackDir);
    return tmpDir;
  }

}
