package gridpilot;

import gridfactory.common.ConfirmBox;
import gridfactory.common.DBRecord;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.Util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.globus.util.GlobusURL;

public class ExportImport {
  
  /**
   * Placeholder for the import directory, used in database records produced by
   * {@link #exportDB()}.
   */
  private static final String IMPORT_DIR = "GRIDPILOT_IMPORT_DIR";
  private static final String [] EXE_FILE_FIELDS = new String [] {"executableFile", "inputFiles"};
  private static final String EXE_FILES_DIR = "executables";
  private static final String OUTPUT_LOCATION_FIELD = "outputLocation";
  private static final String DEFAULT_DATA_DIR = "data";

  /**
   * Exports dataset and corresponding executable information from the chosen
   * database. If 'dbName' is null, a popup is displayed asking to choose a database.
   * If 'datasetId' is null, the whole "dataset" and "executable" tables are exported.
   * Input files for the tranformation(s) are bundled in the exported tarball.
   * @param exportDir
   * @param datasetIds
   * @param _dbName
   * @throws Exception
   */
  public static boolean exportDB(String exportDir, String _dbName, String[] datasetIds) throws Exception{
    String dbName = _dbName;
    String[] executableIds = null;
    String[] datasetNames = null;
    String exeName;
    String exeVersion;
    if(datasetIds!=null && datasetIds.length>0){
      executableIds = new String [datasetIds.length];
      datasetNames = new String [datasetIds.length];
      DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
      for(int i=0; i<datasetIds.length; ++i){
        datasetNames[i] = mgr.getDatasetName(datasetIds[i]);
        exeName = mgr.getDatasetExecutableName(datasetIds[i]);
        exeVersion = mgr.getDatasetExecutableVersion(datasetIds[i]);
        executableIds[i] = mgr.getExecutableID(exeName, exeVersion);
      }
    }
    String exportFileName = null;;
    Object [] choices;
    String proposedName;
    if(datasetNames!=null && datasetNames.length>0){
      proposedName = datasetNames[0]+GridPilot.APP_EXTENSION;
    }
    else{
      proposedName = "GridPilot_EXPORT_"+MyUtil.getDateInMilliSeconds()+GridPilot.APP_EXTENSION;
    }
    JTextField tf = new JTextField(proposedName, 32);
    choices = new Object[] {"Cancel", "Export to this file", tf};
    ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame());
    String message = (datasetIds==null?
         "This will export all dataset(s) and executable(s) of the chosen database plus\n" +
              "any physical file(s) associated with the executable(s). Non-local file(s) will\n" +
              "be downloded first.\n\n" :
         "This will export the dataset(s) \n\n" +
               "\"" + MyUtil.arrayToString(datasetNames) + "\"\n\n" +
               "and associated executable(s) plus any physical file(s) associated\n" +
               "with the executable(s). Non-local file(s) will be downloaded first.\n\n") +
               "Choose a  name for the exported file. Notice that if a file with this name\n" +
               "already exists in "+exportDir+", it will be overwritten.\n\n";
    for(int j=0; j<3; ++j){
      int choice = confirmBox.getConfirm("Export from database", message, choices, 1);
      if(choice<0 || choice!=1){
        Debug.debug("Export cancelled.", 2);
        return false;
      }
      exportFileName = tf.getText().trim();
      if(exportFileName==null || exportFileName.trim().length()==0){
        MyUtil.showError("You must give a name for the export file.");
      }
      else{
        break;
      }
    }
    if(exportFileName==null || exportFileName.trim().length()==0){
      Debug.debug("No file name given.", 2);
      return false;
    }
    exportFileName = exportFileName.trim();
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
    saveTableAndFiles(tmpDir, DEFAULT_DATA_DIR, dbName, "dataset", new String [] {OUTPUT_LOCATION_FIELD},
        datasetIds);
    saveTableAndFiles(tmpDir, EXE_FILES_DIR, dbName, "executable", EXE_FILE_FIELDS,
        executableIds);
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
    return true;
  }

  /**
   * This will fill a tmp directory with an SQL file, 'table'.sql plus a subdirectory
   * 'table'Files, containing the files found in 'fileFields'.
   * The URLs found in 'fileFields' will be changed to 'exportImportDir'/filesDirName/[record name]/[file name].
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
   * @param filesDirName
   * @param dbName
   * @param table
   * @param fileFields
   * @param sqlExtension
   * @param id the identifier of the row to be exported. If this is null, the whole table is exported.
   * @throws Exception
   */
  private static void saveTableAndFiles(File dbDir, String filesDirName, String dbName, String table,
      String [] fileFields, String [] ids) throws Exception{
    String idField = MyUtil.getIdentifierField(dbName, table);
    String query = "SELECT * FROM "+table;
    if(ids!=null && ids.length>0){
      Debug.debug("Exporting rows "+MyUtil.arrayToString(ids), 2);
      query += " WHERE";
      for(int i=0; i<ids.length; ++i){
        query += (i>0?" AND ":" ")+idField+" = "+ids[i];
      }
    }
    DBResult dbResult =
      GridPilot.getClassMgr().getDBPluginMgr(dbName).select(query, idField, true);
    File dir = new File(dbDir, filesDirName);
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
          if(fileFields[i].equals(OUTPUT_LOCATION_FIELD) || urlsStr==null || urlsStr.endsWith("/")){
            continue;
          }
          urls = MyUtil.splitUrls(urlsStr);
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

  private static String toSql(DBResult dbResult, String dbName, String table, String [] fileFields) throws Exception{
    Debug.debug("Converting DBResult with "+dbResult.values.length+" rows", 2);
    String executableDir = GridPilot.getClassMgr().getConfigFile().getValue(
        "Fork", "executable directory");
    File executableDirectory = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(
        executableDir)));
    File gridPilotDir = executableDirectory.getParentFile();
    String gpDirStr = MyUtil.replaceWithTildeLocally(gridPilotDir.getAbsolutePath());
    String nameField = MyUtil.getNameField(dbName, table);
    StringBuffer res = new StringBuffer();
    StringBuffer insFields = new StringBuffer();
    insFields.append("INSERT INTO "+table+" (");
    StringBuffer insValues = new StringBuffer();
    insValues.append(") VALUES (");
    Object [][] newValues = dbResult.values.clone();
    String name;
    String checkVal;
    boolean oneInserted = false;
    for(int i=0; i<dbResult.values.length; ++i){
      name = (String) dbResult.getValue(i, nameField);
      for(int j=0; j<dbResult.fields.length; ++j){
        if(newValues[i][j]==null || newValues[i][j].toString().trim().equals("")){
          continue;
        }
        checkVal = MyUtil.clearFile(((String) newValues[i][j]).trim());
        // This is for outputLocation: catch those that have been set to a local path
        Debug.debug("Checking dir "+MyUtil.replaceWithTildeLocally(checkVal)+"<->"+gpDirStr, 2);
        Debug.debug("Checking field "+fileFields.length+":"+fileFields[0], 2);
        if(fileFields.length==1 && dbResult.fields[j].equalsIgnoreCase(fileFields[0]) &&
            fileFields[0].equalsIgnoreCase(OUTPUT_LOCATION_FIELD)/* &&
            (checkVal.matches("^/.+$") || checkVal.matches("^\\w+:\\\\.+ 4"))*/){
          // If the path is inside of ~/GridPilot set ~/GridPilot to IMPORT_DIR
          if(MyUtil.replaceWithTildeLocally(checkVal).startsWith(gpDirStr)){
            Debug.debug("Replacing in "+newValues[i][j], 2);
            newValues[i][j] = MyUtil.replaceWithTildeLocally(checkVal).replaceFirst(
               gpDirStr, IMPORT_DIR).replaceAll("\\\\", "/");
            Debug.debug("--> "+newValues[i][j], 2);
          }
          // Otherwise replace the whole string with with IMPORT_DIR/DEFAULT_DATA_DIR
          else{
            Debug.debug("Replacing in "+newValues[i][j], 2);
            newValues[i][j] = ((String) newValues[i][j]).trim().replaceAll("\\\\", "/"
               ).replaceAll("^.*/([^/]+)$", IMPORT_DIR+"/$1");
            Debug.debug("--> "+newValues[i][j], 2);
          }
        }
        else if(MyUtil.arrayContainsIgnoreCase(fileFields, dbResult.fields[j]) &&
            newValues[i][j]!=null){
          Debug.debug("Replacing in "+newValues[i][j], 2);
          // For executable files, replace URLs with file names in exportImportDir
          newValues[i][j] = fixExportedURLs((String) newValues[i][j], name);
          Debug.debug("--> "+newValues[i][j], 2);
        }
      }
      oneInserted = false;
      for(int j=0; j<dbResult.fields.length; ++j){
        if(newValues[i][j]!=null && !newValues[i][j].equals("")){
          insFields.append((oneInserted?", ":"")+dbResult.fields[j]);
          insValues.append((oneInserted?", ":"")+"'"+sqlToText((String) newValues[i][j])+"'");
          oneInserted = true;
        }
      }
      res.append(insFields.toString()+insValues.toString()+");\n");
    }
    return res.toString();
  }
  
  private static String fixExportedURLs(String urls, String name) throws Exception {
    String [] newUrls = MyUtil.splitUrls(urls);
    for(int i=0; i<newUrls.length; ++i){
      newUrls[i] = newUrls[i].trim().replaceAll("\\\\", "/"
      ).replaceAll("^.*/([^/]+)$", IMPORT_DIR+"/"+EXE_FILES_DIR+"/"+name+"/$1");
    }
    return MyUtil.arrayToString(newUrls);
  }

  private static String sqlToText(String value){
    String value1 = value.replaceAll("\\\\\'", "\\\\quote");
    value1 = value1.replaceAll("\'", "\\\\quote");
    return value1;
  }

  private static String textToSql(String value, boolean excludeIdentifier){
    if(value==null){
      return "";
    }
    if(excludeIdentifier){
      //String pattern = "(?i)(?s)(INSERT\\s+INTO\\s+\\w+\\s+\\([^)]+),\\s*identifier(\\)\\s+VALUES\\s+\\([^)]+),\\s*\\S+\\);*$";
      String pattern = "(?i)(?s),\\s*identifier\\)";
      Debug.debug("Checking identifier in: "+value, 3);
      if(value.matches(".*"+pattern+".*")){
        //value1 = value1.replaceFirst(pattern, "$1$2)");
        value = value.replaceFirst(pattern, ")");
        value = value.replaceFirst(",\\s*\\S+\\);*$", ");");
        Debug.debug("Dropped identifier: "+value, 3);
      }
    }
    //value = value.replaceAll("\\\\quote", "\\\'");
    return value;
  }

  /**
   * Returns {db name, import report text}.
   */
  public static String [] importToDB(String importFile) throws Exception{
    String executableDir = GridPilot.getClassMgr().getConfigFile().getValue(
        "Fork", "executable directory");
    File executableDirectory = new File(MyUtil.clearTildeLocally(MyUtil.clearFile(
        executableDir)));
    File gridPilotDir = executableDirectory.getParentFile();
    File dataDirectory = new File(gridPilotDir, DEFAULT_DATA_DIR);
    Vector<String> choicesVec = new Vector<String>();
    for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
      if(GridPilot.getClassMgr().getDBPluginMgr(GridPilot.DB_NAMES[i]).isJobRepository()){
        choicesVec.add(GridPilot.DB_NAMES[i]);
      }
    }
    String dbName;
    if(choicesVec.size()==0){
      return null;
    }
    else if(choicesVec.size()==1){
      dbName = choicesVec.get(0);
    }
    else{
      choicesVec.add("none (cancel)");
      String [] choices = choicesVec.toArray(new String[choicesVec.size()]);
      ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame());
      int choice = confirmBox.getConfirm("Import in database",
          "<html>This will import the dataset(s) and executable(s) contained in<br>"+
          importFile+"<br>in the chosen database.<br>" +
          "Any file(s) associated with the executable(s) will be copied to<br>" +
          executableDirectory + "/.<br><br>" +
          "Non-existing local output location(s) will be set to<br>" +
          dataDirectory + "/.<br><br>" +
          "Choose database to use.<br></html>", choices);
      if(choice<0 || choice>=choices.length-1){
        return null;
      }
      dbName = choices[choice];
    }
    DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    // Download the import file, unpack it in a tmp dir
    File tmpDir = downloadAndUnpack(importFile);
    // have the tmp dir deleted on exit
    GridPilot.addTmpFile(tmpDir.getAbsolutePath(), tmpDir);
    // Insert the SQL
    String sqlFile = (new File(tmpDir, "executable.sql")).getAbsolutePath();
    String sql = LocalStaticShell.readFile(sqlFile);
    String fixedSql = textToSql(sql, true);
    mgr.executeUpdate(fixedSql);
    Debug.debug("mgr reports: "+mgr.getError(), 3);
    if(mgr.getError()!=null && !mgr.getError().equals("")){
      throw new SQLException(mgr.getError());
    }
    String ret = "Successfully imported:\n\n";
    ret += (sql!=null&&!sql.equals("")?
        " - "+((sql.length()-sql.toLowerCase().replaceAll("(?i)(?s)insert ", "").length())/7)+
           " executable record(s)\n":
        "");
    sqlFile = (new File(tmpDir, "dataset.sql")).getAbsolutePath();
    sql = LocalStaticShell.readFile(sqlFile);
    fixedSql = textToSql(sql, false);
    mgr.executeUpdate(fixedSql);
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
    fixImportedExecutableFilesLocations(dbName, gridPilotDir);
    fixImportedOutputLocations(dbName, gridPilotDir);
    // Clean up
    tmpDir.delete();
    ret += "\ninto database "+dbName+"";
    ret +=(numFiles>0? "\n\nand\n\n - "+numFiles+ " application file(s)\n\ninto directory "+executableDirectory+".":
        ".");
    return new String[] {dbName, ret};
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

  private static void fixImportedOutputLocations(String dbName, File dir) {
    DBPluginMgr mgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
    String sql = "SELECT * FROM dataset WHERE outputLocation CONTAINS "+IMPORT_DIR+"/";
    String idField = MyUtil.getIdentifierField(dbName, "dataset");
    String nameField = MyUtil.getNameField(dbName, "dataset");
    DBResult dbResult = mgr.select(sql, idField, true);
    String id;
    String name;
    String urlStr;
    String newUrlStr;
    int recNr = 0;
    while(dbResult.moveCursor()){
      newUrlStr = null;
      id = dbResult.getString(idField);
      name = dbResult.getString(nameField);
      urlStr = dbResult.getString(OUTPUT_LOCATION_FIELD);
      if(urlStr!=null){
        // replace IMPORT_DIR/ with 'dir'/
        newUrlStr = urlStr.replaceFirst("^"+IMPORT_DIR, "file:"+MyUtil.replaceWithTildeLocally(
            dir.getAbsolutePath()));
      }
      if(newUrlStr!=null){
        // write back the record
        mgr.updateDataset(id, name, new String [] {OUTPUT_LOCATION_FIELD}, new String [] {newUrlStr});
        try{
          if(MyUtil.isLocalFileName(newUrlStr)){
            LocalStaticShell.mkdirs(newUrlStr);
          }
        }
        catch(Exception e){
        }
      }
      ++recNr;
    }
  }
  
  private static void fixImportedExecutableFilesLocations(String dbName, File dir) throws Exception {
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
          urls = MyUtil.splitUrls(urlsStr);
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

  private static int moveTransInputs(String tmpDir, File executableDirectory) throws IOException {
    String [] files = LocalStaticShell.listFiles(tmpDir);
    if(files==null){
      return 0;
    }
    String fileName;
    String dest;
    int ret = files.length;
    for(int i=0; i<ret; ++i){
      if(!files[i].endsWith(".sql")){
        fileName = (new File(files[i])).getName();
        dest = (new File(executableDirectory, fileName)).getAbsolutePath();
        if(!LocalStaticShell.moveFile(files[i], dest)){
          throw new IOException("Could not move "+files[i]+" to "+dest);
        }
      }
    }
    return ret;
  }

/////////////// FUNCTIONS FOR IMPORTNG FILES INTO DATASETS ////////////////
  
  /**
   * Ask for directory and import all files in it.
   * @throws Exception 
   */
  public static void importFiles(final String datasetID, final String datasetName,
     final DBPluginMgr dbPluginMgr) throws Exception{
    // Find the list of files
    String [] regUrls = null;
    String [] regSizes = null;
    String regBaseURL = null;
    DBRecord dataset = dbPluginMgr.getDataset(datasetID);
    String outputLocation = (String) dataset.getValue("outputLocation");
    String [][] importFiles = getImportFiles(outputLocation);
    if(importFiles[0].length==0){
      MyUtil.showMessage("No files imported", "No files found.\n\n");
      return;
    }
    regUrls = importFiles[0];
    regSizes = importFiles[1];
    regBaseURL = importFiles[2][0];
    dbPluginMgr.importFiles(datasetID, datasetName, regUrls, regSizes, regBaseURL);
    dbPluginMgr.updateDataset(datasetID, datasetName, new String[]{"outputLocation"}, new String[]{regBaseURL});
  }

  // Returns a 3xn array of URLs, sizes and URL dirs
  private static String [][] getImportFiles(String startURL) throws Exception{
    final String finUrl = startURL;
    final String finBaseUrl = "";
    JCheckBox cbRecursive = new JCheckBox();
    cbRecursive.setSelected(false);
    JPanel panel = new JPanel();
    panel.add(new JLabel("Recursive"));
    panel.setToolTipText("<html>Find files in subdirectories recursively.</html>");
    panel.add(cbRecursive);
    BrowserPanel wb = null;
    try{
      wb = new BrowserPanel(
           GridPilot.getClassMgr().getGlobalFrame(),
           "Choose files",
           finUrl,
           finBaseUrl,
           true,
           /*filter*/true,
           /*navigation*/true,
           panel,
           null,
           false,
           true,
           false);   
      Debug.debug("NOT registering", 2);
    }
    catch(Exception eee){
      Debug.debug("Could not open URL "+finUrl+". "+eee.getMessage(), 1);
      eee.printStackTrace();
      GridPilot.getClassMgr().getStatusBar().setLabel("Could not open URL "+finBaseUrl+". "+eee.getMessage());
      ConfirmBox confirmBox = new ConfirmBox(GridPilot.getClassMgr().getGlobalFrame()/*,"",""*/); 
      try{
        confirmBox.getConfirm("URL could not be opened",
                             "The URL "+finBaseUrl+" could not be opened. \n"+eee.getMessage(),
                          new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())});
      }
      catch(Exception eeee){
        Debug.debug("Could not get confirmation, "+eeee.getMessage(), 1);
      }
    }
    if(wb!=null && wb.getLastURL()!=null &&
        wb.getLastURL().startsWith(finBaseUrl)){
        //GridPilot.getClassMgr().getStatusBar().setLabel("");
    }
    else{
      // Don't do anything if we cannot get a URL
      Debug.debug("ERROR: Could not open URL "+finBaseUrl, 1);
      return null;
    }
    Debug.debug("Checking files/dirs "+MyUtil.arrayToString(wb.getLastURLs())+
        ":"+MyUtil.arrayToString(wb.getLastSizes()), 2);
    String [][] ret0 = new String [2][];
    String [][] ret = new String [3][];
    String filter = wb.getFilter();
    boolean prependedStar = false;
    if(!filter.startsWith("*")){
      prependedStar = true;
      filter = "*"+filter;
    }
    // First find all files matching *filter, then chop off basedir/ and match on filter
    if(cbRecursive.isSelected() /*&& MyUtil.isLocalFileName(wb.getLastURLs()[0])*/){
      ret0 = MyTransferControl.findAllFiles(wb.getLastURLs(), wb.getLastSizes(), filter);
    }
    else{
      ret0 = findFiles(wb.getLastURLs(), wb.getLastSizes(), filter);
    }
    Vector<String> retVec0 = new Vector<String>();
    Vector<String> retVec1 = new Vector<String>();
    String path;
    String fileName;
    for(int i=0; i<ret0[0].length; ++i){
      path = ret0[0][i].replaceFirst(startURL, "");
      fileName = ret0[0][i].replaceFirst("^.*/([^/]+)$", "$1");
      if(!prependedStar ||
         Util.filterMatches(fileName, wb.getFilter()) || Util.filterMatches(path, wb.getFilter())){
        retVec0.add(ret0[0][i]);
        retVec1.add(ret0[1][i]);
      }
    }
    Debug.debug("Found "+ret0[0].length+" further matched to "+retVec0.size(), 2);
    ret[0] = new String[retVec0.size()];
    ret[1] = retVec1.toArray(new String[retVec1.size()]);
    ret[2] = new String[retVec0.size()];
    for(int i=0; i<ret[0].length; ++i){
      ret[0][i] = fixLocalFile(retVec0.get(i));
      ret[2][i] = wb.getLastURL();
    }
    //GridPilot.getClassMgr().getStatusBar().setLabel("");
    Debug.debug("Returning last URL list "+MyUtil.arrayToString(ret[0])+" --> "+
        MyUtil.arrayToString(ret[1]), 2);
    return ret;
  }
  
  private static String fixLocalFile(String fil) {
    if(MyUtil.isLocalFileName(fil) && !fil.startsWith("file:")){
      return "file://"+fil;
    }
    return fil;
  }

  private static String[][] findFiles(String[] lastUrlsList, String[] lastSizesList, String filter) {
    Vector<String> files = new Vector<String>();
    Vector<String> sizes = new Vector<String>();
    String path = null;
    for(int i=0; i<lastUrlsList.length; ++i){
      try{
        path = (new GlobusURL(lastUrlsList[i])).getPath();
      }
      catch (MalformedURLException e) {
        e.printStackTrace();
        path = null;
      }
      if(!lastUrlsList[i].endsWith("/") && MyUtil.filterMatches(path, filter)){
        files.add(lastUrlsList[i]);
        sizes.add(lastSizesList[i]);
      }
    };
    String[][] ret = new String[2][];
    ret[0] = files.toArray(new String[files.size()]);
    ret[1] = sizes.toArray(new String[sizes.size()]);
    return ret;
  }
  
}
