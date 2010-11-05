package gridpilot;

import gridfactory.common.DBRecord;
import gridfactory.common.Debug;
import gridfactory.common.StatusBar;

import javax.swing.*;

import java.awt.Window;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Creates the dataset with data given by DatasetCreationPanel.
 */
public class DatasetCreator{

  private boolean showResults;
  private boolean okAll = false;
  private boolean skip = false;
  private String [] cstAttr;
  private StatusBar statusBar;
  private String [] resCstAttr;
  private String [] cstAttrNames;
  private String [] datasetIDs;
  private String targetDB;
  private static JProgressBar pb;
  private static Object semaphoreDatasetCreation = new Object();
  private DBPluginMgr dbPluginMgr;
  private String [] datasetExecutableReference;
  private String [] datasetExecutableVersionReference;
  private String datasetIdentifier = "identifier";
  private boolean autoFillName = false;
  private Window parent;
  public boolean anyCreated = false;

  public DatasetCreator(  Window _parent,
                          StatusBar _statusBar,
                          DBPluginMgr _dbPluginMgr,
                          boolean _showResults,
                          String [] _cstAttr,
                          String [] _cstAttrNames,
                          String [] _datasetIDs,
                          String _targetDB
                          ){
    parent = _parent;
    statusBar = _statusBar;
    showResults = _showResults;
    cstAttr = _cstAttr;
    cstAttrNames =  _cstAttrNames;
    resCstAttr = _cstAttr;
    datasetIDs =  _datasetIDs;
    targetDB = _targetDB;
    dbPluginMgr = _dbPluginMgr;
    datasetIdentifier = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "dataset");
    datasetExecutableReference =
      MyUtil.getDatasetExecutableReference(dbPluginMgr.getDBName());
    datasetExecutableVersionReference =
      MyUtil.getDatasetExecutableVersionReference(dbPluginMgr.getDBName());

    makeDataset();
  }
  
  private void makeDataset(){
    pb = new JProgressBar();
    statusBar.setProgressBar(pb);
    statusBar.setProgressBarMax(pb, datasetIDs.length);
    if(targetDB!=null){
      createAllInTargetDB();
    }
    else{
      createAllInThisDB();
    }
    statusBar.removeProgressBar(pb);
  }

  private void createAllInThisDB() {
    
    StringBuffer error = new StringBuffer();
    if(!checkFields(error)){
      MyUtil.showError(parent, error.toString());
      return;
    }  
 
    for(int i=0; i<datasetIDs.length; ++i){       
      Debug.debug("creating #"+datasetIDs[i], 2);
      if(showResults && !okAll){
      int choice = MyUtil.showResult(cstAttrNames, resCstAttr, "dataset",
          (i+1<datasetIDs.length ? 2 : 1), "Skip");  
      switch(choice){
        case 0  : skip = false; break;  // OK
        case 1  : skip = true ; break;  // Skip
        case 2  : skip = false; okAll = true ;break;  // OK for all
        case 3  : skip = true ; return; // Skip all
        default : skip = true; break;   // other (closing the dialog). Same action as "Skip"
        }
      }      
      if(!skip || okAll){
        if(!createDataset(dbPluginMgr, "dataset")){
          return;
        };
        anyCreated = true;
        //statusBar.removeLabel();
      }
    }
  }

  private void createAllInTargetDB() {
    // this set is used to keep track of which fields were set to ""
    HashSet<Integer> clearAttrs = new HashSet<Integer>();
    String executableName = "";
    String executableVersion = "";
    for(int j=0; j<cstAttrNames.length; ++j){
      if(cstAttrNames[j].equalsIgnoreCase(datasetExecutableReference[1])){
        executableName = cstAttr[j];
        Debug.debug("executable name:"+cstAttr[j], 3);
      }
      else if(cstAttrNames[j].equalsIgnoreCase(datasetExecutableVersionReference[1])){
        executableVersion = cstAttr[j];
      }
    }
    for(int i=0; i<datasetIDs.length; ++i){
      Debug.debug("Creating #"+datasetIDs[i], 2);
      if(!createInTargetDB(clearAttrs, i, executableName,
          executableVersion)){
        return;
      }
    }
  }

  private boolean createInTargetDB(HashSet<Integer> clearAttrs, int i,
       String exeName, String exeVersion) {
    DBRecord res = dbPluginMgr.getDataset(datasetIDs[i]);
    Debug.debug("Input records "+MyUtil.arrayToString(res.fields), 2);
    Debug.debug("Input values "+MyUtil.arrayToString(res.values), 2);
    clearAttrs.clear();
    if(!datasetIDs[i].equals("-1")){
      fillInValues(datasetIDs[i], i, exeName, exeVersion);
    }
    if(showResults && !okAll){
    int choice = MyUtil.showResult(cstAttrNames, resCstAttr, "dataset",
        (i+1<datasetIDs.length ? 2 : 1), "Skip");  
    switch(choice){
      case 0  : skip = false; break;  // OK
      case 1  : skip = true ; break;  // Skip
      case 2  : skip = false; okAll = true ;break;  // OK for all
      case 3  : skip = true ; return false; // Skip all
      default : skip = true; break;   // other (closing the dialog). Same action as "Skip"
      }
    }
    
    if(!skip || okAll){
      if(!createDataset(GridPilot.getClassMgr().getDBPluginMgr(targetDB), "dataset")){
        return false;
      };
      anyCreated = true;
      //statusBar.removeLabel();
    }
    // Clear attributes that were set to "" on the panel and are thus
    // to be read from each source dataset.
    for(Iterator<Integer> it=clearAttrs.iterator(); it.hasNext(); ){
      resCstAttr[it.next().intValue()]="";
    }
    return true;
  }

  private void fillInValues(String datasetId, int datasetIndex, String exeName, String exeVersion) {
    String datasetNameField = MyUtil.getNameField(targetDB, "Dataset");
    for(int j=0; j<cstAttrNames.length; ++j){         
      // Get values from source dataset in question, excluding
      // executable, executableVersion and any other filled-in values.
      // Construct name for new target dataset.
      Debug.debug("Filling in "+cstAttrNames[j]+" : "+resCstAttr[j], 3);
      if((autoFillName || resCstAttr[j]==null || resCstAttr[j].equals("") || datasetIDs!=null && datasetIDs.length>1) &&
          cstAttrNames[j].equalsIgnoreCase(datasetNameField)){
        autoFillName = true;
        resCstAttr[j] = dbPluginMgr.createTargetDatasetName(resCstAttr[j], targetDB,
           dbPluginMgr.getDatasetName(datasetId), datasetIndex, exeName, exeVersion);
      }
      else if(cstAttrNames[j].equalsIgnoreCase("runNumber")){
        String runNum = dbPluginMgr.getRunNumber(datasetId);
        if(runNum!=null && !runNum.equals("") && !runNum.equals("-1")){
          resCstAttr[j] = runNum;
        }
      }
      else if(cstAttrNames[j].equalsIgnoreCase("inputDataset")){
        resCstAttr[j] = dbPluginMgr.getDatasetName(datasetId);
      }
      else if(cstAttrNames[j].equalsIgnoreCase("inputDB")){
        resCstAttr[j] = dbPluginMgr.getDBName();
      }
      else if(cstAttrNames[j].equalsIgnoreCase(datasetIdentifier) ||
          cstAttrNames[j].equalsIgnoreCase("percentageValidatedFiles") ||
          cstAttrNames[j].equalsIgnoreCase("percentageFailedFiles ") ||
          //cstAttrNames[j].equalsIgnoreCase("totalFiles") ||
          //cstAttrNames[j].equalsIgnoreCase("totalEvents") ||
          cstAttrNames[j].equalsIgnoreCase("averageEventSize") ||
          cstAttrNames[j].equalsIgnoreCase("totalDataSize") ||
          cstAttrNames[j].equalsIgnoreCase("averageCPUTime") ||
          cstAttrNames[j].equalsIgnoreCase("totalCPUTime") ||
          cstAttrNames[j].equalsIgnoreCase("created") ||
          cstAttrNames[j].equalsIgnoreCase("lastModified") ||
          cstAttrNames[j].equalsIgnoreCase("lastStatusUpdate")){
        resCstAttr[j] = "";
      }
      // See if attribute has not been set. If it hasn't, set it and clear it
      // again after the new dataset has been created.
      // --> hmm... Bad idea for metaData - not sure if this is a good idea in general.
      /*else if(resCstAttr[j]==null || resCstAttr[j].equals("")){
        try{
          resCstAttr[j] = res.getValue(cstAttrNames[j]).toString();
          clearAttrs.add(new Integer(j));
        }
        catch(Exception e){
          e.printStackTrace();
        }
        Debug.debug("Setting "+cstAttrNames[j]+" to "+resCstAttr[j], 2);
      }*/
      if(resCstAttr[j]==null){
        resCstAttr[j] = "";
      }
      Debug.debug("Filled in "+cstAttrNames[j]+" : "+resCstAttr[j], 3);
    }
  }

  private boolean createDataset(DBPluginMgr dbPluginMgr, String targetTable){
    synchronized(semaphoreDatasetCreation){    
      statusBar.setLabel("Creating dataset...");
      pb.setValue(pb.getValue()+1);
      boolean succes = dbPluginMgr.createDataset(targetTable, cstAttrNames, resCstAttr);
      if(!succes){
        JOptionPane.showMessageDialog(parent,
           "ERROR: dataset cannot be created.\n"+
           dbPluginMgr.getError(),
           "", JOptionPane.ERROR_MESSAGE);
        statusBar.setLabel("Dataset NOT created.");
        return false;
      }
      else{
        statusBar.setLabel("Dataset created.");
      }
    }
    return true;
  }
  
  private boolean checkFields(StringBuffer error) {
    boolean ok = true;
    // Check that name and version have been set
    String [] jobDefDatasetReference = MyUtil.getJobDefDatasetReference(dbPluginMgr.getDBName());
    for(int i=0; i<cstAttrNames.length; ++i){
      if(cstAttrNames[i].toString().equalsIgnoreCase(jobDefDatasetReference[0])){
        if(cstAttr[i]==null || cstAttr[i].equals("")){
          ok = false;
          error.append("You must fill in "+jobDefDatasetReference[0]+". ");
        }
      }
    }
    return ok;
  }

}
