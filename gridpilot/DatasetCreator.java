package gridpilot;

import gridfactory.common.DBRecord;
import gridfactory.common.Debug;

import javax.swing.*;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Creates the dataset with data given by DatasetCreationPanel.
 */
public class DatasetCreator{

  private boolean showResults;
  private String [] cstAttr;
  private StatusBar statusBar;
  private String [] resCstAttr;
  private String [] cstAttrNames;
  private String [] datasetIDs;
  private String targetDB;
  private static JProgressBar pb = new JProgressBar();
  private static Object semaphoreAMICreation = new Object();
  private DBPluginMgr dbPluginMgr;
  private String [] datasetTransformationReference;
  private String [] datasetTransformationVersionReference;
  public boolean anyCreated = false;

  public DatasetCreator(  StatusBar _statusBar,
                          DBPluginMgr _dbPluginMgr,
                          boolean _showResults,
                          String [] _cstAttr,
                          String [] _cstAttrNames,
                          String [] _datasetIDs,
                          String _targetDB
                          ){
    statusBar = _statusBar;
    showResults = _showResults;
    cstAttr = _cstAttr;
    cstAttrNames =  _cstAttrNames;
    resCstAttr = _cstAttr;
    datasetIDs =  _datasetIDs;
    targetDB = _targetDB;
    dbPluginMgr = _dbPluginMgr;
    
    datasetTransformationReference =
      MyUtil.getDatasetTransformationReference(dbPluginMgr.getDBName());
    datasetTransformationVersionReference =
      MyUtil.getDatasetTransformationVersionReference(dbPluginMgr.getDBName());

    makeDataset();
  }
  
  private void makeDataset(){
    
    boolean okAll = false;
    boolean skip = false;
    boolean showThis  = showResults;
        
    if(targetDB!=null){   
      // this set is used to keep track of which fields were set to ""
      HashSet clearAttrs = new HashSet();
      String transformationName = "";
      String transformationVersion = "";
      for(int j=0; j<cstAttrNames.length; ++j){
        if(cstAttrNames[j].equalsIgnoreCase(datasetTransformationReference[1])){
          transformationName = cstAttr[j];
          Debug.debug("transformation name:"+cstAttr[j], 3);
        }
        else if(cstAttrNames[j].equalsIgnoreCase(datasetTransformationVersionReference[1])){
          transformationVersion = cstAttr[j];
        }
      }
      String datasetNameField = MyUtil.getNameField(dbPluginMgr.getDBName(), "Dataset");
      for(int i=0; i<datasetIDs.length; ++i){
        Debug.debug("Creating #"+datasetIDs[i], 2);
        DBRecord res = dbPluginMgr.getDataset(datasetIDs[i]);
        Debug.debug("Input records "+MyUtil.arrayToString(res.fields), 2);
        Debug.debug("Input values "+MyUtil.arrayToString(res.values), 2);
        clearAttrs.clear();        
        for(int j=0; j<cstAttrNames.length; ++j){                   
          if(!datasetIDs[i].equals("-1")){
            // Get values from source dataset in question, excluding
            // transformation, transVersion and any other filled-in values.
            // Construct name for new target dataset.
            if((resCstAttr[j]==null || resCstAttr[j].equals("")) &&
                cstAttrNames[j].equalsIgnoreCase(datasetNameField)){
              resCstAttr[j] = dbPluginMgr.getTargetDatasetName(
                  targetDB,
                  dbPluginMgr.getDatasetName(datasetIDs[i]),
                  transformationName, transformationVersion);
            }
            else if(cstAttrNames[j].equalsIgnoreCase("runNumber")){
              String runNum = dbPluginMgr.getRunNumber(datasetIDs[i]);
              if(runNum!=null && !runNum.equals("") && !runNum.equals("-1")){
                resCstAttr[j] = runNum;
              }
            }
            else if(cstAttrNames[j].equalsIgnoreCase("inputDataset")){
              resCstAttr[j] = dbPluginMgr.getDatasetName(datasetIDs[i]);
            }
            else if(cstAttrNames[j].equalsIgnoreCase("inputDB")){
              resCstAttr[j] = dbPluginMgr.getDBName();
            }
            else if(cstAttrNames[j].equalsIgnoreCase("identifier") ||
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
            else if(resCstAttr[j]==null || resCstAttr[j].equals("")){
              try{
                resCstAttr[j] = res.getValue(cstAttrNames[j]).toString();
                clearAttrs.add(new Integer(j));
              }
              catch(Exception e){
                e.printStackTrace();
              }
              Debug.debug("Setting "+cstAttrNames[j]+" to "+resCstAttr[j], 2);
            }
            if(resCstAttr[j]==null){
              resCstAttr[j] = "";
            }
          }       
        }
        if(showThis && !okAll){
        //int choice = showResult(resCstAttr, /*datasetIDs[i],*/ i+1<datasetIDs.length);  
        int choice = MyUtil.showResult(cstAttrNames, resCstAttr, "dataset",
            (i+1<datasetIDs.length ? 2 : 1));  
        switch(choice){
          case 0  : skip = false; break;  // OK
          case 1  : skip = true ; break;  // Skip
          case 2  : skip = false; okAll = true ;break;  // OK for all
          case 3  : skip = true ; return; // Skip all
          default : skip = true; break;   // other (closing the dialog). Same action as "Skip"
          }
        }
        
        if(!skip || okAll){
          if(!createDataset(GridPilot.getClassMgr().getDBPluginMgr(targetDB), "dataset")){
            return;
          };
          anyCreated = true;
          //statusBar.removeLabel();
        }
        // Clear attributes that were set to "" on the panel and are thus
        // to be read from each source dataset.
        for(Iterator it=clearAttrs.iterator(); it.hasNext(); ){
          resCstAttr[((Integer) it.next()).intValue()]="";
        }
      }
    }
    else{
      for(int i=0; i<datasetIDs.length; ++i){     
        
        Debug.debug("creating #"+datasetIDs[i], 2);
        //vDataset.add(new Integer(0));
        for(int j=0; j<resCstAttr.length; ++j){
        }

        if(showThis && !okAll){
        //int choice = showResult(resCstAttr, /*datasetIDs[i],*/ i+1<datasetIDs.length);  
        int choice = MyUtil.showResult(cstAttrNames, resCstAttr, "dataset",
            (i+1<datasetIDs.length ? 2 : 1));  
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
  }

  private boolean createDataset(DBPluginMgr dbPluginMgr, String targetTable){
    synchronized(semaphoreAMICreation){
      statusBar.setLabel("Creating dataset...");
      pb.setValue(pb.getValue()+1);
    
      boolean succes = dbPluginMgr.createDataset(
          targetTable, cstAttrNames, resCstAttr);
      if(!succes){
        JOptionPane.showMessageDialog(JOptionPane.getRootFrame(),
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
}
