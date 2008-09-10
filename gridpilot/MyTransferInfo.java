package gridpilot;

import org.globus.util.GlobusURL;

import gridfactory.common.TransferInfo;

/**
 * Each instance of this class represents a submitted transfer.
 */
public class MyTransferInfo extends TransferInfo{

  public MyTransferInfo(GlobusURL arg0, GlobusURL arg1) {
    super(arg0, arg1);
  }

  private DBPluginMgr dbPluginMgr = null;
  private String datasetName = null;
  private String datasetID = null;
  
  //       transferStatusFields = new String [] {
  // " ", "Transfer ID", "Source", "Destination", "User", "Status", "Transferred"};

  public final static int FIELD_CONTROL = 0;
  public final static int FIELD_TRANSFER_ID = 1;
  public final static int FIELD_SOURCE = 2;
  public final static int FIELD_DESTINATION = 3;
  public final static int FIELD_USER = 4;
  public final static int FIELD_STATUS = 5;
  public final static int FIELD_TRANSFERRED = 6;

  public DBPluginMgr getDBPluginMgr(){
    return dbPluginMgr;
  }

  public String getDatasetName(){
    return datasetName;
  }

  public String getDatasetID(){
    return datasetID;
  }

  /**
   * If dbPluginMgr is set, it is implicitly assumed that the file should be
   * registered in the corresponding database upon completion of the transfer.
   */
  public void setDBPluginMgr(DBPluginMgr _dbPluginMgr){
    dbPluginMgr = _dbPluginMgr;
  }

  public void setDatasetName(String _datasetName){
    datasetName = _datasetName;
  }

  public void setDatasetID(String _datasetID){
    datasetID = _datasetID;
  }
  
  // Plugins are only loaded once and always carry a hard-coded name,
  // identical to the protocol name, e.g. srm.
  // IDs are of the form protocol-(get|put|copy):....
  public String getFTName(){
    String ft = "";
    try{
      String [] split = MyUtil.split(getTransferID(), "-");
      ft = split[0];
    }
    catch(Exception e){
      //e.printStackTrace();
    }
    return ft;
  }

}


