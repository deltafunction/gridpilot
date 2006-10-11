package gridpilot;

import org.globus.util.GlobusURL;

/**
 * Each instance of this class represents a submitted transfer.
 */
public class TransferInfo extends DBRecord{

  private String id = null;
  private GlobusURL source = null;
  private GlobusURL destination = null;
  private String status = null;
  private String transferred = null;
  private int internalStatus = -1;
  // Used for registration on completion
  private DBPluginMgr dbPluginMgr = null;
  private String datasetName = null;
  private String datasetID = null;
  private String lfn = null;
  private String guid = null;
  
  //       transferStatusFields = new String [] {
  // " ", "Transfer ID", "Source", "Destination", "User", "Status", "Transferred"};

  public final static int FIELD_CONTROL = 0;
  public final static int FIELD_TRANSFER_ID = 1;
  public final static int FIELD_SOURCE = 2;
  public final static int FIELD_DESTINATION = 3;
  public final static int FIELD_USER = 4;
  public final static int FIELD_STATUS = 5;
  public final static int FIELD_TRANSFERRED = 6;

  private boolean needUpdate;
  private int tableRow = -1;

  public TransferInfo(GlobusURL source, GlobusURL destination){
    setSource(source);
    setDestination(destination);
  }
  
  public String getTransferID(){
    return id;
  }
  
  public String getStatus(){
    return status;
  }
  
  public String getTransferred(){
    return transferred;
  }
  
  public DBPluginMgr getDBPluginMgr(){
    return dbPluginMgr;
  }

  public String getDatasetName(){
    return datasetName;
  }

  public String getDatasetID(){
    return datasetID;
  }

  public String getLFN(){
    return lfn;
  }

  public String getGUID(){
    return guid;
  }

  public int getInternalStatus(){
    return internalStatus;
  }
  
  // Plugins are only loaded once and always carry a hard-coded name,
  // identical to the protocol name, e.g. srm.
  // IDs are of the form protocol-(get|put|copy):....
  public String getFTName(){
    String [] split = Util.split(id, "-");
    return split[0];
  }
  
  public GlobusURL getSource(){
    return source;
  }
  
  public void setTransferID(String _id){
    id = _id;
  }

  public void setStatus(String _status){
    status = _status;
  }

  public void setTransferred(String _transferred){
    transferred = _transferred;
  }

  public void setInternalStatus(int _status){
    internalStatus = _status;
  }

  public void setSource(GlobusURL _source){
    source = _source;
  }

  public GlobusURL getDestination(){
    return destination;
  }
  
  public void setDestination(GlobusURL _destination){
    destination = _destination;
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

  public void setLFN(String _lfn){
    lfn = _lfn;
  }

  public void setGUID(String _guid){
    guid = _guid;
  }

  public String toString(){
    return "Transfer ID : " + getTransferID()+ "\n" +
        "Source : " + getSource().getURL() + "\n" +
        "Destination : " + getDestination().getURL();
  }

  public boolean needToBeRefreshed(){
    return needUpdate;
  }

  public void setNeedToBeRefreshed(boolean _needUpdate){
    needUpdate = _needUpdate;
  }

  public int getTableRow(){
    return tableRow;
  }

  public void setTableRow(int _tableRow){
    tableRow = _tableRow;
  }

}


