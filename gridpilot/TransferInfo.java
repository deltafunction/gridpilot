package gridpilot;

import java.awt.Color;
import java.util.HashMap;

import org.globus.util.GlobusURL;

/**
 * Each instance of this class represents a submitted transfer.
 */
public class TransferInfo extends DBRecord{

  private String id = null;
  private GlobusURL source = null;
  private GlobusURL destination = null;
  private HashMap values = null;
  private String status = null;
  private int internalStatus = -1;
  private DBPluginMgr dbPluginMgr = null;
  
  //       transferStatusFields = new String [] {
  // " ", "Transfer ID", "Source", "Destination", "User", "Status"};

  public final static int FIELD_CONTROL = 0;
  public final static int FIELD_TRANSFER_ID = 1;
  public final static int FIELD_SOURCE= 2;
  public final static int FIELD_DESTINATION= 3;
  public final static int FIELD_USER= 4;
  public final static int FIELD_STATUS= 5;

  private boolean needUpdate;
  private int tableRow = -1;
  
  /**
   * Returns status names for statistics panel.
   */
  public static String [] getStatusNames(){
    return new String [] {"Wait", "Run", "Done"};
  }

  /**
   * Returns colors corresponding to getStatusNames for statistics panel.
   */
  public static Color [] getStatusColors(){
    return new Color [] {Color.black,
                         Color.blue,
                         Color.green,
                         Color.orange,
                         Color.magenta,
                         Color.red};
  }

  /**
   * DB status names for statistics panel. <p>
   */
  private static String [] ftStatusNames = new String [] {
      "Defined",
      "Queued",
      "Running",
      "Done",
      "Failed",
      "Aborted"};

  public static String [] getFTStatusNames(){
    return ftStatusNames;
  }

  public static String getFTStatusName(int status){
    switch(status){
      case FileTransfer.STATUS_WAIT : return "Queued";
      case FileTransfer.STATUS_RUNNING : return "Running";
      case FileTransfer.STATUS_DONE : return "Done";
      case FileTransfer.STATUS_FAILED : return "Failed";
      case FileTransfer.STATUS_ERROR : return "Error";
      default : return "status not found";
    }
  }

  public TransferInfo(GlobusURL source, GlobusURL destination){
    for(int i=0; i<GridPilot.transferStatusFields.length; ++i){
      values.put(GridPilot.transferStatusFields[i], null);
    }
    values.put("Source", source);
    values.put("Destination", destination);
  }
  
  public String getTransferID(){
    return id;
  }
  
  public String getStatus(){
    return status;
  }
  
  public DBPluginMgr getDBPluginMgr(){
    return dbPluginMgr;
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
  
  public void setDBPluginMgr(DBPluginMgr _dbPluginMgr){
    dbPluginMgr = _dbPluginMgr;
  }

  public String toString(){
    return "\nTransfer # " + getTransferID()+ "\n" +
        "  Source \t: " + values.get("Source") + "\n" +
        "  Destination \t: " + values.get("Destination");
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


