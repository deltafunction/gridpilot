package gridpilot.ftplugins.https;

import gridpilot.Debug;
import gridpilot.GridPilot;

import org.globus.io.urlcopy.UrlCopyListener;

public class MyUrlCopyTransferListener implements UrlCopyListener {
    
  private Exception exception;
  private String status;
  private String error = "";
  private long percentComplete = 0;
  private long bytesTransferred = 0;
  
  private final String STATUS_WAIT = "Wait";
  private final String STATUS_TRANSFER = "Transfer";
  private final String STATUS_DONE = "Done";
  private final String STATUS_ERROR = "Error";

  public MyUrlCopyTransferListener(){ 
  }
  
  public long getPercentComplete(){
    return percentComplete;
  }
  
  public long getBytesTransferred(){
    return bytesTransferred;
  }
	
  public String getStatus(){
    return status;
  }
  
  public String getError(){
    return error;
  }
  
  public void transfer(long current, long total){
    //Debug.debug(current+" out of "+total+" : "+status, 3);
    bytesTransferred = current;
    percentComplete = 100*current/total;
    if(total==-1){
      if(current==-1){
        //JOptionPane.showMessageDialog(null, "The server does not support third party");
        Debug.debug("Transfer not started: "+current, 3);
        status = STATUS_WAIT;
      }
      else{
        status = STATUS_TRANSFER;
      }
    }
    else{
      status = STATUS_TRANSFER;
    }
  }

  public void transferError(Exception e){
    exception = e;
    error = e.getMessage();
    status = STATUS_ERROR;
  }

  public void transferCompleted(){
    if(exception==null){
        Debug.debug("Transfer completed successfully", 2);
        status = STATUS_DONE;
    }
    else{
      String error = "Transfer failed: "+exception.getMessage();
      GridPilot.getClassMgr().getLogFile().addMessage(error);
      Debug.debug(error, 2);
      status = STATUS_ERROR;
    }
  }
}
