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
        status = "Wait";
      }
      else{
        status = "Transfer";
      }
    }
    else{
      status = "Transfer";
    }
  }

  public void transferError(Exception e){
    exception = e;
    error = e.getMessage();
    status = "Error";
  }

  public void transferCompleted(){
    if(exception==null){
        Debug.debug("Transfer completed successfully", 2);
        status = "Done";
    }
    else{
      String error = "Transfer failed: "+exception.getMessage();
      GridPilot.getClassMgr().getLogFile().addMessage(error);
      Debug.debug(error, 2);
      status = "Error";
    }
  }
}
