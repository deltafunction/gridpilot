package gridpilot.ftplugins.sss;

import gridpilot.GridPilot;

import org.jets3t.service.multithread.CreateObjectsEvent;
import org.jets3t.service.multithread.DownloadObjectsEvent;
import org.jets3t.service.multithread.S3ServiceEventAdaptor;
import org.jets3t.service.multithread.ServiceEvent;
import org.jets3t.service.multithread.ThreadWatcher;

public class MyS3ServiceEventListener extends S3ServiceEventAdaptor{
  
  private final String STATUS_WAIT = "Wait";
  private final String STATUS_TRANSFER = "Transfer";
  private final String STATUS_DONE = "Done";
  private final String STATUS_ERROR = "Error";

  private ThreadWatcher watcher = null;
  private String status = STATUS_WAIT;
  
  public ThreadWatcher getThreadWatcher(){
    return watcher;
  }
  
  public String getStatus(){
    return status;
  }
  
  public void s3ServiceEventPerformed(DownloadObjectsEvent event) {
    super.s3ServiceEventPerformed(event);
    if(ServiceEvent.EVENT_STARTED==event.getEventCode()){    
      watcher = event.getThreadWatcher();
      status = STATUS_TRANSFER;
    }  
    else if (ServiceEvent.EVENT_IN_PROGRESS==event.getEventCode()){
      watcher = event.getThreadWatcher();  
      status = STATUS_TRANSFER;
    }
    else if (ServiceEvent.EVENT_COMPLETED==event.getEventCode()){
      status = STATUS_DONE;
    }
    else if (ServiceEvent.EVENT_CANCELLED==event.getEventCode()){
      status = STATUS_ERROR;
    }
    else if (ServiceEvent.EVENT_ERROR==event.getEventCode()){
      String message = "Unable to download object(s)";
      status = STATUS_ERROR;
      GridPilot.getClassMgr().getLogFile().addMessage(message, event.getErrorCause());
    }
  }
  
  public void s3ServiceEventPerformed(final CreateObjectsEvent event){
    super.s3ServiceEventPerformed(event);
    if(ServiceEvent.EVENT_STARTED==event.getEventCode()){    
      watcher = event.getThreadWatcher();
      status = STATUS_TRANSFER;
    }  
    else if (ServiceEvent.EVENT_IN_PROGRESS==event.getEventCode()){
      watcher = event.getThreadWatcher();  
      status = STATUS_TRANSFER;
    }
    else if (ServiceEvent.EVENT_COMPLETED==event.getEventCode()){
      status = STATUS_DONE;
    }
    else if (ServiceEvent.EVENT_CANCELLED==event.getEventCode()){
      status = STATUS_ERROR;
    }
    else if (ServiceEvent.EVENT_ERROR==event.getEventCode()){
      String message = "Unable to upload object(s)";
      status = STATUS_ERROR;
      GridPilot.getClassMgr().getLogFile().addMessage(message, event.getErrorCause());
    }
  }

}
