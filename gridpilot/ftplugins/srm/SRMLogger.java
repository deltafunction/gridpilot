package gridpilot.ftplugins.srm;

import gridpilot.Debug;
import gridpilot.GridPilot;

import org.dcache.srm.Logger;

/* *
 * This class allows having SRM use GridPilot's
 * debugging and logging system
 */
public class SRMLogger implements Logger {
  private boolean debug;
  public SRMLogger(boolean _debug){
    debug = _debug;
  }
  public void elog(String s){
    if(debug){
      Debug.debug(s, 2);
    }
  }
  public void elog(Throwable t){
    Debug.debug(t.getMessage(), 2);
    if(debug){
      t.printStackTrace(System.err);
    }
    GridPilot.getClassMgr().getLogFile().addMessage("ERROR from SRM subsystem: "+
        t.getMessage(), t);
  }
  public void log(String s){
    if(debug){
      Debug.debug(s, 2);
    }
  }
}
