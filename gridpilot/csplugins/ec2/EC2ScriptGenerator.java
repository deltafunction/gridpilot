package gridpilot.csplugins.ec2;

import java.io.IOException;

import gridfactory.common.ConfigFile;
import gridfactory.common.Shell;
import gridfactory.common.jobrun.ScriptGenerator;
import gridpilot.DBPluginMgr;
import gridpilot.GridPilot;
import gridpilot.csplugins.fork.ForkScriptGenerator;

/**
 * Script generator for the local shell plugin.
 *
 */
public class EC2ScriptGenerator extends ForkScriptGenerator{
  
  private boolean terminateHostsOnJobEnd;
  
  public EC2ScriptGenerator(String _csName, String _workingDir, boolean _ignoreBaseSystemAndVMRTEs,
      boolean _onWindows){
    this(_csName, _workingDir, _ignoreBaseSystemAndVMRTEs, _onWindows, true);
  }
  
  public EC2ScriptGenerator(String _csName, String _workingDir, boolean _ignoreBaseSystemAndVMRTEs,
      boolean _onWindows, boolean _writeRTESection){
    super(_csName, _workingDir, _ignoreBaseSystemAndVMRTEs, _onWindows, _writeRTESection);
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    String termHostsStr = configFile.getValue(_csName, "Terminate hosts on job end");
    terminateHostsOnJobEnd = termHostsStr!=null&&!termHostsStr.equals("")&&
                             (termHostsStr.equalsIgnoreCase("yes") || termHostsStr.equalsIgnoreCase("true"))?
                             true:false;
  }
  
  protected void writeExecutableSection(String jobDefID, DBPluginMgr dbPluginMgr, String commentStart,
      StringBuffer buf, boolean onWindows, Shell shell,
      String scriptDest, String scriptSrc, String scriptName) throws IOException {
    super.writeExecutableSection(jobDefID, dbPluginMgr, commentStart, buf, onWindows,
        shell, scriptDest, scriptSrc, scriptName);
    if(terminateHostsOnJobEnd){
      writeBlock(buf, "shutdown after job completion", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      writeLine(buf, "halt || sudo shutdown -h now");
    }
  }

}