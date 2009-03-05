package gridpilot.csplugins.ec2;

import gridfactory.common.Shell;
import gridpilot.MyJobInfo;
import gridpilot.csplugins.fork.ForkScriptGenerator;

public class EC2ScriptGenerator extends ForkScriptGenerator {

  public EC2ScriptGenerator(String _csName, String _workingDir) {
    super(_csName, _workingDir);
    // TODO Auto-generated constructor stub
  }

  public boolean createWrapper(Shell shell, MyJobInfo job, String fileName){
    // TODO
    return super.createWrapper(shell, job, fileName);
  }
  
}
