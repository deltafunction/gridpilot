package gridpilot.csplugins.fork;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;

import org.globus.util.GlobusURL;

import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.GridPilot;
import gridpilot.ScriptGenerator;
import gridpilot.ShellMgr;
import gridpilot.TransferControl;
import gridpilot.Util;

/**
 * Script generator for the local shell plugin.
 *
 */
public class ForkScriptGenerator extends ScriptGenerator{
  private String workingDir = null;
  private String runtimeDirectory = null;
  private String remoteCopyCommand = null;
  private String requiredRuntimeEnv = null;
  
  /**
   * Constructor
   */
  public ForkScriptGenerator(String csName, String _workingDir){
    super(csName);
    workingDir = _workingDir;
    runtimeDirectory = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "runtime directory");
    remoteCopyCommand = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "remote copy command");
    requiredRuntimeEnv = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "required runtime environment");
  }
  
  public boolean createWrapper(ShellMgr shellMgr, JobInfo job, String fileName){
    
    String jobDefID = job.getJobDefId();
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String line; //used as temp working string
    StringBuffer buf = new StringBuffer();
    String commentStart = "REM";

    // Header
    if(!shellMgr.isLocal() || !System.getProperty("os.name").toLowerCase().startsWith("windows")){
      commentStart = "#";
      writeLine(buf, "#!/bin/sh");
      // write out the process name, for SecureShellMgr.submit to pick up
      writeLine(buf, "echo $$");
    }
    
    // sleep 5 seconds, to give GridPilot a chance to pick up the process id
    // for very short jobs
    writeBloc(buf, "Sleep 5 seconds before start", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    // this is to be sure to have some stdout (jobs without are considered failed)
    writeLine(buf, "echo starting...");
    if(shellMgr.isLocal() && System.getProperty("os.name").toLowerCase().startsWith("windows")){
      writeLine(buf, "ping -n 10 127.0.0.1 >/nul");
    }
    else{
      writeLine(buf, "sleep 5");
    }
    writeBloc(buf, "Script generated by ForkSCriptGenerator", ScriptGenerator.TYPE_SECTION, commentStart);

    // Runtime section
    writeBloc(buf, "Runtime setup", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    // For each runtime environment used, get its init text (if present) and write it out,
    // source the setup script
    String[] rtes = dbPluginMgr.getRuntimeEnvironments(jobDefID);
    for(int i=0; i<rtes.length; ++i){
      writeBloc(buf, "runtime environment: " + rtes[i], ScriptGenerator.TYPE_COMMENT, commentStart);
      String initTxt = dbPluginMgr.getRuntimeInitText(rtes[i], csName).toString();
      writeLine(buf, initTxt); 
      writeLine(buf, ("source "+Util.clearFile(runtimeDirectory)+
          "/"+rtes[i]).replaceAll("//", "/"));
      writeLine(buf, "");
    }

    // Input files section
    // Notice: job.getDownloadFiles() will only be non-empty if downloading failed.
    String [] inputFiles = job.getDownloadFiles();
    //String [][] outputFiles = job.getUploadFiles();
    if(inputFiles!=null && inputFiles.length>0 /*|| outputFiles!=null && outputFiles.length>0*/){
      if(requiredRuntimeEnv!=null && requiredRuntimeEnv.length()>0){
        Debug.debug("Adding sourcing of required RTEs: "+requiredRuntimeEnv, 2);
        // requiredRuntimeEnv is only needed to get input files from
        // remote sources or copy ouput files to final destinations
        String initTxt = null;
        try{
          initTxt = dbPluginMgr.getRuntimeInitText(requiredRuntimeEnv, csName).toString();
        }
        catch(Exception e){
          e.printStackTrace();
          logFile.addMessage("WARNING: could not find required runtime environment "+requiredRuntimeEnv, e);
        }
        writeLine(buf, initTxt);
        writeLine(buf, ("source "+Util.clearFile(runtimeDirectory)+
            "/"+requiredRuntimeEnv).replaceAll("//", "/"));
        writeLine(buf, "");
      }
      if(remoteCopyCommand!=null && remoteCopyCommand.length()>0){
        if(inputFiles!=null && inputFiles.length>0){
          writeBloc(buf, "Input files", ScriptGenerator.TYPE_SUBSECTION, commentStart);
          String name = null;
          for(int i=0; i<inputFiles.length; ++i){
            try{
              name = new File((new GlobusURL(inputFiles[i])).getPath()).getName();
            }
            catch(MalformedURLException e){
              e.printStackTrace();
              logFile.addMessage("ERROR: could not get input file "+inputFiles[i], e);
              continue;
            }
            writeLine(buf, remoteCopyCommand+" "+inputFiles[i]+" file:///`pwd`/"+name);
          }
          writeLine(buf, "");
        }
      }
      else{
        logFile.addMessage("ERROR: remote input files needed and no remote copy command defined. "+
            "Cannot proceed with "+job);
        return false;
      }
    }

    // transformation script call section
    String [] formalParam =
        dbPluginMgr.getTransformationArguments(jobDefID);
    String [] actualParam =
        dbPluginMgr.getJobDefTransPars(jobDefID);
    // write out the signature
    line = "Transformation script arguments: ";
    for(int i=0; i<formalParam.length; ++i){
      line += " " + formalParam[i];
    }
    writeBloc(buf, line, ScriptGenerator.TYPE_SUBSECTION, commentStart);
    for(int i=0; i<formalParam.length; ++i){
      try{
        actualParam[i] = Util.encode(actualParam[i]);
      }
      catch(Exception e){
        Debug.debug("WARNING: parameter "+formalParam[i]+" is not set.", 2);
      }
    }
    writeLine(buf, "");
    writeBloc(buf, "transformation script call", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    String scriptSrc = dbPluginMgr.getTransformationScript(jobDefID);
    String scriptDest = Util.clearFile(scriptSrc);
    scriptDest = scriptDest.replaceAll("\\\\", "/");
    scriptDest = workingDir + scriptDest.replaceFirst(".*(/[^/]+)", "$1");
    // Don't think we need this...
    /*if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
      line = line.replaceAll("/", "\\\\");
    }*/
    // Copy the script to the working directory
    try{
      TransferControl.copyInputFile(scriptSrc, scriptDest, shellMgr, null, logFile);
    }
    catch(Exception e){
      logFile.addMessage("ERROR: transformation script could not be copied. "+
          "Cannot proceed with "+job);
      return false;
    }
    line = Util.clearFile(scriptDest) + " " + Util.arrayToString(actualParam);
    writeLine(buf, line);
    writeLine(buf, "");
    
    // Output files section
    // This should not be necessary any longer.
    /*if(outputFiles!=null && outputFiles.length>0){
      writeBloc(buf, "Output files", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      for(int i=0; i<outputFiles.length; ++i){
        writeLine(buf, remoteCopyCommand+" "+outputFiles[i][0]+" "+outputFiles[i][1]);
      }
      writeLine(buf, "");
    }*/

    try{
      shellMgr.writeFile(workingDir+"/"+fileName, buf.toString(), false);
      // This will not work under Windows; we silently ignore...
      try{
        StringBuffer stdout = new StringBuffer();
        StringBuffer stderr = new StringBuffer();
        String workDir = workingDir;
        if(shellMgr.isLocal()){
          workDir = Util.clearTildeLocally(workDir);
        }
        shellMgr.exec("chmod +x "+workDir+"/"+fileName, stdout, stderr);
        if(stderr!=null && stderr.length()!=0){
          logFile.addMessage("Could not set job executable. "+stderr);
          throw new FileNotFoundException(stderr.toString());
        }
        shellMgr.exec("chmod +x "+Util.clearTildeLocally(Util.clearFile(scriptDest)), stdout, stderr);
        if(stderr!=null && stderr.length()!=0){
          logFile.addMessage("Could not set transformation executable. "+stderr);
          throw new FileNotFoundException(stderr.toString());
        }
      }
      catch(Exception e){
        Debug.debug("Warning: NOT setting files executable. " +
            "Probably not on UNIX. "+e.getMessage(), 2);
      }
    }
    catch(FileNotFoundException fnfe){
      logFile.addMessage("Could not create job wrapper. ", fnfe);
      return false;
    }
    catch(IOException ioe){
      ioe.printStackTrace();
      return false;
    }
    catch(Exception ioe){
      ioe.printStackTrace();
      return false;
    }
    return true;
  }
}