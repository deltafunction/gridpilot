package gridpilot.csplugins.fork;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;

import org.globus.util.GlobusURL;

import gridfactory.common.Debug;
import gridfactory.common.jobrun.ScriptGenerator;
import gridfactory.common.Shell;

import gridpilot.DBPluginMgr;
import gridpilot.MyJobInfo;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

/**
 * Script generator for the local shell plugin.
 *
 */
public class ForkScriptGenerator extends ScriptGenerator{
  private String workingDir = null;
  private String runtimeDirectory = null;
  private String remoteCopyCommand = null;
  private String requiredRuntimeEnv = null;
  private String [] stdoutExcludeWords = null;
  private String [] stderrExcludeWords = null;
  private String csName = null;
  private boolean ignoreBaseSystemAndVMRTEs;
  
  /**
   * Constructor
   */
  public ForkScriptGenerator(String _csName, String _workingDir, boolean _ignoreBaseSystemAndVMRTEs){
    super(GridPilot.getClassMgr().getLogFile());
    csName = _csName;
    String onWindowsStr = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "On windows");
    onWindows = onWindowsStr!=null && (onWindowsStr.equalsIgnoreCase("yes") || onWindowsStr.equalsIgnoreCase("true"));
    workingDir = _workingDir;
    runtimeDirectory = GridPilot.runtimeDir;
    remoteCopyCommand = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "Remote copy command");
    requiredRuntimeEnv = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "Required runtime environment");
    ignoreBaseSystemAndVMRTEs = _ignoreBaseSystemAndVMRTEs;
    stdoutExcludeWords = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "Stdout exclude words");
    stderrExcludeWords = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "Stderr exclude words");
  }
  
  public boolean createWrapper(Shell shell, MyJobInfo job, String fileName){
    
    String jobDefID = job.getIdentifier();
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String line; //used as temp working string
    StringBuffer buf = new StringBuffer();
    String commentStart;
    boolean notOnWindows = !shell.isLocal() && !onWindows || shell.isLocal() &&
                            (!onWindows || !MyUtil.onWindows());

    // Header
    if(!notOnWindows){
      commentStart = "REM ";
      writeLine(buf, "@echo off");
    }
    else{
      commentStart = "#";
      writeLine(buf, "#!/bin/bash");
      // write out the process name, for MySecureShell.submit to pick up
      writeLine(buf, "echo $$");
    }

    // sleep 5 seconds, to give GridPilot a chance to pick up the process id
    // for very short jobs
    writeBlock(buf, "Sleep 5 seconds before start", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    // this is to be sure to have some stdout (jobs without are considered failed)
    writeLine(buf, "echo starting...");
    if(!notOnWindows){
      writeLine(buf, "ping -n 10 127.0.0.1 >/nul");
    }
    else{
      writeLine(buf, "sleep 5");
      // Start time
      writeLine(buf, "START_TIME=`date '+%s'`");  
    }
    writeBlock(buf, "Script generated by ForkSCriptGenerator", ScriptGenerator.TYPE_SECTION, commentStart);

    // Runtime section
    writeBlock(buf, "Runtime setup", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    // For each runtime environment used, get its init text (if present) and write it out,
    // source the setup script
    String[] rtes = dbPluginMgr.getRuntimeEnvironments(jobDefID);
    if(ignoreBaseSystemAndVMRTEs){
      rtes = MyUtil.removeBaseSystemAndVM(rtes, null);
    }
    for(int i=0; i<rtes.length; ++i){
      writeBlock(buf, "runtime environment: " + rtes[i], ScriptGenerator.TYPE_COMMENT, commentStart);
      String initTxt = dbPluginMgr.getRuntimeInitText(rtes[i], csName);
      writeLine(buf, initTxt==null?"":initTxt);
      // Just try and source any setup script found by scanRTEDir or from
      // the catalog.
      // Notice that catalog RTEs are not installed on the fly by this computing system,
      // (so they must have been installed by hand...)
      // Classes that inherit may choose to do this.
      if(notOnWindows){
        writeLine(buf, ("source "+MyUtil.clearFile(runtimeDirectory)+
            "/"+rtes[i]+" 1 >& /dev/null").replaceAll("//", "/"));
        writeLine(buf, ("source "+MyUtil.clearFile(runtimeDirectory)+
            "/"+rtes[i]+"/"+"control/runtime 1 >& /dev/null").replaceAll("//", "/"));
      }
      else{
        writeLine(buf, ("call "+MyUtil.clearFile(runtimeDirectory)+
            "/"+rtes[i]+" 1 1>NUL 2>NUL").replaceAll("//", "/").replaceAll("/", "\\\\"));
        writeLine(buf, ("call "+MyUtil.clearFile(runtimeDirectory)+
            "/"+rtes[i]+"/"+"control/runtime.bat 1 1>NUL 2>NUL").replaceAll("//", "/").replaceAll("/", "\\\\"));
      }
      writeLine(buf, "");
    }

    // Input files section
    // Notice: job.getDownloadFiles() will only be non-empty if downloading failed.
    String [] inputFiles = job.getDownloadFiles();
    String [][] outputFiles = job.getUploadFiles();
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
        if(notOnWindows){
          writeLine(buf, ("source "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnv+" 1").replaceAll("//", "/"));
        }
        else{
          writeLine(buf, ("call "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnv+" 1").replaceAll("//", "/").replaceAll("/", "\\\\"));
        }
        writeLine(buf, "");
      }
      if(remoteCopyCommand!=null && remoteCopyCommand.length()>0){
        if(inputFiles!=null && inputFiles.length>0){
          writeBlock(buf, "Input files", ScriptGenerator.TYPE_SUBSECTION, commentStart);
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
    String [] formalParam = dbPluginMgr.getTransformationArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefTransPars(jobDefID);
    // write out the signature
    line = "Transformation script arguments: ";
    for(int i=0; i<formalParam.length; ++i){
      line += " " + formalParam[i];
    }
    writeBlock(buf, line, ScriptGenerator.TYPE_SUBSECTION, commentStart);
    for(int i=0; i<formalParam.length; ++i){
      try{
        actualParam[i] = MyUtil.encode(actualParam[i]);
      }
      catch(Exception e){
        Debug.debug("WARNING: parameter "+formalParam[i]+" is not set.", 2);
      }
    }
    writeLine(buf, "");
    writeBlock(buf, "transformation script call", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    String scriptSrc = dbPluginMgr.getTransformationScript(jobDefID);
    String scriptPath = MyUtil.clearFile(scriptSrc).replaceAll("\\\\", "/");
    String scriptName = scriptPath.replaceFirst(".*/([^/]+)", "$1");
    String scriptDest = "file:" + MyUtil.clearFile(workingDir) + "/" + scriptName;
    Debug.debug("Copying over transformation script "+scriptSrc+"-->"+scriptDest, 3);
    // Don't think we need this...
    /*if(MyUtil.onWindows()){
      line = line.replaceAll("/", "\\\\");
    }*/
    // Copy the script to the working directory
    // If scriptSrc script is an unqualified file name, don't try to copy it over, just ignore and assume
    // it's already on the worker node - and on the PATH.
    boolean copyScript = MyUtil.urlIsRemote(scriptSrc) || MyUtil.isLocalFileName(scriptSrc);
    try{
      if(copyScript &&
          !GridPilot.getClassMgr().getTransferControl().copyInputFile(
              MyUtil.clearFile(scriptSrc), scriptDest, shell, true, null)){
        throw new IOException("Copying transformation script to working dir failed.");
      }
    }
    catch(Exception e){
      logFile.addMessage("ERROR: transformation script could not be copied. "+
          "Cannot proceed with "+job);
      return false;
    }
    
    if(notOnWindows){
      writeLine(buf, filterStdOutErrLines());
    }

    // Running the transformation script with ./ instead of a full path is to allow this to 
    // be used by GridFactoryComputingSystem, where we don't have a shell on the worker node
    // and the full path is not known.
    line = (notOnWindows?"split \"":"")+(copyScript&&notOnWindows?"./":"") + scriptName + " " +
       MyUtil.arrayToString(actualParam) + (notOnWindows?"\" s1 | s2":"");
    // Filter off unwanted stuff from stdout and stderr
    writeLine(buf, line);
    writeLine(buf, "");
    
    // Metadata section
    /* Print the running time, size and md5sum of the output file for validation
       to pick up and write in the job DB or file catalog.
       NOTICE that we assume one output file per job. There is nothing
       in principle preventing multiple output files per job, but as it is now,
       only the first of the output files will be registered. */
    // TODO: reconsider
    // TODO: implement metadata on Windows
    if(notOnWindows){
      writeBlock(buf, "Metadata", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      writeLine(buf, "END_TIME=`date '+%s'`");
      writeLine(buf, "echo " +
          gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG +
          ": cpuSeconds = $(( END_TIME - START_TIME ))");
      for(int i=0; i<outputFiles.length; ++i){
        writeLine(buf, "echo "+
            gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
            ": outputFileBytes = `du -b "+outputFiles[i][0]+" | awk '{print $1}'`");
        writeLine(buf, "echo "+
            gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
            ": outputFileChecksum = md5:`md5sum "+outputFiles[i][0]+" | awk '{print $1}'`");
        break;
      }
    }

    try{
      shell.writeFile(workingDir+"/"+fileName, buf.toString(), false);
      // This will not work under Windows; we silently ignore...
      try{
        StringBuffer stdout = new StringBuffer();
        StringBuffer stderr = new StringBuffer();
        String workDir = workingDir;
        if(shell.isLocal()){
          workDir = MyUtil.clearTildeLocally(workDir);
        }
        shell.exec("chmod +x "+workDir+"/"+fileName, stdout, stderr);
        if(stderr!=null && stderr.length()!=0){
          logFile.addMessage("Could not set job executable. "+stderr);
          throw new FileNotFoundException(stderr.toString());
        }
        shell.exec("chmod +x "+MyUtil.clearFile(scriptDest), stdout, stderr);
        if(stderr!=null && stderr.length()!=0){
          Debug.debug("Could not set transformation executable. "+stderr, 2);
          throw new FileNotFoundException(stderr.toString());
        }
      }
      catch(Exception e){
        logFile.addInfo("Warning: NOT setting files executable. " +
            "Probably not on UNIX. "+e.getMessage());
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
  
  private String filterStdOutErrLines(){
    String lines = "split() { { { $1 2>&3 | $2 ; } 3>&1; }; }\n";
    String stdoutFilterLine = "s1(){ while read;do echo $REPLY FILTER >&2; done }\n";
    String stderrFilterLine = "s2(){ while read;do echo $REPLY FILTER >&2; done }\n";
    if(stdoutExcludeWords!=null && stdoutExcludeWords.length>0){
      String stdoutFilter = "";
      for(int i=0; i<stdoutExcludeWords.length;){
        stdoutFilter += " | grep -v "+stdoutExcludeWords[i];
      }
      stdoutFilterLine.replaceFirst("FILTER", stdoutFilter);
      lines += stdoutFilterLine;
    }
    else{
      stdoutFilterLine.replaceFirst("FILTER", "");
    }
    if(stderrExcludeWords!=null && stderrExcludeWords.length>0){
      String stderrFilter = "";
      for(int i=0; i<stderrExcludeWords.length;){
        stderrFilter += " | grep -v "+stderrExcludeWords[i];
      }
      stderrFilterLine.replaceFirst("FILTER", stderrFilter);
      lines += stderrFilterLine;
    }
    else{
      stderrFilterLine.replaceFirst("FILTER", "");
    }
    return lines;
  }

}