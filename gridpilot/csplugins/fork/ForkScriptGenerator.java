package gridpilot.csplugins.fork;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.JobInfo;
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
  private HashMap<String, String> remoteCopyCommands = null;
  private String [] requiredRuntimeEnvs = null;
  private String [] stdoutExcludeWords = null;
  private String [] stderrExcludeWords = null;
  private String csName = null;
  private boolean ignoreBaseSystemAndVMRTEs;
  private boolean writeRTESection = true;
  private long submitTimeout;
  
  public ForkScriptGenerator(String _csName, String _workingDir, boolean _ignoreBaseSystemAndVMRTEs,
      boolean _onWindows){
    this(_csName, _workingDir, _ignoreBaseSystemAndVMRTEs, _onWindows, true);
  }
  
  public ForkScriptGenerator(String _csName, String _workingDir, boolean _ignoreBaseSystemAndVMRTEs,
      boolean _onWindows, boolean _writeRTESection){
    super(GridPilot.getClassMgr().getLogFile(), _onWindows);
    writeRTESection = _writeRTESection;
    csName = _csName;
    ConfigFile configFile = GridPilot.getClassMgr().getConfigFile();
    String [] rtCpCmds = configFile.getValues(csName, "Remote copy commands");
    if(rtCpCmds!=null && rtCpCmds.length>1){
      remoteCopyCommands = new HashMap<String, String>();
      for(int i=0; i<rtCpCmds.length/2; ++i){
        remoteCopyCommands.put(rtCpCmds[2*i], rtCpCmds[2*i+1]);
      }
    }
    workingDir = _workingDir;
    runtimeDirectory = GridPilot.RUNTIME_DIR;
    requiredRuntimeEnvs = configFile.getValues(csName, "Required runtime environments");
    ignoreBaseSystemAndVMRTEs = _ignoreBaseSystemAndVMRTEs;
    try{
      String stdoutExW = configFile.getValue(csName, "Stdout exclude words");
      stdoutExcludeWords = splitPhrases(stdoutExW);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      String stderrExW = configFile.getValue(csName, "Stderr exclude words");
      stderrExcludeWords = splitPhrases(stderrExW);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    Debug.debug("stdoutExcludeWords: "+MyUtil.arrayToString(stdoutExcludeWords), 2);
    Debug.debug("stderrExcludeWords: "+MyUtil.arrayToString(stderrExcludeWords), 2);
    submitTimeout = 700L;
    String st = configFile.getValue(GridPilot.TOP_CONFIG_SECTION, "submit timeout");
    if(st!=null && !st.equals("")){
      submitTimeout = Long.parseLong(st);
    }
  }
  
  private static String [] splitPhrases(String _phrases) throws Exception{
    Vector<String> phrasesVec = new Vector<String>();
    String pattern = "([^']*)('[^']+')(.*)";
    String phrases = _phrases;
    String phrase;
    while(true){
      if(!phrases.matches(pattern)){
        break;
      }
      phrase = phrases.replaceFirst(pattern, "$2");
      phrases = phrases.replaceFirst(pattern, "$1$3");
      phrasesVec.add(phrase);
    }
    if(phrases.indexOf("'")>=0){
      throw new Exception("Cannot parse "+_phrases);
    }
    String [] words = MyUtil.split(phrases.trim());
    if(words!=null && words.length>0){
      Collections.addAll(phrasesVec, words);
    }
    return phrasesVec.toArray(new String[phrasesVec.size()]);
  }
  
  /**
   * Write the wrapper script.
   * @param shell
   * @param job
   * @param fileName
   * @return
   */
  public boolean createWrapper(Shell shell, MyJobInfo job, String fileName){
    
    String jobDefID = job.getIdentifier();
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    StringBuffer buf = new StringBuffer();
    String commentStart;

    if(onWindows){
      commentStart = "REM ";
      writeLine(buf, "@echo off");
    }
    else{
      commentStart = "#";
      writeLine(buf, "#!/bin/bash");
      // write out the process name, for MySecureShell.submit to pick up
      writeLine(buf, "echo $$");
    }

    String scriptSrc = dbPluginMgr.getExecutableFile(jobDefID);
    String scriptPath = MyUtil.clearFile(scriptSrc).replaceAll("\\\\", "/");
    String scriptName = scriptPath.replaceFirst(".*/([^/]+)", "$1");
    String scriptDest = "file:" + MyUtil.clearFile(workingDir) + "/" + scriptName;
    
    if(scriptName.trim().equals("")){
      logFile.addMessage("ERROR: executable script undefined. "+
          "Cannot proceed with "+job);
      return false;
    }

    // Header
    writeHeader(onWindows, buf, commentStart);

    // Runtime section
    if(writeRTESection){
      writeRuntimeSection(commentStart, buf, dbPluginMgr, jobDefID, onWindows);
    }
    
    // Input files section
    try{
      writeBlock(buf, "Input files", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      if(onWindows){
        writeLine(buf, "echo Download start: %date% %time%");
      }
      else{
        writeLine(buf, "echo Download start: `date`");
      }
      MyUtil.writeInputFilesSection(job, buf, commentStart, remoteCopyCommands);
      if(onWindows){
        writeLine(buf, "echo Download end: %date% %time%");
      }
      else{
        writeLine(buf, "echo Download end: `date`");
      }
    }
    catch(IOException e){
      logFile.addMessage("Problem with input files. Cannot proceed with "+job, e);
      return false;
    }

    // Executable script call section
    try{
      writeExecutableSection(jobDefID, dbPluginMgr, commentStart, buf, onWindows,
          shell, scriptDest, scriptSrc, scriptName);
    }
    catch(Exception e){
      logFile.addMessage("ERROR: executable script could not be copied. "+
          "Cannot proceed with "+job, e);
      return false;
    }
    
    writeMetadataSection(buf, onWindows, dbPluginMgr, commentStart, job);

    // Output files section
    try{
      writeBlock(buf, "Output files", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      MyUtil.writeOutputFilesSection(job, buf, commentStart, remoteCopyCommands);
    }
    catch(IOException e){
      logFile.addMessage("Problem with output files. Cannot proceed with "+job, e);
      return false;
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
          throw new FileNotFoundException(stderr.toString());
        }
        // TODO: this does not appear to be necessary...
        shell.exec("chmod +x "+MyUtil.clearFile(scriptDest), stdout, stderr);
        /*if(stderr!=null && stderr.length()!=0){
          Debug.debug("Could not set executable permission. "+stderr, 2);
          throw new FileNotFoundException(stderr.toString());
        }*/
      }
      catch(Exception e){
        logFile.addInfo("Warning: NOT setting files executable. " +
            "Probably not on UNIX. "+e.getMessage());
      }
    }
    catch(Exception e){
      logFile.addMessage("Could not create job wrapper. ", e);
      return false;
    }
    return true;
  }
  
  private void writeMetadataSection(StringBuffer buf, boolean onWindows,
      DBPluginMgr dbPluginMgr, String commentStart, JobInfo job) {
    // Metadata section
    /* Print the running time, size and md5sum of the output file for validation
       to pick up and write in the job DB or file catalog.
       NOTICE that we assume one output file per job. There is nothing
       in principle preventing multiple output files per job, but as it is now,
       only the first of the output files will be registered. */
    // TODO: reconsider
    // TODO: implement metadata on Windows
    writeLine(buf, "");
    if(!onWindows){
      writeBlock(buf, "Metadata", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      writeLine(buf, "END_TIME=`date '+%s'`");
      writeLine(buf, "echo " +
          gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG +
          ": cpuSeconds = $(( END_TIME - START_TIME ))");
      String [] outputFiles = dbPluginMgr.getOutputFiles(job.getIdentifier());
      for(int i=0; i<outputFiles.length; ++i){
        writeLine(buf, "echo "+
            gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
            ": outputFileBytes = `du -b "+outputFiles[i]+" | awk '{print $1}'`");
        writeLine(buf, "echo "+
            gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
            ": outputFileChecksum = md5:`md5sum "+outputFiles[i]+" | awk '{print $1}'`");
        break;
      }
    }
  }

  protected void writeExecutableSection(String jobDefID, DBPluginMgr dbPluginMgr, String commentStart,
      StringBuffer buf, boolean onWindows, Shell shell,
      String scriptDest, String scriptSrc, String scriptName) throws IOException {
    String [] formalParam = dbPluginMgr.getExecutableArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefExecutableParameters(jobDefID);
    String line; //used as temp working string
    // write out the signature
    line = "Executable script arguments: ";
    for(int i=0; i<formalParam.length; ++i){
      line += " " + formalParam[i];
    }
    writeBlock(buf, line, ScriptGenerator.TYPE_SUBSECTION, commentStart);
    for(int i=0; i<formalParam.length; ++i){
      try{
        actualParam[i] = encode(actualParam[i]);
      }
      catch(Exception e){
        Debug.debug("WARNING: parameter "+formalParam[i]+" is not set.", 2);
      }
    }
    writeLine(buf, "");
    writeBlock(buf, "executable script call", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    Debug.debug("Copying over executable script "+scriptSrc+"-->"+scriptDest, 3);
    // Copy the script to the working directory
    // If scriptSrc script is an unqualified file name, don't try to copy it over, just ignore and assume
    // it's already on the worker node - and on the PATH.
    boolean copyScript = MyUtil.urlIsRemote(scriptSrc) || MyUtil.isLocalFileName(scriptSrc);
    if(copyScript &&
        !GridPilot.getClassMgr().getTransferStatusUpdateControl().copyInputFile(
            MyUtil.clearFile(scriptSrc), scriptDest, shell, true, submitTimeout, null)){
      throw new IOException("Copying executable script to working dir failed.");
    }
    
    if(!onWindows){
      // Shell utils to filter off unwanted stuff from stdout and stderr
      writeLine(buf, filterStdOutErrLines());
      // Running the executable script with ./ instead of a full path is to allow this to 
      // be used by GridFactoryComputingSystem, where we don't have a shell on the worker node
      // and the full path is not known.
      line = "split \""+(copyScript?"./":"")+ scriptName + " " +
             MyUtil.arrayToString(actualParam) + "\" | s1";
    }
    else{
      line = scriptName+ " " + MyUtil.arrayToString(actualParam);
    }
    writeLine(buf, line);
  }

  private void writeRuntimeSection(String commentStart, StringBuffer buf, DBPluginMgr dbPluginMgr,
      String jobDefID, boolean onWindows) {
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
      if(!onWindows){
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
    }
    writeBlock(buf, "Runtime setup", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    // Source any required runtime environments.
    // ### Doing this after the job RTEs breaks ATLAS on GridFactory for some jobs - GridCopy has to be
    //     sourced before ATLAS. Let's just hope other RTEs are not that fragile...
    // ### Well, guess what: sourcing  ATLAS after GridCopy break GridCopy...
    if(requiredRuntimeEnvs!=null && requiredRuntimeEnvs.length>0){
      Debug.debug("Adding sourcing of required RTEs: "+MyUtil.arrayToString(requiredRuntimeEnvs), 2);
      // requiredRuntimeEnv is only needed to get input files from
      // remote sources or copy ouput files to final destinations
      String initTxt = null;
      for(int i=0; i<requiredRuntimeEnvs.length; ++i){
        try{
          initTxt = dbPluginMgr.getRuntimeInitText(requiredRuntimeEnvs[i], csName).toString();
        }
        catch(Exception e){
          e.printStackTrace();
          logFile.addMessage("WARNING: could not find required runtime environment "+requiredRuntimeEnvs[i], e);
        }
        writeLine(buf, initTxt);
        if(!onWindows){
          writeLine(buf, ("source "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnvs[i]+" 1 >& /dev/null").replaceAll("//", "/"));
          writeLine(buf, ("source "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnvs[i]+"/control/runtime 1 >& /dev/null").replaceAll("//", "/"));
        }
        else{
          writeLine(buf, ("call "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnvs[i]+" 1 1>NUL 2>NUL").replaceAll("//", "/").replaceAll("/", "\\\\"));
          writeLine(buf, ("call "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnvs[i]+"/control/runtime.bat 1 1>NUL 2>NUL").replaceAll("//", "/").replaceAll("/", "\\\\"));
        }
      }
    }
    writeLine(buf, "");
  }

  private void writeHeader(boolean onWindows, StringBuffer buf, String commentStart) {
    // sleep 2 seconds, to give GridPilot a chance to pick up the process id
    // for very short jobs
    writeBlock(buf, "Sleep 2 seconds before start", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    // this is to be sure to have some stdout (jobs without are considered failed)
    writeLine(buf, "echo starting...");
    if(!!onWindows){
      writeLine(buf, "ping -n 2 127.0.0.1 >/nul");
    }
    else{
      writeLine(buf, "sleep 2");
      // Start time
      writeLine(buf, "START_TIME=`date '+%s'`");  
    }
    writeBlock(buf, "Script generated by ForkSCriptGenerator", ScriptGenerator.TYPE_SECTION, commentStart);
  }

  private static String encode(String s){
    // If s contains spaces, enclose in single quotes.
    // Escape all quotes/dollars.
    // Tried to get this working using replaceAll without luck.
    String tmp = "";
    for(int i=0; i<s.length(); i++){
      if(s.charAt(i)=='"'){
        tmp = tmp + '\\';
        tmp = tmp + '"';
      }
      else if(s.charAt(i)=='$'){
        tmp = tmp + '\\';
        tmp = tmp + '$';
      }
      else {
        tmp = tmp + s.charAt(i);
      }
    }
    tmp = tmp.trim();
    if(tmp.indexOf(" ")>0){
      tmp = "'" + tmp + "'";
    }
    return tmp;
  }
  
  private String filterStdOutErrLines(){
    Debug.debug("Creating stdout/stderr filters", 3);
    String lines = "";
    String stdoutFilterLine = "s1(){ while read;do echo $REPLY; done FILTER ; }\n";
    String stderrFilterLine = "s2(){ while read;do echo $REPLY; done FILTER ; }\n";
    Debug.debug("stdoutExcludeWords: "+MyUtil.arrayToString(stdoutExcludeWords), 3);
    Debug.debug("stderrExcludeWords: "+MyUtil.arrayToString(stderrExcludeWords), 3);
    if(stdoutExcludeWords!=null && stdoutExcludeWords.length>0){
      String stdoutFilter = "";
      for(int i=0; i<stdoutExcludeWords.length; ++i){
        stdoutFilter += " | grep -v "+stdoutExcludeWords[i];
      }
      stdoutFilterLine = stdoutFilterLine.replaceFirst("FILTER", stdoutFilter);
    }
    else{
      stdoutFilterLine = stdoutFilterLine.replaceFirst("FILTER", "");
    }
    lines += stdoutFilterLine;
    if(stderrExcludeWords!=null && stderrExcludeWords.length>0){
      String stderrFilter = "";
      for(int i=0; i<stderrExcludeWords.length; ++i){
        stderrFilter += " | grep -v "+stderrExcludeWords[i];
      }
      stderrFilterLine = stderrFilterLine.replaceFirst("FILTER", stderrFilter);
    }
    else{
      stderrFilterLine = stderrFilterLine.replaceFirst("FILTER", "");
    }
    lines += stderrFilterLine;
    lines += "split(){ { $1 2>&1 1>&3 | s2 1>&2 ; } 3>&1 ;}\n";
    Debug.debug("Filtering stdout/stderr with "+lines, 2);
    return lines;
  }

}