package gridpilot.csplugins.fork;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.globus.util.GlobusURL;

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
  private HashMap remoteCopyCommands = null;
  private String [] requiredRuntimeEnvs = null;
  private String [] stdoutExcludeWords = null;
  private String [] stderrExcludeWords = null;
  private String csName = null;
  private boolean ignoreBaseSystemAndVMRTEs;
  
  /**
   * Constructor
   */
  public ForkScriptGenerator(String _csName, String _workingDir,boolean _ignoreBaseSystemAndVMRTEs){
    super(GridPilot.getClassMgr().getLogFile());
    csName = _csName;
    String onWindowsStr = GridPilot.getClassMgr().getConfigFile().getValue(
        csName, "On windows");
    onWindows = onWindowsStr!=null && (onWindowsStr.equalsIgnoreCase("yes") ||
        onWindowsStr.equalsIgnoreCase("true"));
    String [] rtCpCmds = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "Remote copy commands");
    if(rtCpCmds!=null && rtCpCmds.length>1){
      remoteCopyCommands = new HashMap();
      for(int i=0; i<rtCpCmds.length/2; ++i){
        remoteCopyCommands.put(rtCpCmds[2*i], rtCpCmds[2*i+1]);
      }
    }
    workingDir = _workingDir;
    runtimeDirectory = GridPilot.runtimeDir;
    requiredRuntimeEnvs = GridPilot.getClassMgr().getConfigFile().getValues(
        csName, "Required runtime environment");
    ignoreBaseSystemAndVMRTEs = _ignoreBaseSystemAndVMRTEs;
    try{
       String stdoutExW = GridPilot.getClassMgr().getConfigFile().getValue(
           csName, "Stdout exclude words");
      stdoutExcludeWords = splitPhrases(stdoutExW);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    try{
      String stderrExW = GridPilot.getClassMgr().getConfigFile().getValue(
          csName, "Stderr exclude words");
     stderrExcludeWords = splitPhrases(stderrExW);
    }
    catch(Exception e){
      e.printStackTrace();
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
    boolean notOnWindows = !shell.isLocal() && !onWindows || shell.isLocal() &&
                            (!onWindows || !MyUtil.onWindows());
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

    String scriptSrc = dbPluginMgr.getTransformationScript(jobDefID);
    String scriptPath = MyUtil.clearFile(scriptSrc).replaceAll("\\\\", "/");
    String scriptName = scriptPath.replaceFirst(".*/([^/]+)", "$1");
    String scriptDest = "file:" + MyUtil.clearFile(workingDir) + "/" + scriptName;

    // Header
    writeHeader(notOnWindows, buf, commentStart);

    // Runtime section
    writeRuntimeSection(commentStart, buf, dbPluginMgr, jobDefID, notOnWindows);
    
    // Input files section
    try{
      writeInputFilesSection(job, buf, commentStart);
    }
    catch(IOException e){
      logFile.addMessage("Problem with input files. Cannot proceed with "+job, e);
      return false;
    }

    // Output files section
    try{
      writeOutputFilesSection(job, buf, commentStart);
    }
    catch(IOException e){
      logFile.addMessage("Problem with output files. Cannot proceed with "+job, e);
      return false;
    }

    // Transformation script call section
    try{
      writeTransformationSection(jobDefID, dbPluginMgr, commentStart, buf, notOnWindows,
          shell, scriptDest, scriptSrc, scriptName);
    }
    catch(Exception e){
      logFile.addMessage("ERROR: transformation script could not be copied. "+
          "Cannot proceed with "+job, e);
      return false;
    }
    
    writeMetadataSection(buf, notOnWindows, commentStart, job);

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
    catch(Exception e){
      logFile.addMessage("Could not create job wrapper. ", e);
      return false;
    }
    return true;
  }
  
  private void writeMetadataSection(StringBuffer buf, boolean notOnWindows,
      String commentStart, JobInfo job) {
    // Metadata section
    /* Print the running time, size and md5sum of the output file for validation
       to pick up and write in the job DB or file catalog.
       NOTICE that we assume one output file per job. There is nothing
       in principle preventing multiple output files per job, but as it is now,
       only the first of the output files will be registered. */
    // TODO: reconsider
    // TODO: implement metadata on Windows
    writeLine(buf, "");
    if(notOnWindows){
      writeBlock(buf, "Metadata", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      writeLine(buf, "END_TIME=`date '+%s'`");
      writeLine(buf, "echo " +
          gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG +
          ": cpuSeconds = $(( END_TIME - START_TIME ))");
      String [] outputFiles = job.getOutputFileNames();
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

  private void writeTransformationSection(String jobDefID, DBPluginMgr dbPluginMgr, String commentStart,
      StringBuffer buf, boolean notOnWindows, Shell shell,
      String scriptDest, String scriptSrc, String scriptName) throws IOException {
    String [] formalParam = dbPluginMgr.getTransformationArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefTransPars(jobDefID);
    String line; //used as temp working string
    // write out the signature
    line = "Transformation script arguments: ";
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
    writeBlock(buf, "transformation script call", ScriptGenerator.TYPE_SUBSECTION, commentStart);
    Debug.debug("Copying over transformation script "+scriptSrc+"-->"+scriptDest, 3);
    // Copy the script to the working directory
    // If scriptSrc script is an unqualified file name, don't try to copy it over, just ignore and assume
    // it's already on the worker node - and on the PATH.
    boolean copyScript = MyUtil.urlIsRemote(scriptSrc) || MyUtil.isLocalFileName(scriptSrc);
    if(copyScript &&
        !GridPilot.getClassMgr().getTransferControl().copyInputFile(
            MyUtil.clearFile(scriptSrc), scriptDest, shell, true, null)){
      throw new IOException("Copying transformation script to working dir failed.");
    }
    
    if(notOnWindows){
      // Shell utils to filter off unwanted stuff from stdout and stderr
      writeLine(buf, filterStdOutErrLines());
      // Running the transformation script with ./ instead of a full path is to allow this to 
      // be used by GridFactoryComputingSystem, where we don't have a shell on the worker node
      // and the full path is not known.
      line = "split \""+(copyScript?"./":"")+ scriptName + " " +
             MyUtil.arrayToString(actualParam) + "\" | s1";
    }
    else{
      line = scriptName+ " " + MyUtil.arrayToString(actualParam);
    }
  }

  private void writeOutputFilesSection(MyJobInfo job, StringBuffer buf,
      String commentStart) throws IOException {
    String [][] uploadFiles = job.getUploadFiles();
    if(uploadFiles!=null && uploadFiles.length>0){
      writeBlock(buf, "Input files", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      String protocol = null;
      for(int i=0; i<uploadFiles.length; ++i){
        protocol = uploadFiles[i][1].replaceFirst("^(\\w+):.*$", "$1");
        writeLine(buf, remoteCopyCommands.get(protocol)+" file:///`pwd`/"+uploadFiles[i][0]+" "+uploadFiles[i][1]);
      }
      writeLine(buf, "");
    }
  }

  private void writeInputFilesSection(JobInfo job, StringBuffer buf,
      String commentStart) throws IOException {
    String [] downloadFiles = job.getDownloadFiles();
    if(downloadFiles!=null && downloadFiles.length>0){
      writeBlock(buf, "Input files", ScriptGenerator.TYPE_SUBSECTION, commentStart);
      String name = null;
      String protocol = null;
      for(int i=0; i<downloadFiles.length; ++i){
        try{
          name = new File((new GlobusURL(downloadFiles[i])).getPath()).getName();
          protocol = downloadFiles[i].replaceFirst("^(\\w+):.*$", "$1");
        }
        catch(MalformedURLException e){
          e.printStackTrace();
          logFile.addMessage("ERROR: could not get input file "+downloadFiles[i], e);
          continue;
        }
        writeLine(buf, remoteCopyCommands.get(protocol)+" "+downloadFiles[i]+" file:///`pwd`/"+name);
      }
      writeLine(buf, "");
    }
  }

  private void writeRuntimeSection(String commentStart, StringBuffer buf, DBPluginMgr dbPluginMgr,
      String jobDefID, boolean notOnWindows) {
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
        if(notOnWindows){
          writeLine(buf, ("source "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnvs[i]+" 1").replaceAll("//", "/"));
        }
        else{
          writeLine(buf, ("call "+MyUtil.clearFile(runtimeDirectory)+
              "/"+requiredRuntimeEnvs[i]+" 1").replaceAll("//", "/").replaceAll("/", "\\\\"));
        }
        writeLine(buf, "");
      }
    }
  }

  private void writeHeader(boolean notOnWindows, StringBuffer buf, String commentStart) {
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
    if(stdoutExcludeWords!=null && stdoutExcludeWords.length>0){
      String stdoutFilter = "";
      for(int i=0; i<stdoutExcludeWords.length; ++i){
        stdoutFilter += " | grep -v "+stdoutExcludeWords[i];
      }
      stdoutFilterLine = stdoutFilterLine.replaceFirst("FILTER", stdoutFilter);
      lines += stdoutFilterLine;
    }
    else{
      stdoutFilterLine.replaceFirst("FILTER", "");
    }
    if(stderrExcludeWords!=null && stderrExcludeWords.length>0){
      String stderrFilter = "";
      for(int i=0; i<stderrExcludeWords.length; ++i){
        stderrFilter += " | grep -v "+stderrExcludeWords[i];
      }
      stderrFilterLine = stderrFilterLine.replaceFirst("FILTER", stderrFilter);
      lines += stderrFilterLine;
    }
    else{
      stderrFilterLine.replaceFirst("FILTER", "");
    }
    lines += "split(){ { $1 2>&1 1>&3 | s2 1>&2 ; } 3>&1 ;}\n";
    Debug.debug("Filtering stdout/stderr with "+lines, 2);
    return lines;
  }

}