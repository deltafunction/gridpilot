package gridpilot.csplugins.ng;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;

import gridfactory.common.ConfigFile;
import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.jobrun.ScriptGenerator;
import gridpilot.DBPluginMgr;
import gridpilot.MyComputingSystem;
import gridpilot.MyJobInfo;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

/**
 * Script generator for the NorduGrid plugin. <br>
 * <p><a href="NGScriptGenerator.java.html">see sources</a>
 */

public class NGScriptGenerator extends ScriptGenerator{

  private String cpuTime = null;
  private String memory = null;
  private String reRun = null;
  private String extraXrsl = null;
  private String csName = null;
  private ConfigFile configFile = null;
  private HashMap<String, String> remoteCopyCommands = null;
  
  public NGScriptGenerator(String _csName){
    super(GridPilot.getClassMgr().getLogFile(), false);
    configFile = GridPilot.getClassMgr().getConfigFile();
    csName = _csName;
    cpuTime = configFile.getValue(csName, "CPU time");
    memory = configFile.getValue(csName, "Memory");
    reRun = configFile.getValue(csName, "Max rerun");
    extraXrsl = configFile.getValue(csName, "Extra xrsl");
    String [] rtCpCmds = configFile.getValues(csName, "Remote copy commands");
    if(rtCpCmds!=null && rtCpCmds.length>1){
      remoteCopyCommands = new HashMap<String, String>();
      for(int i=0; i<rtCpCmds.length/2; ++i){
        remoteCopyCommands.put(rtCpCmds[2*i], rtCpCmds[2*i+1]);
      }
    }
  }

  // Returns list of input files, needed for ARCGridFTPJob.submit()
  public Collection<String> createXRSLandJobScript(MyJobInfo job, String exeFileName, String xrslFileName, boolean join)
     throws IOException {

    LinkedHashSet<String> localInputFilesList = new LinkedHashSet<String>();
    String jobDefID = job.getIdentifier();
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] uses = dbPluginMgr.getRuntimeEnvironments(jobDefID);
    String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getIdentifier());

    // The executable script
    String scriptFileName = dbPluginMgr.getExecutableFile(jobDefID);
    String shortScriptName = scriptFileName;
    int lastSlash = shortScriptName.replaceAll("\\\\", "/").lastIndexOf("/");
    if(lastSlash>-1){
      shortScriptName = shortScriptName.substring(lastSlash + 1);
    }
    // names starting with file: will be uploaded, names starting with
    // / or c:\ are considered to be locally available on the server
    if(scriptFileName.startsWith("file:")){
      localInputFilesList.add(MyUtil.clearTildeLocally(MyUtil.clearFile(scriptFileName)));
    }

    try{
      
      writeXRSL(job, exeFileName, xrslFileName, join,
          scriptFileName, shortScriptName,
          dbPluginMgr, jobDefID, localInputFilesList,
          uses, outputFileNames);
      
      writeScript(job, exeFileName, xrslFileName,
          join, uses, outputFileNames, dbPluginMgr,
          jobDefID, shortScriptName);
      
    }
    catch(Exception ioe){
      logFile.addMessage("ERROR: could not write script", ioe);
      ioe.printStackTrace();
      return null;
    }
    
    Debug.debug("Will upload input files for job "+job.getIdentifier()+": "+MyUtil.arrayToString(localInputFilesList.toArray()), 2);
    return localInputFilesList;

  }

  private void writeXRSL(MyJobInfo job, String exeFileName, String xrslFileName, boolean join,
      String scriptFileName, String shortScriptName,
      DBPluginMgr dbPluginMgr, String jobDefID, LinkedHashSet<String> localInputFilesList,
      String[] uses, String[] outputFileNames)
  throws IOException {
    
    //The xrsl file
    String shortExeFileName = new File(exeFileName).getName();
    String xrslExeFileName = MyUtil.clearTildeLocally(MyUtil.clearFile(exeFileName));
    Debug.debug("shortName : " + shortExeFileName, 3);
    localInputFilesList.add(xrslExeFileName);

    StringBuffer bufXRSL = new StringBuffer();
    String line = "";
    writeLine(bufXRSL,"&");
    
    // grid-manager stuff
    writeLine(bufXRSL,"(*action=\"request\"*)");
    writeLine(bufXRSL,"(*queue=\"_submitqueue_\"*)");

    writeLine(bufXRSL,"(gmlog=\"log\")");
    writeLine(bufXRSL,"(reRun=\""+(reRun!=null?reRun:"0")+"\")");
    writeLine(bufXRSL,"(jobname=\""+shortExeFileName+"\")");
    //writeLine(bufXRSL,"(arguments=\"\")");
    // Necessary for 0.4 servers.
    writeLine(bufXRSL,"(arguments=\""+shortExeFileName+"\")");
    writeLine(bufXRSL,"(executable=\""+shortExeFileName+"\")");
    //writeLine(bufXRSL,"(stdlog=stdlog)");
    writeLine(bufXRSL,"(stdout=stdout)");
    if(join){
      writeLine(bufXRSL,"(join=\"yes\")");
    }
    else{
      writeLine(bufXRSL,"(stderr=stderr)");
    }
    if(!MyUtil.onWindows() && scriptFileName.startsWith("/") ||
        MyUtil.onWindows() && scriptFileName.matches("^\\w:.*")){
      writeLine(bufXRSL,"(executables=\""+shortExeFileName+"\" \""+shortScriptName+"\")");
    }
    //writeLine(bufXRSL,"(executables=\""+shortScriptName+"\")");
    if(cpuTime!=null && !cpuTime.equals("")){
      writeLine(bufXRSL,"(cpuTime=\""+cpuTime+"\")(*endCpu*)");
    }
    if(memory!=null && !memory.equals("")){
      writeLine(bufXRSL,"(memory>=\""+memory+"\")");
    }
    // Input files: scripts
    writeLine(bufXRSL,"(inputFiles=");
    writeLine(bufXRSL,"(\""+shortExeFileName+"\" \""+/*shortExeFileName+*/"\")");
    if(scriptFileName.startsWith("file:")){
      writeLine(bufXRSL,"(\""+shortScriptName+"\" \""+/*shortScriptName+*/"\")");
    }
    // A script file that's unqualified is assumed to be present on the
    // path on the execution node and does not need to be uploaded.
    else if(!MyUtil.onWindows() && scriptFileName.startsWith("/") ||
        MyUtil.onWindows() && scriptFileName.matches("^\\w:.*")){
      writeLine(bufXRSL,"(\""+shortScriptName+"\" \""+scriptFileName+"\")");
    }
    
    // Input files.
    String[] inputFiles1 = new String [] {};
    String [] inputs = dbPluginMgr.getJobDefInputFiles(jobDefID);
    Debug.debug("input files: "+inputs.length+" "+MyUtil.arrayToString(inputs), 3);
    if(inputs!=null && inputs.length>0){
      inputFiles1 = inputs;
    }
    // Input files from executable definition
    // (typically a tarball with code)
    String[] inputFiles2 = new String [] {};
    inputs = dbPluginMgr.getExecutableInputs(
        dbPluginMgr.getJobDefExecutableID(jobDefID));
    Debug.debug("input files: "+inputs.length+" "+MyUtil.arrayToString(inputs), 3);
    if(inputs!=null && inputs.length>0){
      inputFiles2 = inputs;
    }
    String [] inputFiles = new String[inputFiles1.length+inputFiles2.length];
    System.arraycopy(inputFiles1, 0, inputFiles, 0, inputFiles1.length);
    System.arraycopy(inputFiles2, 0, inputFiles, inputFiles1.length, inputFiles2.length);
    
    int lfc_input_file_nr = 0;
    int lastSlash;
    String inputFileName;
    String inputFileURL;
    for(int i=0; i<inputFiles.length; ++i){
      // Find unqualified name of input file and use this for destination
      lastSlash = inputFiles[i].replaceAll("\\\\", "/").lastIndexOf("/");
      if(lastSlash>-1){
        inputFileName = inputFiles[i].substring(lastSlash + 1);
      }
      else{
        inputFileName = inputFiles[i];
      }
      inputFileURL = null;
      if(inputFiles[i].startsWith("srm://") ||
         inputFiles[i].startsWith("http://") ||
         inputFiles[i].startsWith("https://") ||
         inputFiles[i].startsWith("gsiftp://") ||
         inputFiles[i].startsWith("ftp://") ||
         inputFiles[i].startsWith("rls://") ||
         inputFiles[i].startsWith("se://") ||
         inputFiles[i].startsWith("httpg://")){
         inputFileURL = inputFiles[i];
      }
      else if(inputFiles[i].startsWith("lfc://")){
        //inputFileURL = inputFiles[i].replaceFirst("/:", ";cache=no/:");
        inputFileURL = inputFiles[i];
        inputFileName = MyComputingSystem.LFC_INPUT_FILE_BASE_NAME+lfc_input_file_nr;
        ++lfc_input_file_nr;
      }
      else{
        // URL is full path of input file
        inputFileURL = MyUtil.clearTildeLocally(MyUtil.clearFile(inputFiles[i]));
      }
      Debug.debug("Input file physical name: "+inputFileURL+"<--"+inputFiles[i], 3);
     
      String protocol = inputFiles[i].replaceFirst("^(\\w+):.*$", "$1");
      if(remoteCopyCommands==null || !remoteCopyCommands.containsKey(protocol)){
        // Add local files to the return value.
        // Files starting with / are assumed to already be on the server.
        if(inputFiles[i].startsWith("file:")){
          writeLine(bufXRSL,"(\""+inputFileName+"\" \""/*+inputFileURL*/+"\")");
          localInputFilesList.add(inputFileURL);
        }
        else if(inputFiles[i].startsWith("/")){
          // do nothing
        }
        else{
          writeLine(bufXRSL,"(\""+inputFileName+"\" \""+inputFileURL+"\")");
          //remoteInputFilesList.add(inputFileURL);
        }
      }
      else{
        Debug.debug("File will be downloaded by script: "+inputFileURL+"<--"+inputFiles[i], 3);
      }
    }
    writeLine(bufXRSL,")");
    
    // outputfiles
    
    String finalStdoutDest = dbPluginMgr.getStdOutFinalDest(jobDefID);
    if((finalStdoutDest.startsWith("gsiftp://") ||
        finalStdoutDest.startsWith("ftp://") ||
        finalStdoutDest.startsWith("rls://") ||
        finalStdoutDest.startsWith("se://") ||
        finalStdoutDest.startsWith("httpg://"))){
      line = "(outputFiles=" + "(\"stdout\" \""+finalStdoutDest+"\")" ;
    }
    else{
      line = "(outputFiles=" + "(\"stdout\" \"\")" ;
    }
    String finalStderrDest = dbPluginMgr.getStdErrFinalDest(jobDefID);
    if((finalStderrDest.startsWith("gsiftp://") ||
        finalStderrDest.startsWith("ftp://") ||
        finalStderrDest.startsWith("rls://") ||
        finalStderrDest.startsWith("se://") ||
        finalStderrDest.startsWith("httpg://"))){
      if(!join){
        line += "(\"stderr\" \""+finalStderrDest+"\")";
      }
    }
    else{
      if(!join){
        line += "(\"stderr\" \"\")";
      }
    }
    String localName;
    String remoteName;
    // output file copy
    String protocol;
    for(int i=0; i<outputFileNames.length; ++i){
      localName = dbPluginMgr.getJobDefOutLocalName(job.getIdentifier(), outputFileNames[i]);
      remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getIdentifier(), outputFileNames[i]);
      protocol = remoteName.replaceFirst("^(\\w+):.*$", "$1");
      if(remoteCopyCommands!=null && remoteCopyCommands.containsKey(protocol)){
        continue;
      }
      if(remoteName.startsWith("/") || remoteName.matches("^\\w:.*")){
        // In analogy with ForkComputingSystem, this should trigger
        // copying the file to a place on the file system on the server.
        // There should not be any reason to support this
        // (if needed we could add a cp command to the job script).
        throw new IOException("ERROR: copying files locally on the worker node" +
              " is not supported. "+remoteName);
      }
      else if(remoteName.startsWith("file:")){
        // These are copied back to the client by jarclib and GridPilot
        // moves them locally on the client to their final destination
        remoteName = "";
      }
      else{
        // Files like gsiftp://... will be dealt with by the computing element
      }
      Debug.debug("remote name: "+remoteName,3);
      line += "(\""+localName+"\" \""+remoteName+"\")" ;
    }
    line += ")";
    writeLine(bufXRSL,line);

    // runtime environment
    if(uses!=null && uses.length>0 && uses[0]!=null){
      for(int i=0; i<uses.length; ++i){
        // At least for now, we only have Linux resources on NorduGrid
        if(uses[i].equals(NGComputingSystem.OS)){
          continue;
        }
        writeLine(bufXRSL, "(runTimeEnvironment>="+MyUtil.dos2unix(uses[i])+")");
        writeLine(bufXRSL, "");
      }
    }
    
    // Extra XRSL
    if(extraXrsl!=null && !extraXrsl.equals("")){
      writeLine(bufXRSL, extraXrsl);
    }
    
    // TODO: maxCPUTime maxDisk ftpThreads MinMemory

    LocalStaticShell.writeFile(xrslFileName, bufXRSL.toString(), false);

  }
  
  private void writeScript(MyJobInfo job, String exeFileName, String xrslFileName,
      boolean join, String[] uses, String[] outputFileNames, DBPluginMgr dbPluginMgr,
      String jobDefID, String shortScriptName)
  throws IOException {
    
    // create job script
    
    StringBuffer bufScript = new StringBuffer();
    String line = "";

    // Header
    writeLine(bufScript, "#!/bin/bash");
    writeLine(bufScript,"# Script generated by GridPilot/NGScriptGenerator");
    writeBlock(bufScript, csName + " wrapper script", 0, "# ");
    
    // Start time
    writeLine(bufScript, "START_TIME=`date '+%s'`");

    // Runtime environment dependencies. Text from runtimeEnvironment.init
    writeBlock(bufScript, "runtime environment dependencies", 1, "# ");
    for(int i=0; i<uses.length; ++i){
      String initTxt = dbPluginMgr.getRuntimeInitText(uses[i], csName);
      if(initTxt!=null && !initTxt.trim().equals("")){
        writeBlock(bufScript, "use "+ uses[i], 2, "# ");
        writeLine(bufScript, MyUtil.dos2unix(initTxt));
        writeLine(bufScript, "");
      }
    }

    // Input files section
    try{
      Debug.debug("Script will download "+MyUtil.arrayToString(job.getDownloadFiles()), 2);
      writeBlock(bufScript, "Input files", ScriptGenerator.TYPE_SUBSECTION, "# ");
      MyUtil.writeInputFilesSection(job, bufScript, "# ", remoteCopyCommands);
    }
    catch(IOException e){
      logFile.addMessage("Problem writing input files section in script of job "+job, e);
    }

    // Output files section
    try{
      Debug.debug("Script will upload "+MyUtil.arrayToString(job.getUploadFiles()), 2);
      writeBlock(bufScript, "Output files", ScriptGenerator.TYPE_SUBSECTION, "# ");
      MyUtil.writeOutputFilesSection(job, bufScript, "# ", remoteCopyCommands);
    }
    catch(IOException e){
      logFile.addMessage("Problem writing output files section in script of job  "+job, e);
    }

    // Parameter translation
    String [] formalParam = dbPluginMgr.getExecutableArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefExecutableParameters(jobDefID);
    writeBlock(bufScript, "parameter translation", 1, "# ");
    line ="PARAM";
    for(int i=0; i<formalParam.length; ++i){
      line += " "+formalParam[i];
    }
    writeBlock(bufScript, line, 1, "# ");
    String [] tmpParams = null;
    // Use actualParam.length instead of formalParam.length.
    // This way, if not all parameters are filled in the first will
    // be used and the rest just left empty.
    for(int i=0; i</*formalParam.length*/actualParam.length; ++i){
      try{
        // replace spaces with commas
        tmpParams = MyUtil.split(actualParam[i]);
        writeLine(bufScript, "p"+(i+1)+"="+MyUtil.arrayToString(tmpParams, ","));
      }
      catch(Exception ex){
        logFile.addMessage("Warning: problem with job parameter "+i);
        Debug.debug("WARNING: problem with job parameter "+i+": "+ex.getMessage(), 1);
        ex.printStackTrace();
      }
    }
    writeLine(bufScript, "");

    // core script call
    writeBlock(bufScript, "core script call", 1, "# ");
    
    // if the executable is not in the working directory,
    // we call it and assume it's on the path
    line = shortScriptName;
    for(int i=0; i<actualParam.length; ++i){
      line += " $p"+(i+1);
    }
    // workaround for bug in NG on Condor
    writeLine(bufScript, "if [ -e "+shortScriptName+" ]; then");
    writeLine(bufScript, "  chmod +x "+shortScriptName);
    writeLine(bufScript, "  ./"+line);
    writeLine(bufScript, "else");
    writeLine(bufScript, "  "+line);
    writeLine(bufScript, "fi");
    writeLine(bufScript, "");
    
    // Metadata section
    /* Print the running time, size and md5sum of the output file for validation
       to pick up and write in the job DB or file catalog.
       NOTICE that we assume one output file per job. There is nothing
       in principle preventing multiple output files per job, but as it is now,
       only the first of the output files will be registered. */
    writeBlock(bufScript, "Metadata", ScriptGenerator.TYPE_SUBSECTION);
    writeLine(bufScript, "END_TIME=`date '+%s'`");
    writeLine(bufScript, "echo " +
        gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG +
        ": cpuSeconds = $(( END_TIME - START_TIME ))");
    for(int i=0; i<outputFileNames.length; ++i){
      writeLine(bufScript, "echo "+gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
          ": outputFileBytes = `du -b "+outputFileNames[i]+" | awk '{print $1}'`");
      writeLine(bufScript, "echo "+gridfactory.common.jobrun.ForkScriptGenerator.METADATA_TAG+
          ": outputFileChecksum = md5:`md5sum "+outputFileNames[i]+" | awk '{print $1}'`");
      break;
    }

    LocalStaticShell.writeFile(exeFileName, bufScript.toString(), false);
    MyUtil.dos2unix(new File(exeFileName));

  }

}

