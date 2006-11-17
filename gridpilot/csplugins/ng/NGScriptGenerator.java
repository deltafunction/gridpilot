package gridpilot.csplugins.ng;

import java.io.*;
import java.util.List;
import java.util.Vector;

import gridpilot.DBPluginMgr;
import gridpilot.Debug;
import gridpilot.JobInfo;
import gridpilot.LocalStaticShellMgr;
import gridpilot.GridPilot;
import gridpilot.ScriptGenerator;
import gridpilot.Util;

/**
 * Script generator for the NorduGrid plugin. <br>
 * <p><a href="NGScriptGenerator.java.html">see sources</a>
 */

public class NGScriptGenerator extends ScriptGenerator{

  String cpuTime = null;
  List inputFilesList = null;
  
  public NGScriptGenerator(String _csName){
    super(_csName);
    csName = _csName;
    cpuTime = configFile.getValue(csName, "CPU time");
  }

  // Returns List of input files, needed for ARCGridFTPJob.submit()
  public List createXRSL(JobInfo job, String exeFileName, String xrslFileName, boolean join){

    inputFilesList = new Vector();
    StringBuffer bufXRSL = new StringBuffer();
    StringBuffer bufScript = new StringBuffer();
    String line = "";
    String jobDefID = job.getJobDefId();
    DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(job.getDBName());
    String [] formalParam = dbPluginMgr.getTransformationArguments(jobDefID);
    String [] actualParam = dbPluginMgr.getJobDefTransPars(jobDefID);

    // The transformation script
    String scriptFileName = dbPluginMgr.getTransformationScript(jobDefID);
    String shortScriptName = scriptFileName;
    int lastSlash = shortScriptName.replaceAll("\\\\", "/").lastIndexOf("/");
    if(lastSlash>-1){
      shortScriptName = shortScriptName.substring(lastSlash + 1);
    }
    String xrslScriptName = null;
    // names starting with file: will be uploaded, names starting with
    // / or c:\ are considered to be locally available on the server
    if(scriptFileName.startsWith("file:")){
      xrslScriptName = Util.clearTildeLocally(Util.clearFile(scriptFileName));
      inputFilesList.add(xrslScriptName);
    }
    else{
      xrslScriptName = scriptFileName;
    }

    //The xrsl file
    String shortExeFileName = new File(exeFileName).getName();
    String xrslExeFileName = Util.clearTildeLocally(Util.clearFile(exeFileName));
    Debug.debug("shortName : " + shortExeFileName, 3);
    inputFilesList.add(xrslExeFileName);

    String inputFileName = null;
    String inputFileURL = null;

    try{
      writeLine(bufXRSL,"&");
      
      // grid-manager stuff
      writeLine(bufXRSL,"(*action=\"request\"*)");
      writeLine(bufXRSL,"(*queue=\"_submitqueue_\"*)");

      writeLine(bufXRSL,"(gmlog=\"log\")");
      writeLine(bufXRSL,"(reRun=\"1\")");
      writeLine(bufXRSL,"(jobname=\""+shortExeFileName+"\")");
      //writeLine(bufXRSL,"(arguments=\"\")");
      // This hack seems to be necessary, as the executable is there,
      // but logged as not found.
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
      writeLine(bufXRSL,"(executables=\""+shortExeFileName+"\" \""+shortScriptName+"\")");
      if(cpuTime!=null && !cpuTime.equals("")){
        writeLine(bufXRSL,"(cpuTime=\""+cpuTime+"\")(*endCpu*)");
      }
      // Input files: scripts
      writeLine(bufXRSL,"(inputFiles=");
      writeLine(bufXRSL,"(\""+shortExeFileName+"\" \"\")");
      if(!scriptFileName.startsWith("file:")){
        writeLine(bufXRSL,"(\""+shortScriptName+"\" \""+xrslScriptName+"\")");
      }
      
      // Input files.
      String[] inputFiles1 = new String [] {};
      String [] inputs = dbPluginMgr.getJobDefInputFiles(jobDefID);
      Debug.debug("input files: "+inputs.length+" "+Util.arrayToString(inputs), 3);
      if(inputs!=null && inputs.length>0){
        inputFiles1 = inputs;
      }
      // Input files from transformation definition
      // (typically a tarball with code)
      String[] inputFiles2 = new String [] {};
      inputs = dbPluginMgr.getTransformationInputs(
          dbPluginMgr.getJobDefTransformationID(jobDefID));
      Debug.debug("input files: "+inputs.length+" "+Util.arrayToString(inputs), 3);
      if(inputs!=null && inputs.length>0){
        inputFiles2 = inputs;
      }
      String [] inputFiles = new String[inputFiles1.length+inputFiles2.length];
      System.arraycopy(inputFiles1, 0, inputFiles, 0, inputFiles1.length);
      System.arraycopy(inputFiles2, 0, inputFiles, inputFiles1.length, inputFiles2.length);
            
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
        if(inputFiles[i].startsWith("http://") ||
            inputFiles[i].startsWith("https://") ||
            inputFiles[i].startsWith("gsiftp://") ||
            inputFiles[i].startsWith("ftp://")){
          inputFileURL = inputFiles[i];
        }
        else{
          // URL is full path of input file
          inputFileURL = Util.clearTildeLocally(Util.clearFile(inputFiles[i]));
        }
        Debug.debug("Input file physical name: "+inputFileURL, 3);
       
        // Add local files to the return value.
        // Files starting with / are assumed to already be on the server.
        if(inputFiles[i].startsWith("file:")){
          inputFilesList.add(inputFileURL);
        }
        else if(inputFiles[i].startsWith("/")){
          // do nothing
        }
        else{
          writeLine(bufXRSL,"(\""+inputFileName+"\" \""+inputFileURL+"\")");       
        }
      }
      writeLine(bufXRSL,")");

      // outputfiles
      line = "(outputFiles=" + "(\"stdout\" \"stdout\")" ;
      if(!join){
        line += "(\"stderr\" \"stderr\")";
      }

      String[] outputFileNames = dbPluginMgr.getOutputFiles(job.getJobDefId());
      String localName;
      String remoteName;
      // output file copy
      for(int i=0; i<outputFileNames.length; ++i){
        localName = dbPluginMgr.getJobDefOutLocalName(job.getJobDefId(), outputFileNames[i]);
        remoteName = dbPluginMgr.getJobDefOutRemoteName(job.getJobDefId(), outputFileNames[i]);
        if(remoteName.startsWith("/") || remoteName.matches("^\\w:.*") ||
            remoteName.startsWith("file:")){
          remoteName = localName;
        }
        Debug.debug("remote name: "+remoteName,3);
        line += "(\""+localName+"\" \""+remoteName+"\")" ;
      }
      line += ")";
      writeLine(bufXRSL,line);
      
      // runtime environment
      String [] uses = dbPluginMgr.getRuntimeEnvironments(jobDefID);
      if(uses!=null && uses.length>0 && uses[0]!=null){
        for(int i=0; i<uses.length; ++i){
          // At least for now, we only have Linux resources on NorduGrid
          if(uses[i].equals("Linux")){
            continue;
          }
          writeLine(bufXRSL, "(runTimeEnvironment="+Util.dos2unix(uses[i])+")");
          writeLine(bufXRSL, "");
        }
      }
      
      // TODO: maxCPUTime maxDisk ftpThreads MinMemory

      try{
        LocalStaticShellMgr.writeFile(xrslFileName, bufXRSL.toString(), false);
      }
      catch(Exception fnfe) {
        System.err.print(fnfe.getMessage());
        Debug.debug("Could not write file. "+fnfe.getMessage(), 1);
        return null;
      }

      //create job script

      // Header
      writeLine(bufScript, "#!" + configFile.getValue(csName, "Shell"));
      writeLine(bufScript,"# Script generated by GridPilot/NGScriptGenerator");
      writeBloc(bufScript, csName + " wrapper script", 0, "# ");

      //Package dependenties. Text from package.init
      writeBloc(bufScript, "packages dependencies", 1, "# ");
      for(int i=0; i<uses.length; ++i){
        writeBloc(bufScript, "use "+ uses[i], 2, "# ");
        String initTxt = dbPluginMgr.getRuntimeInitText(uses[i], csName).toString();
        writeLine(bufScript, Util.dos2unix(initTxt));
        writeLine(bufScript, "");
      }

      // parameter translation
      writeBloc(bufScript, "parameter translation", 1, "# ");
      line ="PARAM";
      for(int i=0; i<formalParam.length; ++i){
        line += " "+formalParam[i];
      }
      writeBloc(bufScript, line, 1, "# ");
      String [] tmpParams = null;
      for(int i=0; i< formalParam.length; ++i){
      	try{
          // replace spaces with commas
      	  tmpParams = Util.split(actualParam[i]);
          writeLine(bufScript, "p"+(i+1)+"="+Util.arrayToString(tmpParams, ","));
      	}
      	catch(Exception ex){
      		GridPilot.getClassMgr().getStatusBar().setLabel("Warning: problem with job parameter "+i);
      	  Debug.debug("WARNING: problem with job parameter "+i+": "+ex.getMessage(), 1);
      	  ex.printStackTrace();
      	}
      }
      writeLine(bufScript, "");

      // core script call
      writeBloc(bufScript, "core script call", 1, "# ");
      
      // workaround for bug in NG on Condor
      writeLine(bufScript, "chmod +x "+shortScriptName);
      line = "./"+shortScriptName ;
      for(int i=0; i<formalParam.length; ++i)
        line += " $p"+(i+1);
      writeLine(bufScript, line);
      writeLine(bufScript, "");

      try{
        LocalStaticShellMgr.writeFile(exeFileName,
            bufScript.toString(), false);
        Util.dos2unix(new File(exeFileName));
        /*Util.dos2unixConvert(new File(exeFileName),
            new File(exeFileName+".unix"));
        LocalStaticShellMgr.deleteFile(exeFileName);
        LocalStaticShellMgr.moveFile(exeFileName+".unix", exeFileName);*/
      }
      catch(FileNotFoundException fnfe){
        System.err.print(fnfe.getMessage());
        Debug.debug("Could not write file. "+fnfe.getMessage(), 1);
        return null;
      }
      catch(Exception ioe){
        Debug.debug(ioe.getMessage(), 1);
        ioe.printStackTrace();
        return null;
      }
      return inputFilesList;
    }
    catch(Exception ioe){
      logFile.addMessage("ERROR: could not write script", ioe);
      ioe.printStackTrace();
      return null;
    }

  }
}

