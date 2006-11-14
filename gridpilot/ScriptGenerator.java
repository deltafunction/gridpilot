package gridpilot;

import java.io.*;


/**
 * Abstract class, has to be extended for each computing system. <p>
 *
 * Provides 4 (defined) protected methods : <ul>
 * <li> protected String getFullPath(String file) : <br>
 *    converts a file name into a full path name
 * <li> protected void writeLine(RandomAccessFile out, String s) : <br>
 *    writes a line in file out
 * <li> protected void writeBloc(RandomAccessFile out, String [] s, int type) : <br>
 *    writes a comment "bloc" containing multi lines s. type is in {0,1,2}, from the biggest
 *    bloc to the smallest
 * <li> protected void writeBloc(RandomAccessFile out, String s, int type) : <br>
 *    idem, but for a simple line
 *
 * <p><a href="ScriptGenerator.java.html">see sources</a>
 */
abstract public class ScriptGenerator{
  protected ConfigFile configFile;
  protected String csName;
  protected LogFile logFile;
  
  public final static int TYPE_SECTION = 0;
  public final static int TYPE_SUBSECTION = 1;
  public final static int TYPE_COMMENT = 2;

  public ScriptGenerator(String _csName) {
    csName = _csName;
    configFile = GridPilot.getClassMgr().getConfigFile();
    logFile = GridPilot.getClassMgr().getLogFile();
  }

  private String getLine(String s){return s + "\n";}

  private String getBloc(String [] s, int type, String _commentStart){
    String bloc = null;
    String commentStart = "";
    if(_commentStart==null){
      if(System.getProperty("os.name").toLowerCase().startsWith("windows")){
        commentStart = "REM ";
      }
      else{
        commentStart = "#";
      }
    }
    else{
      commentStart = _commentStart;
    }
    switch(type){
      case TYPE_SECTION:
        bloc = commentStart+"##########################################################################\n";
        for(int i = 0; i<s.length ; ++i)
          bloc += commentStart+"          " + s[i]+"\n";
        bloc += commentStart+"##########################################################################\n";
        break;

      case TYPE_SUBSECTION:
        bloc = commentStart+"--------------------------------------------------------------------------\n";
        for(int i = 0; i<s.length ; ++i)
          bloc += commentStart+"          " + s[i]+"\n";
        bloc += commentStart+"--------------------------------------------------------------------------\n";
        break;

      case TYPE_COMMENT:
        bloc = commentStart+"-------------------\n";
        for(int i = 0; i<s.length ; ++i)
          bloc += commentStart+"      " + s[i]+"\n";
        bloc += commentStart+"-------------------\n";
        break;
    }
    return bloc;
  }

  protected void writeLine(RandomAccessFile out, String s) throws IOException{
    out.writeBytes(getLine(s));
  }

  protected void writeBloc(RandomAccessFile out, String [] s, int type, String commentStart) throws IOException{
    writeLine(out, getBloc(s, type, commentStart));
  }

  protected void writeBloc(RandomAccessFile out, String s, int type,
      String commentStart) throws IOException{
    String [] s2 = {s};
    writeBloc(out, s2, type, commentStart);
  }

  public void writeLine(StringBuffer buf, String s){
    buf.append(getLine(s));
  }

  protected void writeBloc(StringBuffer buf, String [] s, int type, String commentStart){
    buf.append(getBloc(s, type, commentStart));
  }

  protected void writeBloc(StringBuffer buf, String s, int type, String commentStart){
    writeBloc(buf, new String[]{s}, type, commentStart);
  }
  
  protected void writeBloc(StringBuffer buf, String [] s, int type){
    buf.append(getBloc(s, type, null));
  }

  protected void writeBloc(StringBuffer buf, String s, int type){
    writeBloc(buf, new String[]{s}, type, null);
  }

}