package gridpilot.dbplugins.atlas;

import gridpilot.Debug;

import java.util.Vector;

public class PoolFileCatalog {
  
  private String [] lines = null;
  
  /**
   * Vector of PoolFile objects.
   */
  public Vector files = null;

  /**
   * Parses the Pool file catalog represented by an array of lines.
   * After calling this constructor, the pool file catalogs are available
   * as a vector "files" of PoolFile objects.
   */
  public PoolFileCatalog(String [] _lines) {
   lines = _lines;
   files = new Vector();
   parse();
  }

  private void parse() {
    // parse the pool file catalog
    PoolFile pf = null;
    StringBuffer fileSB = new StringBuffer();
    String str = "";
    int fileEnd = 0;
    for(int i=0; i<lines.length; ++i){
      Debug.debug("Line: "+i+":"+lines[i], 3);
      if(lines[i].indexOf("<File ID=")>-1){
        pf = new PoolFile();
      }
      fileSB.append(lines[i]);
      fileEnd = fileSB.indexOf("</File>");
      Debug.debug("Length of entry "+fileEnd, 3);
      if(pf==null || fileEnd<1){
        continue;
      }
      else{
        str = fileSB.toString();
        // get all the PoolFile attributes
        // id
        pf.id = str.replaceFirst("(?i)(?s).*<File\\s+ID=\\s*\"([^\"]*)\".*", "$1");
        if(pf.id.equals(str)){
          pf.id = "";
        }
        // pfns
        Vector pfnsVec = new Vector();
        String thisPfn = null;
        String drop = null;
        while(true){
          thisPfn = str.replaceFirst("(?i)(?s).*<pfn\\s+[^<]*name\\s*=\\s*\"([^\"]*)\".*", "$1");
          if(!thisPfn.equals(str)){
            Debug.debug("--> PFN "+thisPfn, 3);
            pfnsVec.add(thisPfn);
            drop = str.replaceFirst("(?i)(?s).*(<pfn\\s+[^<]*name\\s*=\\s*\"[^\"]*\").*", "$1");
            str = str.replace(drop, "");
            fileEnd = fileEnd-drop.length();
          }
          else{
            break;
          }
        }
        pf.pfns = new String [pfnsVec.size()];
        for(int j=0; j<pfnsVec.size(); ++j){
          pf.pfns[j] = (String) pfnsVec.get(j);
        }
        // lfn
        pf.lfn = str.replaceFirst("(?i)(?s).*<lfn\\s+name\\s*=\\s*\"([^\"]*)\".*", "$1");
        if(pf.lfn.equals(str)){
          pf.lfn = "";
        }
        // fsize
        pf.fsize = str.replaceFirst("(?i)(?s).*<metadata\\s+att_name=\"fsize\"\\s+att_value\\s*=\\s*\"([^\"]*)\".*", "$1");
        if(pf.fsize.equals(str)){
          pf.fsize = "";
        }
        // lastModified
        pf.lastModified = str.replaceFirst("(?i)(?s).*<metadata\\s+att_name=\"lastmodified\"\\s+att_value\\s*=\\s*\"([^\"]*)\".*", "$1");
        if(pf.lastModified.equals(str)){
          pf.lastModified = "";
        }
        // md5sum
        pf.md5sum = str.replaceFirst("(?i)(?s).*<metadata\\s+att_name=\"md5sum\"\\s+att_value\\s*=\\s*\"([^\"]*)\".*", "$1");
        if(pf.md5sum.equals(str)){
          pf.md5sum = "";
        }
        // add the PoolFile
        files.add(pf);
        fileSB = new StringBuffer(str.substring(fileEnd+7)); 
        str = fileSB.toString();
        fileEnd = str.indexOf("</File>");
        Debug.debug("New length of entry "+fileEnd, 3);
      }
      if(fileEnd<6){
        break;
      }
    }

  }
  
  public class PoolFile {
    public String id = null;
    public String [] pfns = null;
    public String lfn = null;
    public String lastModified = null;
    public String md5sum = null;
    public String fsize = null;
  }

}
