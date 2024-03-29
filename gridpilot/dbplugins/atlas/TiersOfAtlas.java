package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.MyLinkedHashSet;
import gridfactory.common.Util;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Vector;

public class TiersOfAtlas {

  private File toaFile;
  private String toaLocation;
  // Hash of catalog server mappings found in TiersOfAtlas
  private HashMap<String, String> fileCatalogs = new HashMap<String, String>();
  private HashMap<String, String> httpFileCatalogs = new HashMap<String, String>();
  private MyLinkedHashSet<String> blacklistedParents = new MyLinkedHashSet<String>();

  /**
   * Create TiersOfAtlas object.
   * @param _toaLocation URL of the tiers of ATLAS file
   * @param _localCacheFile local file to use for caching - can be null,
   *                        in which case caching is not persistent across sessions
   */
  public TiersOfAtlas(String _toaLocation, String _localCacheFile) {
    try{
      toaLocation = _toaLocation;
      URL toaURL = null;
      File tmpFile = File.createTempFile(/*prefix*/"GridPilot-TOA", /*suffix*/"");
      tmpFile.delete();
      toaLocation = MyUtil.clearFile(toaLocation);
      if(toaLocation.startsWith("~")){
        toaLocation = System.getProperty("user.home") + File.separator +
        toaLocation.substring(1);
      }
      if(toaLocation.matches("\\w:.*") || toaLocation.indexOf(":")<0){
        toaURL = (new File(toaLocation)).toURI().toURL();
      }
      else{
        toaURL = new URL(toaLocation);
      }
      // Check if the URL is available and write to tmp file - if not, don't overwrite cache.
      try{
        BufferedReader in = new BufferedReader(new InputStreamReader(toaURL.openStream()));
        PrintWriter out = new PrintWriter(new FileWriter(tmpFile)); 
        String line = null;
        while((line = in.readLine())!=null){
          out.println(line);
        }
        in.close();
        out.close();
      }
      catch(Exception ee){
        ee.printStackTrace();
      }
      if(_localCacheFile==null || _localCacheFile.trim().equals("")){
        if(!tmpFile.exists() || tmpFile.length()<1000){
          throw new IOException("ERROR: "+toaURL+" not available and not using cache.");
        }
        toaFile = tmpFile;
        // have the tmp file deleted on exit
        GridPilot.addTmpFile(tmpFile.getName(), toaFile);
      }
      else{
        toaFile = new File(Util.clearTildeLocally(Util.clearFile(_localCacheFile)));
        if(!tmpFile.exists() || tmpFile.length()<1000){
          GridPilot.getClassMgr().getLogFile().addMessage("WARNING: "+toaURL+" not available. Using old cache file.");
        }
        else{
          toaFile.delete();
          LocalStaticShell.copyFile(tmpFile.getAbsolutePath(), toaFile.getAbsolutePath());
          tmpFile.delete();
        }
      }
      Debug.debug("Wrote cache of TiersOfATLAS in "+toaFile.getAbsolutePath(), 2);
    }
    catch(Exception e){
      String error = "WARNING: could not load tiers of ATLAS file. File catalog lookups " +
          "will not work.";    
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      MyUtil.showError(error+" "+e.getMessage());
    }
  }

  protected void clear(){
    fileCatalogs.clear();
    httpFileCatalogs.clear();
    blacklistedParents.clear();
  }
  
  /**
   * @param siteAcronym the site acronym
   * @param preferHttp only relevant for LRC catalogs: whether or not to use the http
   *        interface instead of direct mysql
   * @return the catalog URL
   */
  protected String getFileCatalogServer(String _siteAcronym, boolean preferHttp) throws MalformedURLException, IOException {
    
    if(!preferHttp && fileCatalogs.containsKey(_siteAcronym)){
      return fileCatalogs.get(_siteAcronym);
    }
    else if(preferHttp && httpFileCatalogs.containsKey(_siteAcronym)){
      return httpFileCatalogs.get(_siteAcronym);
    }
    else if(fileCatalogs.containsKey(_siteAcronym)){
      return fileCatalogs.get(_siteAcronym);
    }
    else if(httpFileCatalogs.containsKey(_siteAcronym)){
      return httpFileCatalogs.get(_siteAcronym);
    }
    
    Debug.debug(_siteAcronym+" NOT in cache "+MyUtil.arrayToString(fileCatalogs.keySet().toArray())+
        " : "+preferHttp, 2);
    
    StringBuffer siteAcronym = new StringBuffer(_siteAcronym);
    
    StringBuffer catalogServer= new StringBuffer();
    StringBuffer httpCatalogServer = new StringBuffer();
    StringBuffer catalogSite = new StringBuffer();
    StringBuffer catalogName = new StringBuffer();
    StringBuffer parentSite = new StringBuffer();
    int count = 0;
    Debug.debug("Trying to match "+siteAcronym, 3);
    // This blacklisting stuff is to make up for some topology weirdness - well, inconsistency
    // as far as I can see.
    for(int i=0; i<5; ++i){
      while(catalogServer.toString().trim().equals("") && httpCatalogServer.toString().trim().equals("") && count<5){
        ++count;
        doGetFileCatalogServer(siteAcronym, preferHttp, catalogServer, httpCatalogServer, catalogSite, catalogName, parentSite);
      }
      if(catalogServer.toString().trim().equals("") && httpCatalogServer.toString().trim().equals("") &&
          !parentSite.toString().trim().equals("")){
        Debug.debug("Catalog server not found, blacklisting parent "+parentSite, 2);
        blacklistedParents.add(parentSite.toString().trim());
        count = 0;
        catalogSite.setLength(0);
        catalogName.setLength(0);
        parentSite.setLength(0);
      }
      else{
        break;
      }
    }
    String cServer = catalogServer.toString().trim();
    String hcServer = httpCatalogServer.toString().trim();
    if(!cServer.equals("")){
      fileCatalogs.put(_siteAcronym, catalogServer.toString());
    }
    if(!hcServer.equals("")){
      httpFileCatalogs.put(_siteAcronym, httpCatalogServer.toString());
    }
    if(cServer.equals("") && hcServer.equals("")){
      fileCatalogs.put(_siteAcronym, null);
    }
    return cServer;
  }

  private void doGetFileCatalogServer(StringBuffer siteAcronym, boolean preferHttp,
      StringBuffer catalogServer, StringBuffer httpCatalogServer, StringBuffer catalogSite,
      StringBuffer catalogName, StringBuffer parentSite) throws IOException {
    String line = null;
    String inLine = null;
    BufferedReader in = new BufferedReader(new InputStreamReader((toaFile.toURI().toURL()).openStream()));
    StringBuffer lb = new StringBuffer();
    boolean topology = false;
    // Parse TOA file
    while((inLine = in.readLine())!=null){
      inLine = inLine.trim();
      // take care of "lines" split on multiple lines
      if(inLine.length()==0){
        continue;
      }
      else if(inLine.endsWith("',") || inLine.endsWith("': [")){
        lb.append(inLine);
        continue;
      }
      else if(inLine.matches("^\\s*\\#.*")){
        continue;
      }
      else if(lb.length()>0){
        lb.append(inLine);
        line = lb.toString();
        lb.setLength(0);
      }
      else{
        line = inLine;
      }
      if(line.matches(".*topology\\s*=\\s*\\{.*")){
        topology = true;
      }
      if(line.contains("}")){
        topology = false;
      }
      if(topology){
        parseTopology(siteAcronym, preferHttp,
            catalogServer, httpCatalogServer, catalogSite,
            catalogName, parentSite, line);
      }
      // Now look for
      // FZKLFC: [ 'FZKSITES' ]
      if(!catalogSite.toString().trim().equals("") &&
          line.matches("^\\s*\\S+LFC\\s*:.*'"+catalogSite+"'.*")){
        catalogName.setLength(0);
        catalogName.append(line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+catalogSite+"'.*", "$1"));
        Debug.debug("Catalog name --> "+catalogName, 2);
      }
      else if(!catalogSite.toString().trim().equals("") &&
          line.matches("^\\s*\\S+LRC\\s*:.*'"+catalogSite+"'.*")){
        catalogName.setLength(0);
        catalogName.append(line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+catalogSite+"'.*", "$1"));
        Debug.debug("Catalog name --> "+catalogName, 2);
      }
      else if(!catalogSite.toString().trim().equals("") &&
          line.matches("^\\s*\\S+LFC\\s*:.*'"+siteAcronym+"'.*")){
        catalogName.setLength(0);
        catalogName.append(line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+siteAcronym+"'.*", "$1"));
        Debug.debug("Catalog name --> "+catalogName, 2);
      }
      else if(!catalogSite.toString().trim().equals("") &&
          line.matches("^\\s*\\S+LRC\\s*:.*'"+siteAcronym+"'.*")){
        catalogName.setLength(0);
        catalogName.append(line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+siteAcronym+"'.*", "$1"));
        Debug.debug("Catalog name --> "+catalogName, 2);
      }
      else if(!catalogSite.toString().trim().equals("") &&
          line.matches("^\\s*\\S+LFC\\s*:.*'"+parentSite+"'.*")){
        catalogName.append(line.replaceFirst("^\\s*(\\S+LFC)\\s*:.*'"+parentSite+"'.*", "$1"));
        Debug.debug("Catalog name --> "+catalogName, 2);
      }
      else if(!catalogSite.toString().trim().equals("") &&
          line.matches("^\\s*\\S+LRC\\s*:.*'"+parentSite+"'.*")){
        catalogName.setLength(0);
        catalogName.append(line.replaceFirst("^\\s*(\\S+LRC)\\s*:.*'"+parentSite+"'.*", "$1"));
        Debug.debug("Catalog name --> "+catalogName, 2);
      }
      // Now look for
      // FZKLFC = 'lfc://lfc-fzk.gridka.de:/grid/atlas/'
      else if(!preferHttp && !catalogName.toString().trim().equals("") &&
          line.matches("^\\s*"+catalogName+"\\s*=\\s*'(.+)'.*")){
        catalogServer.setLength(0);
        catalogServer.append(line.replaceFirst("^\\s*"+catalogName+
            "\\s*=\\s*'(.+)'.*", "$1"));
        Debug.debug("Catalog server --> "+siteAcronym+" --> "+catalogServer, 2);
        //fileCatalogs.put(siteAcronym, catalogServer);
      }
      else if(preferHttp && !catalogName.toString().trim().equals("") &&
          line.matches("^\\s*"+catalogName+"HTTP\\s*=\\s*'(.+)'.*")){
        httpCatalogServer.setLength(0);
        httpCatalogServer.append(line.replaceFirst("^\\s*"+catalogName+
            "HTTP\\s*=\\s*'(.+)'.*", "$1"));
        Debug.debug("Catalog server --> "+siteAcronym+" --> "+httpCatalogServer, 2);
        //httpFileCatalogs.put(siteAcronym, httpCatalogServer);
      }
    }
    in.close();
  }
  
  private void parseTopology(StringBuffer siteAcronym, boolean preferHttp,
      StringBuffer catalogServer, StringBuffer httpCatalogServer, StringBuffer catalogSite,
      StringBuffer catalogName, StringBuffer parentSite, String line) {
    String tmp;
    // Say, we have a name 'CSCS'; first look for lines like
    // 'FZKSITES': [ 'FZK', 'FZU', 'CSCS', 'CYF', 'DESY-HH', 'DESY-ZN', 'UNI-FREIBURG', 'WUP' ],
    // 'FZK': [ 'FZKDISK', 'FZKTAPE' ],
    if(!parentSite.toString().trim().equals("") &&
       line.matches("^\\W*'(\\w*)':\\s*\\[.*\\W+'"+parentSite+"'\\W+.*") ||
       
       parentSite.toString().trim().equals("") &&
       line.matches("^\\W*'(\\w*)':\\s*\\[.*\\W+'"+siteAcronym+"'\\W+.*")){
      
      catalogSite.setLength(0);
      catalogSite.append(line.replaceFirst("^\\W*'(\\w*)':.*", "$1"));
      Debug.debug("catalogSite --> "+catalogSite, 2);
      
    }
    if(/*line.indexOf("SITES")<0 && line.indexOf("'TIER1S': [")<0 && */
        line.indexOf("'ALL': [")<0  && line.indexOf("'alternateName' : [")<0 && 
        line.indexOf("LRC")<0 && line.indexOf("LFC")<0 &&
        line.indexOf("'alternateName' : [")<0 && (
            //!parentSite.toString().trim().equals("") && line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+parentSite+"\\W+.*") ||
            !siteAcronym.toString().trim().equals("") && line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+siteAcronym+"\\W+.*") ||
            !catalogSite.toString().trim().equals("") && line.matches("^\\W*(\\w*)\\W*\\s*:\\s*\\[.*\\W+"+catalogSite+"\\W+.*")
          )
        ){
      tmp = line.replaceFirst("^\\W*(\\w*)\\W*\\s*:.*", "$1");
      if(!tmp.equals(catalogSite.toString()) && !blacklistedParents.contains(tmp)){
        parentSite.setLength(0);
        parentSite.append(tmp);
        Debug.debug("parentSite of "+catalogSite+" --> "+parentSite, 2);
      }
    }
  }

  private Vector<String> findAllSiteAcronyms() throws IOException {
    Vector<String> ret = new Vector<String>();
    BufferedReader in = null;
    String line = null;
    String inLine = null;
    in = new BufferedReader(new InputStreamReader((toaFile.toURI().toURL()).openStream()));
    StringBuffer lb = new StringBuffer();
    String sitePattern = "^\\s*'([\\w-]+)'\\s*:\\s*$";
    String site;
    // Parse TOA file
    while((inLine = in.readLine())!=null){
      inLine = inLine.trim();
      // take care of "lines" split on multiple lines
      if(inLine.length()==0){
        continue;
      }
      else if(inLine.endsWith("',") || inLine.endsWith("': [")){
        lb.append(inLine);
        continue;
      }
      else if(inLine.matches("^\\s*\\#.*")){
        continue;
      }
      else if(lb.length()>0){
        lb.append(inLine);
        line = lb.toString();
        lb.setLength(0);
      }
      else{
        line = inLine;
      }
      if(line.matches(sitePattern)){
        site = line.replaceFirst(sitePattern, "$1");
        if(!ret.contains(site)){
          Debug.debug("Adding site "+site, 2);
          ret.add(site);
        }
      }
    }
    Collections.sort(ret);
    return ret;
  }

  /**
   * Find all *LOCALGROUPDISK sites in TOA.
   * @return all *LOCALGROUPDISK site names
   * @throws MalformedURLException
   * @throws IOException
   */
  public String [] getAllLocalGroupDisks() throws MalformedURLException, IOException{
    String site;
    LinkedHashSet<String> ret = new LinkedHashSet<String>();
    for(Iterator<String> it=findAllSiteAcronyms().iterator(); it.hasNext();){
      site = it.next();
      if(site.matches("(?i).*localgroupdisk")){
        ret.add(site);
      }
    }
    return ret.toArray(new String[ret.size()]);
  }
  
}
