package gridpilot.dbplugins.atlas;

import gridfactory.common.Debug;
import gridpilot.GridPilot;
import gridpilot.MyUtil;

import java.net.MalformedURLException;

public class LRCLookupPFN  extends LookupPFN {

  public LRCLookupPFN(ATLASDatabase db, String catalogServer,
      String lfn, String guid, boolean findAll) throws MalformedURLException {
    super(db, catalogServer, lfn, guid, findAll);
  }

  /* E.g. catalogServer--> http://dms02.usatlas.bnl.gov:8000/dq2/
     E.g. to look for trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1 :
      http://dms02.usatlas.bnl.gov:8000/dq2/lrc/PoolFileCatalog?lfns=trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1
      (0, '<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n<!-- Edited By POOL -->\n<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n<POOLFILECATALOG>\n\n  <META name="fsize" type="string"/>\n\n  <META name="md5sum" type="string"/>\n\n  <META name="lastmodified" type="string"/>\n\n  <META name="archival" type="string"/>\n\n  <File ID="af97820a-8cee-4b3b-808a-713e6bd21e8e">\n    <physical>\n      <pfn filetype="" name="srm://dcsrm.usatlas.bnl.gov/pnfs/usatlas.bnl.gov/others01/2007/06/trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432_sub0/trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1"/>\n    </physical>\n    <logical>\n      <lfn name="trig1_misal1_mc11.007211.singlepart_mu10.recon.log.v12000502_tid005432._00001.job.log.tgz.1"/>\n    </logical>\n    <metadata att_name="archival" att_value="V"/>\n    <metadata att_name="fsize" att_value="2748858"/>\n    <metadata att_name="lastmodified" att_value="1171075105"/>\n    <metadata att_name="md5sum" att_value="8a74295a00637eecdd8443cd36df49a7"/>\n  </File>\n\n</POOLFILECATALOG>\n') 
  */
  public String [] lookup() throws Exception {
    String [] ret = null;
    String path;
    if(guid!=null){
      path = "lrc/PoolFileCatalog?lfns="+guid;
    }
    else{
      path = "lrc/PoolFileCatalog?lfns="+lfn;
    }
    String url = catalogServer+(catalogServer.endsWith("/")?"":"/")+path;
    Debug.debug("Querying "+url, 2);
    String [] answer = MyUtil.readURL(url, GridPilot.getClassMgr().getTransferControl(), null, null);
    Debug.debug("Parsing PoolFileCatalog with "+MyUtil.arrayToString(answer), 3);
    PoolFileCatalog pfc = new PoolFileCatalog(answer);
    PoolFileCatalog.PoolFile pf = ((PoolFileCatalog.PoolFile) pfc.files.get(0));
    ret = new String [2+pf.pfns.length];
    ret[0] = pf.fsize;
    ret[1] = pf.md5sum;
    if(ret[1]!=null && !ret[1].equals("") && !ret[1].matches("\\w+:.*")){
      ret[1] = "md5:"+ret[1];
    }
    for(int i=0; i<pf.pfns.length; ++i){
      ret[i+2] = pf.pfns[i];
    }
    Debug.debug("--> Got "+MyUtil.arrayToString(ret), 2);
    return ret;
  }
  
}
