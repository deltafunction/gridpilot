package gridpilot;

import org.glite.security.voms.VOMSAttribute;
import org.glite.security.voms.VOMSValidator;
import org.glite.voms.contact.VOMSProxyInit;
import org.glite.voms.contact.VOMSRequestOptions;
import org.glite.voms.contact.VOMSServerInfo;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSCredential;

import java.net.URI;
import java.util.*;

public class VomsProxyFactory {
  public static final int CERTIFICATE_PEM = 0;
  public static final int CERTIFICATE_PKCS12 = 1;
  public static final int OID_OLD = 2;           // default
  public static final int OID_GLOBUS = 3;
  public static final int OID_RFC820 = 4;
  public static final int DELEGATION_NONE = 1;
  public static final int DELEGATION_LIMITED = 2;
  public static final int DELEGATION_FULL = 3;   // default

  private VOMSProxyInit m_proxyInit;
  private VOMSRequestOptions m_requestOptions;

  /**
   * Utility class for creating VOMS proxies.
   * 
   * @param certificateFormat format of the generated certificate, must be either
   *                          CERTIFICATE_PEM or CERTIFICATE_PKCS12
   * @param serverUrl
   * @param vo
   * @param password
   * @param vomsProxyFile
   * @param certFileStr
   * @param keyFileStr
   * @param caCertsDirStr
   * @param lifeTime
   * @param delegation delegation level. Must be one of DELEGATION_NONE, DELEGATION_NONE,
   *                   DELEGATION_FULL
   * @param proxyType type of the X.509 GSI proxy certificate. Must be one of
   *                  OID_OLD, OID_GLOBUS, OID_RFC820
   * @param fqan VOMS fully qualified attribute name - in the form
   *             &lt;group&gt;[/Role=[&lt;role&gt;][/Capability=&lt;capability&gt;]].
   *             Can be null, in which case it is ignored
   * @throws Exception
   */
  public VomsProxyFactory(
      int certificateFormat, String serverUrl, String vo,
      String password, String vomsProxyFile,
      String certFileStr, String keyFileStr, String caCertsDirStr, String vomsDirStr,
      /*Optional, i.e. can be -1 or null*/
      int lifeTime, int delegation, int proxyType, String fqan) throws Exception {
    
    /*File certFile = new File(clearTildeLocally(clearFile(
        GridPilot.getClassMgr().getSSL().getCertFile())));
    String certFileStr = certFile.getName();
    File keyFile = new File(clearTildeLocally(clearFile(
        GridPilot.getClassMgr().getSSL().getKeyFile())));
    String keyFileStr = keyFile.getName();
    String certDirStr = certFile.getParent();
    String caCertsDirStr = new File(clearTildeLocally(clearFile(
        GridPilot.getClassMgr().getSSL().getCACertsDir()))).getAbsolutePath();*/
    
    // required attributes
    System.setProperty("X509_CERT_DIR", caCertsDirStr);
    System.setProperty("CADIR", caCertsDirStr);
    System.setProperty("VOMSDIR", vomsDirStr);

    // optional attributes
    switch(certificateFormat) {
        case CERTIFICATE_PEM:
            System.setProperty("X509_USER_CERT", certFileStr);
            System.setProperty("X509_USER_KEY", keyFileStr);
            break;
        case CERTIFICATE_PKCS12:
            System.setProperty("PKCS12_USER_CERT", certFileStr);
            break;
        default:
            throw new Exception("ERROR: only PEM and PKCS12 certificates are supported");
    }

    URI uri = new URI(serverUrl);
    if(uri.getHost()==null){
        throw new Exception("Attribute Server has no host name: "+uri.toString());
    }
    VOMSServerInfo server = new VOMSServerInfo();
    server.setHostName(uri.getHost());
    server.setPort(uri.getPort());
    server.setHostDn(uri.getPath());
    server.setVoName(vo);
    m_proxyInit = !"".equals(password)
            ? VOMSProxyInit.instance(password)
            : VOMSProxyInit.instance();
    m_proxyInit.addVomsServer(server);
    m_proxyInit.setProxyOutputFile(vomsProxyFile);
    m_requestOptions = new VOMSRequestOptions();
    m_requestOptions.setVoName(vo);

    // optional attributes
    if(fqan!=null && !fqan.equals("")){
      m_requestOptions.addFQAN(fqan);
    }
    if(lifeTime>0){
        m_proxyInit.setProxyLifetime(lifeTime);
        m_requestOptions.setLifetime(lifeTime);
    }
    if(delegation>=0){
      m_proxyInit.setDelegationType(delegation);
    }
    else{
      m_proxyInit.setDelegationType(DELEGATION_FULL);
    }
    if(proxyType>=0){
      m_proxyInit.setProxyType(proxyType);
    }
    else{
      // Default to old
      m_proxyInit.setProxyType(OID_OLD);
    }
  }

  public GSSCredential createProxy() throws Exception {
    // create
    GlobusCredential globusProxy;
    if("NOVO".equals(m_requestOptions.getVoName())) {     
      // TEST to create gridProxy :         
      globusProxy = m_proxyInit.getVomsProxy(null);        
    }         
    else{
      ArrayList options = new ArrayList();
      options.add(m_requestOptions);
      globusProxy = m_proxyInit.getVomsProxy(options);
      // validate
      try{
        Vector v = VOMSValidator.parse(globusProxy.getCertificateChain());
        for(int i=0; i<v.size(); i++){
            VOMSAttribute attr = (VOMSAttribute) v.elementAt(i);
            if(!attr.getVO().equals(m_requestOptions.getVoName()))
                throw new Exception("The VO name of the created VOMS proxy ('"+attr.getVO()+"') " +
                    "does not match the required VO name ('"+m_requestOptions.getVoName()+"').");
        }
      }
      catch(IllegalArgumentException iAE){
        throw new Exception("The lifetime may be too long : "+iAE.getMessage());
      }
    }
    return new GlobusGSSCredentialImpl(globusProxy, GSSCredential.INITIATE_AND_ACCEPT);
  }

}


