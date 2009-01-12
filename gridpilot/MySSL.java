package gridpilot;

import static gridfactory.common.Util.arrayToString;
import static gridfactory.common.Util.clearFile;
import static gridfactory.common.Util.clearTildeLocally;
import static gridfactory.common.Util.split;
import gridfactory.common.Debug;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.SSL;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.cert.X509Certificate;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.JTextPane;

import jonelo.jacksum.JacksumAPI;
import jonelo.jacksum.algorithm.AbstractChecksum;

import org.globus.common.CoGProperties;
import org.globus.gsi.CertUtil;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.OpenSSLKey;
import org.globus.gsi.X509ExtensionSet;
import org.globus.gsi.bc.BouncyCastleCertProcessingFactory;
import org.globus.gsi.bc.BouncyCastleOpenSSLKey;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

/**
 * This class allows access to all global objects in gridpilot.
 */

public class MySSL extends SSL{

  private static String caCertsTmpdir = null;
  private X509Certificate x509UserCert = null;
  private GSSCredential credential = null;
  private Boolean gridProxyInitialized = Boolean.FALSE;
  private Boolean sslInitialized = Boolean.FALSE;
  private boolean sslOk = false;
  private boolean proxyOk = false;
  private CoGProperties prop = null;
  // the *Str fields are to be fully qualified path names to be fed to the VOMS stuff
  private String certFileStr;
  private String keyFileStr;
  private String vomsDirStr;
  private String caCertsDirStr;
  
  private final static String PROXY_TYPE_OLD = "OLD";
  private final static String PROXY_TYPE_GLOBUS = "GLOBUS";
  private final static String PROXY_TYPE_RFC = "RFC";
  
  public MySSL() throws IOException, GeneralSecurityException {
    
    certFile = GridPilot.certFile;
    keyFile = GridPilot.keyFile;
    keyPassword = GridPilot.keyPassword;
    
    caCertsDir = GridPilot.caCertsDir==null?null:MyUtil.clearTildeLocally(MyUtil.clearFile(GridPilot.caCertsDir));
    vomsDirStr = GridPilot.vomsDir==null?null:MyUtil.clearTildeLocally(MyUtil.clearFile(GridPilot.vomsDir));
    
    certFileStr = (new File(clearTildeLocally(MyUtil.clearFile(certFile)))).getAbsolutePath();
    keyFileStr = (new File(clearTildeLocally(MyUtil.clearFile(keyFile)))).getAbsolutePath();
    
    if(CoGProperties.getDefault()==null){
      prop = new CoGProperties();
    }
    else{
      prop = CoGProperties.getDefault();
    }
    //getGridCredential();
    //activateProxySSL();
  }
  
  public void activateSSL() throws IOException, GeneralSecurityException, GlobusCredentialException, GSSException {
    if(sslOk){
      return;
    }
    initalizeCACertsDir();
    if(GridPilot.keyPassword==null){
      sslInitialized = false;
      credential = null;
      //getProxyFile().delete();
      //getGridCredential();
      synchronized(sslInitialized){
        decryptPrivateKey();
        sslInitialized = true;
      }
    }
    Debug.debug("Activating SSL with password "+GridPilot.keyPassword, 2);
    super.activateSSL(GridPilot.certFile, GridPilot.keyFile, GridPilot.keyPassword, GridPilot.caCertsDir);
    credential = super.getCredential();
    sslOk = true;
  }

  public void activateProxySSL() throws IOException, GeneralSecurityException {
    if(proxyOk){
      Debug.debug("Proxy already ok", 3);
      return;
    }
    Debug.debug("Activating proxy SSL with password "+GridPilot.keyPassword, 2);
    initalizeCACertsDir();
    initalizeVomsDir();
    getGridCredential();
    //super.activateSSL(getProxyFile().getAbsolutePath(), getProxyFile().getAbsolutePath(), "", caCertsDir);
    proxyOk = true;
  }

  public String getCaCertsTmpDir(){
    return caCertsTmpdir;
  }
  
  public X509Certificate getX509UserCert() throws IOException, GeneralSecurityException{
    x509UserCert = CertUtil.loadCertificate(MyUtil.clearTildeLocally(GridPilot.certFile));
    return x509UserCert;
  }
  
  private void initalizeCACertsDir() throws IOException{
    if(caCertsDir==null || caCertsDir.equals("")){
      if(caCertsTmpdir==null){
        caCertsTmpdir = setupDefaultCACertificates();
      }
      // this adds all certificates in the dir to globus authentication procedures
      caCertsTmpdir = caCertsTmpdir.replaceAll("\\\\", "/");
      prop.setCaCertLocations(caCertsTmpdir);
      caCertsDir = caCertsTmpdir;
    }
    else{
      prop.setCaCertLocations(GridPilot.caCertsDir);
      caCertsTmpdir = GridPilot.caCertsDir;
    }
    GridPilot.caCertsDir = caCertsDir;
    caCertsDirStr = (new File(clearTildeLocally(clearFile(caCertsDir)))).getAbsolutePath();
    CoGProperties.setDefault(prop);
    Debug.debug("COG defaults now:\n"+CoGProperties.getDefault(), 3);
    Debug.debug("COG defaults file:\n"+CoGProperties.configFile, 3);
  }
  
  private void initalizeVomsDir() {
    if(GridPilot.vomsDir==null || GridPilot.vomsDir.equals("")){
      GridPilot.vomsDir = setupDefaultVomsDir();
      vomsDirStr = GridPilot.vomsDir;
    }
  }
  
  public /*synchronized*/ GSSCredential getGridCredential(){
    if(gridProxyInitialized){
      Debug.debug("Grid proxy already initialized. "+credential, 2);
      return credential;
    }
    synchronized(gridProxyInitialized){
      // avoids that dozens of popups open if
      // you submit dozen of jobs and proxy not initialized
      try{
        if(credential==null || credential.getRemainingLifetime()<GridPilot.proxyTimeLeftLimit ||
            !getProxyFile().exists()){
          Debug.debug("Initializing credential", 3);
          initGridProxy();
          Debug.debug("Initialized credential", 3);
          gridProxyInitialized = Boolean.TRUE;
          if(credential!=null){
            Debug.debug("Initialized credential"+credential.getRemainingLifetime()+
                ":"+GridPilot.proxyTimeLeftLimit, 3);
          }
        }
        else{
          Debug.debug("Grid proxy already initialized. "+credential.getRemainingLifetime(), 2);
          gridProxyInitialized = Boolean.TRUE;
        }
        // set the proxy default location
        prop.setProxyFile(getProxyFile().getAbsolutePath());
        CoGProperties.setDefault(prop);
      }
      catch(Exception e){
        Debug.debug("ERROR: could not get grid credential", 1);
        e.printStackTrace();
      }
    }
    return credential;
  }
  
  private String setupDefaultCACertificates() throws IOException {
    try{
      // get a temp name
      File tmpFile = File.createTempFile(/*prefix*/"GridPilot-certificates", /*suffix*/"");
      String tmpDir = tmpFile.getAbsolutePath();
      tmpFile.delete();
      LocalStaticShell.mkdirs(tmpDir);
      // have the diretory deleted on exit
      GridPilot.addTmpFile(tmpDir, new File(tmpDir));
      // fill the directory with certificates from resources/certificates
      SSL.copyDefaultCACertificates(tmpDir, super.getClass());
      return tmpDir;
    }
    catch(IOException e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "ERROR: could not setup CA certificates. " +
          "Grid authentication will not work.", e);
      //e.printStackTrace();
      throw e;
    }
  }
  
  private String setupDefaultVomsDir() {
    try{
      // try and see if SSl.class was loaded from a jar
      boolean fromJar = false;
      try{
        fromJar = MyUtil.listFromJAR("/resources/vomsdir", this.getClass(), false).size()>0;
      }
      catch(Exception e){
        Debug.debug("this class NOT loaded from jar.", 2);
      }
      // if so, extract vomsdir to the tmp dir
      if(fromJar){
        // get a temp name
        File tmpFile = File.createTempFile(/*prefix*/"GridPilot-vomsdir", /*suffix*/"");
        String tmpDir = tmpFile.getAbsolutePath();
        tmpFile.delete();
        tmpFile.mkdirs();
        MyUtil.extractFromJAR("resources/vomsdir", tmpFile, super.getClass());
        // have the diretory deleted on exit
        GridPilot.addTmpFile(tmpDir, new File(tmpDir));
        String vDir = (new File(tmpFile, "vomsdir")).getAbsolutePath();
        Debug.debug("Created tmp VOMS dir "+vDir, 3);
        return vDir;
      }
      // otherwise, just use the directory
      else{
        URL dirUrl = super.getClass().getResource("/resources/vomsdir");
        Debug.debug("Using default VOMS dir "+dirUrl.getPath(), 3);
        return dirUrl.getPath();
      }
    }
    catch(IOException e){
      GridPilot.getClassMgr().getLogFile().addMessage(
          "ERROR: could not setup VOMS directory. " +
          "Grid authentication may not work.", e);
      e.printStackTrace();
      return null;
    }
  }
  
  private String [] getFirstCheckCredentials(){
    String [] credentials;
    String password;
    if(GridPilot.keyPassword!=null){
      password = GridPilot.keyPassword;
    }
    else{
      password = "";
    }
    // Check if we are using test credentials
    String dn = null;
    try{
      dn = MyUtil.getDN(GridPilot.certFile);
    }
    catch(Exception ee){
      //ee.printStackTrace();
    };
    if(dn!=null && dn.equals(TEST_CERTIFICATE_DN)){
      // Override anything that might have been set
      password = TEST_KEY_PASSWORD;
    }
    credentials = new String [] {
        password,
        clearTildeLocally(clearFile(GridPilot.keyFile)),
        clearTildeLocally(clearFile(GridPilot.certFile))};
    Debug.debug("Trying with credentials "+MyUtil.arrayToString(credentials), 2);
    return credentials;
  }
  
  private void decryptPrivateKey() throws IOException {
    if(sslInitialized){
      Debug.debug("SSL already initialized. "+credential, 2);
      return;
    }
    String [] credentials = null;
    for(int i=0; i<=4; ++i){
      // First see if we can decrypt using any supplied password or "".
      if(GridPilot.keyFile!=null && !GridPilot.keyFile.equals("") &&
          GridPilot.certFile!=null && !GridPilot.certFile.equals("") &&
          i==0){
        credentials = getFirstCheckCredentials();
      }
      // Otherwise, ask for password
      else{
        try{
          credentials = askForPassword(GridPilot.keyFile, GridPilot.certFile,
              GridPilot.keyPassword);
        }
        catch(IllegalArgumentException e){
          // cancelling
          e.printStackTrace();
          break;
        }
      }
      try{
        BouncyCastleOpenSSLKey key = new BouncyCastleOpenSSLKey(credentials[1]);
        // decrypt key with password
        Debug.debug("Decrypting key...", 3);
        if(key.isEncrypted()){
            key.decrypt(credentials[0]);
          }
        // Keep password in memory - needed by mysql plugin
        GridPilot.certFile = credentials[2];
        GridPilot.keyFile = credentials[1];
        Debug.debug("Setting grid password to "+credentials[0], 3);
        GridPilot.keyPassword = credentials[0];
      }
      catch(Exception e){
        e.printStackTrace();
        continue;
      }
      return;
    }
    throw new IOException("ERROR: could not decrypt private key");
  }

  private int getProxyType(){
    // This works with ARC and gLite (well, would if it had VOMS extensions)
    //int proxyType = GSIConstants.DELEGATION_FULL;
    if(GridPilot.proxyType.equalsIgnoreCase(PROXY_TYPE_OLD)){
      return GSIConstants.GSI_2_PROXY;
    }
    else if(GridPilot.proxyType.equalsIgnoreCase(PROXY_TYPE_GLOBUS)){
      return GSIConstants.GSI_3_IMPERSONATION_PROXY;
    }
    else if(GridPilot.proxyType.equalsIgnoreCase(PROXY_TYPE_RFC)){
      return GSIConstants.GSI_4_IMPERSONATION_PROXY;
    }
    // default
    return GSIConstants.GSI_4_IMPERSONATION_PROXY;
  }
  
  private int getVomsProxyType(){
    if(GridPilot.proxyType.equalsIgnoreCase(PROXY_TYPE_OLD)){
      // This works with gLite and ARC
      return VomsProxyFactory.OID_OLD;
    }
    else if(GridPilot.proxyType.equalsIgnoreCase(PROXY_TYPE_GLOBUS)){
      return VomsProxyFactory.OID_GLOBUS;
    }
    else if(GridPilot.proxyType.equalsIgnoreCase(PROXY_TYPE_RFC)){
      return VomsProxyFactory.OID_RFC820;
    }
    // default
    return GSIConstants.GSI_4_IMPERSONATION_PROXY;
  }
  
  private void initGridProxy() throws IOException, GSSException{
    
    ExtendedGSSManager manager = (ExtendedGSSManager) ExtendedGSSManager.getInstance();
    File proxy = getProxyFile();
        
    // first just try and load proxy file from default location
    try{
      if(proxy.exists()){
        byte [] data = new byte[(int) proxy.length()];
        FileInputStream in = new FileInputStream(proxy);
        Debug.debug("reading proxy "+proxy.getAbsolutePath(), 3);
        in.read(data);
        in.close();
        credential = 
          manager.createCredential(data,
                                   ExtendedGSSCredential.IMPEXP_OPAQUE,
                                               GSSCredential.DEFAULT_LIFETIME,
                                               //GridPilot.proxyTimeValid,
                                               null, // use default mechanism - GSI
                                               GSSCredential.INITIATE_AND_ACCEPT);
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    // if credential ok, return
    if(credential!=null && credential.getRemainingLifetime()>=GridPilot.proxyTimeLeftLimit){
      Debug.debug("proxy ok", 3);
      return;
    }
    // if no valid proxy, init
    else{
      Debug.debug("proxy not ok: "+credential+": "+
          (credential!=null ? credential.getRemainingLifetime() : 0)+"<-->"+
          GridPilot.proxyTimeLeftLimit, 3);
      // Create new proxy
      Debug.debug("creating new proxy", 3);
      String [] credentials = null;
      GlobusCredential cred = null;
      FileOutputStream out = null;
      /**
       * Query for password max 4 times in total. If VOMS server is specified in
       * config file, try max 2 times to create VOMS proxy, then max 2 times to
       * create normal proxy.
       */
      for(int i=0; i<=4; ++i){
        // First see if we can decrypt using any supplied password or "".
        if(GridPilot.keyFile!=null && !GridPilot.keyFile.equals("") &&
            GridPilot.certFile!=null && !GridPilot.certFile.equals("") &&
            i==0){
          credentials = getFirstCheckCredentials();
        }
        // Otherwise, ask for password
        else{
          try{
            credentials = askForPassword(GridPilot.keyFile, GridPilot.certFile,
                GridPilot.keyPassword);
          }
          catch(IllegalArgumentException e){
            // cancelling
            e.printStackTrace();
            break;
          }
        }
        try{
          Debug.debug("Creating proxy, "+arrayToString(credentials), 3);
          if(GridPilot.vomsServerURL==null || GridPilot.vomsServerURL.equals("") ||
              i>1){
            /* Old implementation - no VOMS attributes. */
            cred = createProxy(credentials[1], credentials[2], credentials[0],
                GridPilot.proxyTimeValid, GridPilot.PROXY_STRENGTH, getProxyType());
            credential = new GlobusGSSCredentialImpl(cred, GSSCredential.INITIATE_AND_ACCEPT);
          }
          else{
            // This works with gLite and ARC
            VomsProxyFactory vpf = new VomsProxyFactory(VomsProxyFactory.CERTIFICATE_PEM,
                GridPilot.vomsServerURL, GridPilot.vo, credentials[0], proxy.getAbsolutePath(),
                certFileStr, keyFileStr, caCertsDirStr, vomsDirStr,
                GridPilot.proxyTimeValid, VomsProxyFactory.DELEGATION_FULL,
                getVomsProxyType(),
                GridPilot.fqan);
            credential = vpf.createProxy();
            if(credential instanceof GlobusGSSCredentialImpl) {
              cred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
            }
          }
          // Keep password in memory - needed by mysql plugin
          Debug.debug("Setting grid password to "+credentials[0], 3);
          GridPilot.keyPassword = credentials[0];
        }
        catch(Exception e){
          e.printStackTrace();
          continue;
        }
        try{
          // if we managed to create proxy, save it to default location
          out = new FileOutputStream(proxy);
          cred.save(out);
          out.close();
          return;
        }
        catch(Exception e){
          Debug.debug("ERROR: problem saving proxy. "+e.getMessage(), 3);
          e.printStackTrace();
          break;
        }          
      }
      throw new IOException("ERROR: could not initialize grid proxy");
    }
  }

  /**
   * Puts up a password dialog, asking for the key and certificate locations and
   * the password to decrypt the key.
   * @param keyFile
   * @param certFile
   * @param password
   * @return password, key location, certificate location.
   */
  private static String [] askForPassword(String keyFile, String certFile, String password){
    
    final JPanel panel = new JPanel(new GridBagLayout());
    JTextPane tp = new JTextPane();
    tp.setText("");
    tp.setEditable(false);
    tp.setOpaque(false);
    tp.setBorder(null);
    
    if(keyFile.startsWith("~")){
      try{
        keyFile = System.getProperty("user.home") + File.separator +
        keyFile.substring(2);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }
    if(certFile.startsWith("~")){
      try{
        certFile = System.getProperty("user.home") + File.separator +
        certFile.substring(2);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }

    final JPasswordField passwordField = new JPasswordField(password, 24);
    final JTextField keyField = new JTextField(keyFile, 24);
    final JTextField certField = new JTextField(certFile, 24);

    panel.add(tp, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
        GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(new JLabel("Password:"),
        new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
        GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(passwordField, new GridBagConstraints(1, 1, 1, 1, 1.0, 1.0,
        GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5),
        0, 0));
        
    ImageIcon browseIcon;
    URL imgURL=null;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "folder_blue_open.png");
      browseIcon = new ImageIcon(imgURL);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.resourcesPath + "folder_blue_open.png", 3);
      browseIcon = new ImageIcon();
    }
    
    JButton bBrowse1 = new JButton(browseIcon);
    bBrowse1.setToolTipText("browse file system");
    bBrowse1.setPreferredSize(new java.awt.Dimension(22, 22));
    bBrowse1.setSize(new java.awt.Dimension(22, 22));
    
    JButton bBrowse2 = new JButton(browseIcon);
    bBrowse2.setToolTipText("browse file system");
    bBrowse2.setPreferredSize(new java.awt.Dimension(22, 22));
    bBrowse2.setSize(new java.awt.Dimension(22, 22));
    
    bBrowse1.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        MyUtil.launchCheckBrowser(null, MyUtil.CHECK_URL, keyField, true, true, false, false, false);
      }
    });
    bBrowse2.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent me){
        MyUtil.launchCheckBrowser(null, MyUtil.CHECK_URL, certField, true, true, false, false, false);
      }
    });
    
    JPanel jpk = new JPanel(new BorderLayout());
    JPanel jpKey = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
    jpKey.add(bBrowse1);
    jpk.add(new JLabel("Key: "), BorderLayout.WEST);
    jpk.add(new JLabel(""), BorderLayout.CENTER);
    jpk.add(jpKey, BorderLayout.EAST);

    JPanel jpc = new JPanel(new BorderLayout());
    JPanel jpCert = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
    jpCert.add(bBrowse2);
    jpc.add(new JLabel("Certificate: "), BorderLayout.WEST);
    jpc.add(new JLabel(""), BorderLayout.CENTER);
    jpc.add(jpCert, BorderLayout.EAST);

    panel.add(jpk,
        new GridBagConstraints(0, 2, 1, 1, 1.0, 1.0,
        GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5),
        0, 0)
      );
    panel.add(keyField, new GridBagConstraints(1, 2, 1, 1, 1.0, 1.0,
        GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5),
        0, 0));

    panel.add(jpc,
        new GridBagConstraints(0, 3, 1, 1, 1.0, 1.0,
        GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5),
        0, 0));
    panel.add(certField, new GridBagConstraints(1, 3, 1, 1, 1.0, 1.0,
        GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL, new Insets(5, 5, 5, 5),
        0, 0));
    
    Debug.debug("showing dialog", 3);
    
    if(GridPilot.splash!=null){
      GridPilot.splash.hide();
    }
    
    // TODO: doens't work... Cannot set focus in password field.
    /*SwingUtilities.invokeLater(
        new Runnable(){
          public void run(){
            passwordField.requestFocusInWindow();
          }
        }
    );
    passwordField.requestFocusInWindow();*/
    int choice = JOptionPane.showConfirmDialog(JOptionPane.getRootFrame(), panel,
        "Enter grid password", JOptionPane.OK_CANCEL_OPTION);
    Debug.debug("showing dialog done", 3);
    
    if(GridPilot.splash!=null){
      GridPilot.splash.show();
    }
    
    if(choice!=JOptionPane.OK_OPTION){
      throw new IllegalArgumentException("Cancelling");
    }
    else{
      return new String [] {
          new String(passwordField.getPassword()),
          clearTildeLocally(clearFile(keyField.getText())),
          clearTildeLocally(clearFile(certField.getText()))
          };
    }
  }

  private GlobusCredential createProxy(
      String userKeyFilename,
      String userCertFilename,
      String password,
      int lifetime,
      int strength,
      int type)throws IOException, GeneralSecurityException{
    OpenSSLKey key;

    key = new BouncyCastleOpenSSLKey(userKeyFilename);
    // This was an vain attempt to get gLite/WMProxy to work...
    //System.setProperty("org.globus.gsi.version", "3");
    // get user certificate
    GridPilot.certFile = userCertFilename;
    GridPilot.keyFile = userKeyFilename;
    X509Certificate userCert = getX509UserCert();
    return createProxy(key, userCert, password, lifetime, strength, type);

  }

  private static GlobusCredential createProxy(OpenSSLKey key,
     X509Certificate userCert, String password, int lifetime, int strength,
     int type)
     throws InvalidKeyException, GeneralSecurityException{

    // decrypt key with password
    Debug.debug("Decrypting key...", 3);
    if(key.isEncrypted()){
        key.decrypt(password);
      }

    // factory for proxy generation
    Debug.debug("Creating factory for proxy generation...", 3);
    BouncyCastleCertProcessingFactory factory = BouncyCastleCertProcessingFactory.getDefault();

    Debug.debug("Creating credentials...", 3);
    GlobusCredential myCredentials = factory.createCredential(
        new X509Certificate[] {userCert}, key.getPrivateKey(), strength, lifetime,
            type, (X509ExtensionSet) null);
    return myCredentials;
  }

  /**
   * Returns the file holding the grid (X509) proxy file.
   * If no location has been specified in the preferences,
   * the default location is used.
   * 
   * @return a File object representing the proxy file
   */
  public static File getProxyFile(){
    String proxyDirectory = clearTildeLocally(GridPilot.proxyDir);
    if(proxyDirectory!=null && (new File(proxyDirectory)).exists() &&
        (new File(proxyDirectory)).isDirectory()){
      return new File(proxyDirectory+"/x509up_"+System.getProperty("user.name"));
    }
    return new File("/tmp/x509up_"+System.getProperty("user.name"));
  }

  /**
   * Get the DN of the grid certificate.
   * Here we use the proxy to get it. This means that
   * a proxy will be created if needed.
   * Globus uses the format /C=.../.../...
   */
  public String getGridSubject0(){
    String subject = null;
    try{
      GSSCredential credential = getGridCredential();
      GlobusCredential globusCred = null;
      if(credential instanceof GlobusGSSCredentialImpl){
        globusCred = ((GlobusGSSCredentialImpl)credential).getGlobusCredential();
      }
      Debug.debug("getting identity", 3);
      subject = globusCred.getIdentity().trim();
      Debug.debug("--->"+subject, 3);
    }
    catch(Exception nsae){
      String error = "ERROR: could get grid user subject. "+nsae.getMessage();
      nsae.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage(error, nsae);
    }
    return subject;
  }
  
  /**
   * Get the DN of the grid certificate.
   * Here we simply use the certificate - no proxy involved.
   * Java uses the format ...,...,C=...
   * We translate to the format /C=.../.../...
   * Attention: this may go wrong if the DN contains slashes and/or commas...
   * 
   * @return the DN (subject) of the active grid certificate
   */
  public String getGridSubject(){
    String subject = null;
    try{
      Debug.debug("getting identity", 3);
      X509Certificate userCert = getX509UserCert();
      subject = userCert.getSubjectX500Principal().getName().trim();
      Debug.debug("--->"+subject, 3);
      String [] items = split(subject, ",");
      String [] newItems = new String[items.length];
      int j = 0;
      for(int i=items.length-1; i>-1; --i){
        newItems[j] = items[i];
        ++j;
      }
      subject = "/"+arrayToString(newItems, "/");
      Debug.debug("returning subject "+subject, 3);
    }
    catch(Exception nsae){
      String error = "ERROR: could get grid user subject. "+nsae.getMessage();
      nsae.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage(error, nsae);
    }
    return subject;
  }
  
  /**
   * Generate a unique string, from the user's grid certificate subject,
   * which is usable as a MySQL user name.
   * 
   * @return the DN of the currently active grid (X509) certificate
   */
  public String getGridDatabaseUser(){
    String user = null;
    try{
      String subject = getGridSubject();
      
      AbstractChecksum checksum = null;
      checksum = JacksumAPI.getChecksumInstance("cksum");
      
      /*
       * It would be nicer to use the openssl certificate hash instead
       * of the cksum of the subject, but it seems not possible in
       * practice.
       * 
       * From /openssl/crypto/x509/x509_cmp.c.
       * Without DES MD5 encoding we will not get the right hash.
       * The missing method (c++, from openldap):
       * EVP_Digest(x->bytes->data, x->bytes->length, md, NULL, EVP_md5(), NULL);
       */
      /*
      Debug.debug("Issuer: "+ globusCred.getIssuer(), 3);
      Debug.debug("Identity: "+globusCred.getIdentity(), 3);
      Debug.debug("Subject DN: "+
          globusCred.getIdentityCertificate().getSubjectDN(), 3);         
      AbstractChecksum cs = JacksumAPI.getChecksumInstance("md5");
      cs.update(globusCred.getIdentity().getBytes());
      byte md[] = new byte[16];
      md = cs.getByteArray();
      long ret = ( (md[0])|(md[1]<<8L)|
          (md[2]<<16L)|(md[3]<<24L)
          )&0xffffffffL;
      Debug.debug("Hash: "+ret, 3);
      //Debug.debug("Hash: "+Long.toHexString(Long.parseLong(
      //    cs.getFormattedValue(), 10)), 2);
      Debug.debug("Wanted Hash: "+
          Long.valueOf("806d2203", 16), 3);
      */
      checksum.update(subject.getBytes());
      user = checksum.getFormattedValue();
      Debug.debug("Using user name from cksum of grid subject: "+user, 2);
    }
    catch(Exception nsae){
      String error = "ERROR: could not generate grid user name. "+nsae.getMessage();
      nsae.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage(error, nsae);
      return null;
    }
    return user;
  }
  
  /**
   * The same method as above, except for using getGridSubject1 instead
   * of getGridSubject.
   */
  public String getGridDatabaseUser0(){
    String user = null;
    try{
      String subject = getGridSubject0();
      
      AbstractChecksum checksum = null;
      checksum = JacksumAPI.getChecksumInstance("cksum");
      
      checksum.update(subject.getBytes());
      user = checksum.getFormattedValue();
      Debug.debug("Using user name from cksum of grid subject: "+user, 2);
    }
    catch(Exception nsae){
      String error = "ERROR: could not generate grid user name. "+nsae.getMessage();
      nsae.printStackTrace();
      GridPilot.getClassMgr().getLogFile().addMessage(error, nsae);
      return null;
    }
    return user;
  }

}
