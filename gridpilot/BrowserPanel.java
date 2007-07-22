package gridpilot;

import java.awt.*;
import java.awt.event.*;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;

import javax.swing.*;
import javax.swing.plaf.basic.BasicEditorPaneUI;
import javax.swing.text.Document;
import javax.swing.text.JTextComponent;
import javax.swing.text.html.*;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Vector;

import org.globus.ftp.exception.FTPException;
import org.globus.util.GlobusURL;
import org.safehaus.uuid.UUIDGenerator;

import gridpilot.ftplugins.gsiftp.GSIFTPFileTransfer;
import gridpilot.ftplugins.https.HTTPSFileTransfer;
import gridpilot.ftplugins.https.MyUrlCopy;
import gridpilot.ftplugins.sss.SSSFileTransfer;


/**
 * Box showing URL.
 */
public class BrowserPanel extends JDialog implements ActionListener{

  private static final long serialVersionUID = 1L;
  private JPanel panel = new JPanel(new BorderLayout());
  private JButton bOk = new JButton();
  private JButton bNew = new JButton();
  private JButton bUpload = new JButton();
  private JButton bDownload = new JButton();
  private JButton bRegister = new JButton();
  private JButton bSave = new JButton();
  protected JButton bCancel = new JButton();
  private JLabel currentUrlLabel = new JLabel("");
  private JTextField jtFilter = new JTextField("", 24);
  private JCheckBox jcbFilter = new JCheckBox();
  private JPanel pButton = new JPanel(new FlowLayout());
  private JEditorPane ep = new JEditorPane();
  private StatusBar statusBar = null;
  private String thisUrl;
  private String baseUrl;
  //private String origUrl;
  private boolean withFilter = false;
  private boolean withNavigation = false;
  protected String lastURL = null;
  protected String [] lastUrlList = null;
  protected String [] lastSizesList = null;
  private String currentUrlString = "";
  private JComboBox currentUrlBox = null;
  private GSIFTPFileTransfer gsiftpFileTransfer = null;
  private HTTPSFileTransfer httpsFileTransfer = null;
  private SSSFileTransfer sssFileTransfer = null;
  private boolean ok = true;
  private boolean saveUrlHistory = false;
  private boolean doingSearch = false;
  private JComponent jBox = null;
  private boolean localFS = false;
  private JPopupMenu popupMenu = new JPopupMenu();
  private JMenuItem miDownload = new JMenuItem("Download file");
  private JMenuItem miDelete = new JMenuItem("Delete file");
  private JMenu miRegister = new JMenu("Register file");
  // Menu to popup when clicking "Register all"
  private JPopupMenu bmiRegister = new JPopupMenu("Register all files");
  // File registration semaphor
  private boolean registering = false;
  private JComponent dsField = new JTextField(TEXTFIELDWIDTH);
  // Keep track of which files we are listing.
  private Vector listedUrls = null;
  private Vector listedSizes = null;
  private boolean allowRegister = true;
  private HashSet excludeDBs = new HashSet();
  
  public static int HISTORY_SIZE = 15;
  private static int MAX_FILE_EDIT_BYTES = 100000;
  private static int TEXTFIELDWIDTH = 32;
  private static int HTTP_TIMEOUT = 10000;

  public BrowserPanel(Frame parent, String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS) throws Exception{
    super(parent);
    init(parent, title, url, _baseUrl, modal, _withFilter, _withNavigation, _jBox, _filter,
        _localFS, true, true);
  }
  
  public BrowserPanel(Window parent, String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS, boolean cancelEnabled, boolean registrationEnabled) throws Exception{
    init(parent, title, url, _baseUrl, modal, _withFilter, _withNavigation, _jBox, _filter,
        _localFS, cancelEnabled, registrationEnabled);
    Debug.debug("Setting default cursor", 2);
  }

  public BrowserPanel(String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS) throws Exception{
    init(null, title, url, _baseUrl, modal, _withFilter, _withNavigation, _jBox, _filter,
        _localFS, true, true);
  }
  
  public void init(Window parent, String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS, boolean cancelEnabled, boolean registrationEnabled) throws Exception{
    baseUrl = _baseUrl;
    //origUrl = url;
    withFilter = _withFilter;
    withNavigation = _withNavigation;
    jBox = _jBox;
    localFS = _localFS;
    
    if(_filter!=null && !_filter.equals("")){
      jtFilter.setText(_filter);
    }
    
    setModal(modal);
    
    if(!localFS){
      gsiftpFileTransfer = (GSIFTPFileTransfer) GridPilot.getClassMgr().getFTPlugin("gsiftp");
      httpsFileTransfer = (HTTPSFileTransfer) GridPilot.getClassMgr().getFTPlugin("https");
      sssFileTransfer = (SSSFileTransfer) GridPilot.getClassMgr().getFTPlugin("sss");
    }
    
    String urlHistory = null;
    Debug.debug("browser history file: "+GridPilot.browserHistoryFile, 2);
    if(GridPilot.browserHistoryFile!=null && !GridPilot.browserHistoryFile.equals("")){
      if(GridPilot.browserHistoryFile.startsWith("~")){
        String homeDir = System.getProperty("user.home");
        if(!homeDir.endsWith(File.separator)){
          homeDir += File.separator;
        }
        GridPilot.browserHistoryFile = homeDir+GridPilot.browserHistoryFile.substring(1);
      }
      try{
        if(!LocalStaticShellMgr.existsFile(GridPilot.browserHistoryFile)){
          Debug.debug("trying to create file", 2);
          LocalStaticShellMgr.writeFile(GridPilot.browserHistoryFile, "", false);
        }
        urlHistory = LocalStaticShellMgr.readFile(GridPilot.browserHistoryFile);
        saveUrlHistory = true;
      }
      catch(Exception e){
        Debug.debug("WARNING: could not use "+GridPilot.browserHistoryFile+
            " as history file.", 1);
        GridPilot.browserHistoryFile = null;
        urlHistory = null;
      }
    }
    
    if((GridPilot.getClassMgr().getUrlList()==null ||
        GridPilot.getClassMgr().getUrlList().size()==0) &&
        urlHistory!=null && !urlHistory.equals("")){
      BufferedReader in = null;
      try{
        Debug.debug("Reading file "+GridPilot.browserHistoryFile, 3);
        in = new BufferedReader(
          new InputStreamReader((new URL("file:"+GridPilot.browserHistoryFile)).openStream()));
      }
      catch(IOException ioe){
        Debug.debug("WARNING: could not use "+GridPilot.browserHistoryFile+
            " as history file.", 1);
        GridPilot.browserHistoryFile = null;
        urlHistory = null;
      }
      try{
        String line;
        int lineNumber = 0;
        while((line=in.readLine())!=null){
          ++lineNumber;
          line = line.replaceAll("\\r", "");
          line = line.replaceAll("\\n", "");
          line = URLDecoder.decode(line, "utf-8");
          GridPilot.getClassMgr().addUrl(line);
          Debug.debug("URL: "+line, 3);
        }
        in.close();
      }
      catch(IOException ioe){
        Debug.debug("WARNING: could not use "+GridPilot.browserHistoryFile+
            " as history file.", 1);
        GridPilot.browserHistoryFile = null;
        urlHistory = null;
      }
    }

    currentUrlBox = new JExtendedComboBox();
    currentUrlBox.setEditable(true);
    Dimension d = currentUrlBox.getPreferredSize();
    currentUrlBox.setPreferredSize(new Dimension(320, d.height));
    setUrl(url);
    
    try{
      initGUI(parent, title, url, cancelEnabled, registrationEnabled);
    }
    catch(IOException e){
      //e.printStackTrace();
      throw e;
    }
  }
  
  // These methods are of no use with modal BrowserPanels
  public void okSetEnabled(boolean _ok){
    ok = _ok;
    bOk.setEnabled(ok);
  }
  
  public void registerSetEnabled(boolean _ok){
    allowRegister = _ok;
    bRegister.setEnabled(_ok);
  }
  
  private void addUrlKeyListener(){
    // Listen for enter key in text field
    JTextComponent editor = (JTextComponent) currentUrlBox.getEditor().getEditorComponent();
    editor.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!doingSearch && e.getKeyCode()==KeyEvent.VK_ENTER){
          doingSearch = true;
          Debug.debug("Detected ENTER", 3);
          if(currentUrlBox.getEditor().getItem()!=null &&
              !currentUrlBox.getEditor().getItem().toString().equals("") ||
              currentUrlBox.getSelectedItem()!=null && 
              !currentUrlBox.getSelectedItem().toString().equals("")){
            statusBar.setLabel("Opening URL...");
            ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            MyThread t = (new MyThread(){
              public void run(){
                try{
                  setDisplay(currentUrlBox.getSelectedItem().toString());
                }
                catch(Exception ee){
                  statusBar.setLabel("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage());
                  Debug.debug("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage(), 1);
                  ee.printStackTrace();
                }
                doingSearch = false;
              }
            });     
            SwingUtilities.invokeLater(t);
          }
        }
      }
    });
  }
  
  private void addFilterKeyListener(){
    // Listen for enter key in text field
    jtFilter.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!getUrl().endsWith("/")){
          return;
        }
        if(!doingSearch && e.getKeyCode()==KeyEvent.VK_ENTER){
          doingSearch = true;
          if(!currentUrlBox.getSelectedItem().toString().equals("")){
            Debug.debug("Detected ENTER", 3);
            statusBar.setLabel("Opening URL...");
            ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            MyThread t = (new MyThread(){
              public void run(){
                try{
                  setDisplay(getUrl());
                }
                catch(Exception ee){
                  statusBar.setLabel("ERROR: could not filter "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage());
                  Debug.debug("ERROR: could not filter "+currentUrlBox.getSelectedItem().toString()+
                      ". "+ee.getMessage(), 1);
                  ee.printStackTrace();
                }
                doingSearch = false;
              }
            });
            SwingUtilities.invokeLater(t);
          }
        }
      }
    });
  }
  
  /**
   * Component initialization.
   * If parent is not null, it gets its cursor set to the default after loading url.
   * If cancelEnabled is false, the cancel button is disabled.
   */
  private void initGUI(final Window parent, String title, String url,
      boolean cancelEnabled, boolean registrationEnabled) throws Exception{
    
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    this.getContentPane().setLayout(new BorderLayout());
 
    Debug.debug("Creating BrowserPanel with baseUrl "+baseUrl, 3);
    
    requestFocusInWindow();
    this.setTitle(title);
    
    bOk.setText("OK");
    bOk.setToolTipText("Continue");
    bOk.addActionListener(this);
    
    bNew.setText("New");
    bNew.setToolTipText("Create new file");
    bNew.addActionListener(this);
    
    bUpload.setText("Put");
    bUpload.setToolTipText("Upload file");
    bUpload.addActionListener(this);
    
    bDownload.setText("Get all");
    bDownload.setToolTipText("Download all files in this directory");
    bDownload.addActionListener(this);
    
    bRegister.setText("Register all");
    bRegister.setToolTipText("Register all files in this directory");
    bRegister.addActionListener(this);
    
    bSave.setText("Save");
    bSave.setToolTipText("Save this document");
    bSave.addActionListener(this);
    
    bCancel.setText("Cancel");
    bCancel.setToolTipText("Go back to directory / close window");
    bCancel.addActionListener(this);

    if(jBox!=null){
      pButton.add(jBox);
    }
    pButton.add(bOk);
    pButton.add(bNew);
    pButton.add(bUpload);
    pButton.add(bDownload);
    pButton.add(bRegister);
    pButton.add(bSave);
    pButton.add(bCancel);
    panel.add(pButton, BorderLayout.SOUTH);
    
    bNew.setEnabled(false);
    bUpload.setEnabled(false);
    bDownload.setEnabled(false);
    bRegister.setEnabled(false);
    bSave.setEnabled(false);
    
    if(GridPilot.dbNames!=null){
      DBPluginMgr dbPluginMgr = null;
      for(int i=0; i<GridPilot.dbNames.length; ++i){
        dbPluginMgr = null;
        try{
          dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.dbNames[i]);
          if(dbPluginMgr==null || !dbPluginMgr.isFileCatalog()){
            throw new Exception();
          }
        }
        catch(Exception e){
          excludeDBs.add(Integer.toString(i));
          continue;
        }
        miRegister.add(new JMenuItem(GridPilot.dbNames[i]));
      }
      JMenuItem [] jmiRegisterAll = new JMenuItem[GridPilot.dbNames.length];
      for(int i=0; i<GridPilot.dbNames.length; ++i){
        if(excludeDBs.contains(Integer.toString(i))){
          continue;
        }
        jmiRegisterAll[i] = new JMenuItem(GridPilot.dbNames[i]);
        jmiRegisterAll[i].addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent ev){
            registerAll(((JMenuItem) ev.getSource()).getText());
          }
        });
        bmiRegister.add(jmiRegisterAll[i]);
      }
    }

    JScrollPane sp = new JScrollPane();

    sp.getViewport().add(ep);
    panel.add(sp, BorderLayout.CENTER);
    
    panel.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,
        Color.white, new Color(165, 163, 151)), ""/*title*/));

    this.getContentPane().add(panel, BorderLayout.CENTER);

    panel.setPreferredSize(new Dimension(520, 400));
    setResizable(true);
    
    JPanel topPanel = new JPanel(new GridBagLayout()); 
    
    if(withNavigation){
      ImageIcon homeIcon = null;
      URL imgURL = null;
      try{
        imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "folder_home2.png");
        homeIcon = new ImageIcon(imgURL);
      }
      catch(Exception e){
        Debug.debug("Could not find image "+ GridPilot.resourcesPath + "folder_home2.png", 3);
        //homeIcon = new ImageIcon();
      }
      ImageIcon enterIcon = null;
      imgURL=null;
      try{
        imgURL = GridPilot.class.getResource(GridPilot.resourcesPath + "key_enter.png");
        enterIcon = new ImageIcon(imgURL);
      }
      catch(Exception e){
        Debug.debug("Could not find image "+ GridPilot.resourcesPath + "key_enter.png", 3);
        //enterIcon = new ImageIcon();
      }
      JButton bHome = null;
      if(homeIcon!=null){
        bHome = new JButton(homeIcon);
      }
      else{
        bHome = new JButton("home");
      }
      bHome.setToolTipText("go to grid home-URL");
      bHome.setPreferredSize(new java.awt.Dimension(22, 22));
      bHome.setSize(new java.awt.Dimension(22, 22));
      bHome.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent me){
          MyThread t = (new MyThread(){
            public void run(){
              try{
                statusBar.setLabel("Opening URL...");
                setDisplay(GridPilot.gridHomeURL);
              }
              catch(Exception ee){
                statusBar.setLabel("ERROR: could not open "+GridPilot.gridHomeURL+
                    ". "+ee.getMessage());
                Debug.debug("ERROR: could not open "+GridPilot.gridHomeURL+
                    ". "+ee.getMessage(), 1);
                ee.printStackTrace();
              }
              doingSearch = false;
            }
          });     
          SwingUtilities.invokeLater(t);
        }
      });
      JButton bEnter = null;
      if(enterIcon!=null){
        bEnter = new JButton(enterIcon);
      }
      else{
        bEnter = new JButton();
      }
      bEnter.setToolTipText("go!");
      bEnter.setPreferredSize(new java.awt.Dimension(22, 22));
      bEnter.setSize(new java.awt.Dimension(22, 22));
      bEnter.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent me){
          statusBar.setLabel("Opening URL...");
          ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          MyThread t = (new MyThread(){
            public void run(){
              try{
                setDisplay(currentUrlBox.getSelectedItem().toString());
              }
              catch(Exception ee){
                statusBar.setLabel("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                    ". "+ee.getMessage());
                Debug.debug("ERROR: could not open "+currentUrlBox.getSelectedItem().toString()+
                    ". "+ee.getMessage(), 1);
                ee.printStackTrace();
              }
              doingSearch = false;
            }
          });     
          SwingUtilities.invokeLater(t);
        }
      });

      JPanel jpNavigation = new JPanel(new GridBagLayout());
      if(!GridPilot.firstRun){
        jpNavigation.add(bHome);
      }
      jpNavigation.add(new JLabel(" "));
      jpNavigation.add(new JLabel("URL: "));
      jpNavigation.add(currentUrlBox);
      topPanel.add(jpNavigation, new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
          GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
          new Insets(0, 5, 0, 5), 0, 0));
      jpNavigation.add(new JLabel(" "));
      jpNavigation.add(bEnter);
      GridPilot.getClassMgr().addUrl("");
      addUrlKeyListener();
    }
    
    if(withFilter){
      JPanel jpFilter = new JPanel(new GridBagLayout());      
      if(!withNavigation){
        topPanel.add(currentUrlLabel,
            new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(0, 5, 0, 5), 0, 0));
      }
      jpFilter.add(new JLabel("Filter: "));
      jpFilter.add(jtFilter);
      jpFilter.add(new JLabel(" Show hidden"));
      jpFilter.add(jcbFilter);
      topPanel.add(jpFilter, new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0,
          GridBagConstraints.WEST, GridBagConstraints.BOTH,
          new Insets(0, 5, 0, 5), 0, 0));
      topPanel.setPreferredSize(new Dimension(520, 56));
                
      panel.add(topPanel, BorderLayout.NORTH);

      addFilterKeyListener();
    }  
    
    if(withNavigation || withFilter){
      panel.add(topPanel, BorderLayout.NORTH);
    }
        
    //HTMLDocument d = new HTMLDocument();
    ep.addHyperlinkListener(new HyperlinkListener(){
      public void hyperlinkUpdate(final HyperlinkEvent e){
        if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
          if(e instanceof HTMLFrameHyperlinkEvent){
            ((HTMLDocument) ep.getDocument()).processHTMLFrameHyperlinkEvent(
               (HTMLFrameHyperlinkEvent) e);
          }
          else{
            //setUrl(e.getDescription());
            ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            statusBar.setLabel("Opening URL...");
            (new MyThread(){
              public void run(){
                try{
                  if(e.getURL()!=null){
                    setDisplay(e.getURL().toExternalForm());
                  }
                  else{
                    setDisplay(e.getDescription());
                  }
                }
                catch(Exception ioe){
                  ioe.printStackTrace();
                }
              }
            }).run();               
          }
        }
        else if(e.getEventType()==HyperlinkEvent.EventType.ENTERED){
          if(popupMenu.isShowing()){
            return;
          }
          String linkUrl = null;
          if(e.getURL()!=null){
            linkUrl = e.getURL().toExternalForm();
          }
          else if(e.getDescription()!=null){
            linkUrl = e.getDescription();
          }
          if(linkUrl==null){
            Debug.debug("No URL available.", 2);
            return;
          }
          final String url = linkUrl;
          statusBar.setLabel(url);
          if(bRegister.isEnabled() && !url.endsWith("/")){
            int ii = 0;
            for(int i=0; i<GridPilot.dbNames.length; ++i){
              if(excludeDBs.contains(Integer.toString(i))){
                continue;
              }
              Debug.debug("addActionListener "+Util.arrayToString(excludeDBs.toArray())+":"+i, 3);
              miRegister.getItem(ii).addActionListener(new ActionListener(){
                public void actionPerformed(ActionEvent ev){
                  Debug.debug("registerFile "+((JMenuItem) ev.getSource()).getText(), 3);
                  registerFile(url, ((JMenuItem) ev.getSource()).getText());
                }
              });
              ++ii;
            }
            if(allowRegister){
              popupMenu.add(miRegister);
            }
          }
          //if(bDownload.isEnabled()){
          if(!url.startsWith("http://") && url.indexOf("/..")<0){
            if(url.endsWith("/")){
              miDelete.setText("Delete directory");
            }
            else{
              miDownload.addActionListener(new ActionListener(){
                public void actionPerformed(ActionEvent ev){
                  downloadFile(url);
                }
              });
              popupMenu.add(miDownload);
            }
            miDelete.addActionListener(new ActionListener(){
              public void actionPerformed(ActionEvent ev){
                deleteFile(url);
              }
            });
            popupMenu.add(miDelete);
          }
        }
        else if(e.getEventType()==HyperlinkEvent.EventType.EXITED){
          statusBar.setLabel(" ");
          try{
            if(GridPilot.dbNames!=null){
              int ii = 0;
              for(int i=0; i<GridPilot.dbNames.length; ++i){
                if(excludeDBs.contains(Integer.toString(i))){
                  continue;
                }
                Debug.debug("addActionListener "+i, 3);
                ActionListener [] acls = miRegister.getItem(ii).getActionListeners();
                for(int j=0; j<acls.length; ++j){
                  miRegister.getItem(ii).removeActionListener(acls[j]);
                }
                ++ii;
              }
              ActionListener [] acls = miDownload.getActionListeners();
              for(int j=0; j<acls.length; ++j){
                miDownload.removeActionListener(acls[j]);
              }
              acls = miDelete.getActionListeners();
              for(int j=0; j<acls.length; ++j){
                miDelete.removeActionListener(acls[j]);
              }
              miDelete.setText("Delete file");
              popupMenu.remove(miRegister);
              popupMenu.remove(miDownload);
              popupMenu.remove(miDelete);
            }
          }
          catch(Exception ee){
            ee.printStackTrace();
          }
        }
      }
    });
    
    ep.addMouseListener(new java.awt.event.MouseAdapter() {
      public void mousePressed(MouseEvent e) {
        if (e.getButton()!=MouseEvent.BUTTON1) // right button
          popupMenu.show(e.getComponent(), e.getX(), e.getY());
      }
    });
    
    // Load the actual page
    statusBar = new StatusBar();
    statusBar.setLabel(" ");
    this.getContentPane().add(statusBar, BorderLayout.SOUTH);
    setDisplay(url);
    if(withNavigation){
      statusBar.setLabel("Type in URL and hit return");
    }
    
    // Fix up things if this was e.g. called from a wizard.
    if(parent!=null){
      Debug.debug("Resetting cursor", 2);
      parent.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    }
    if(!cancelEnabled){
      Debug.debug("Disabling cancel button", 2);
       bCancel.setEnabled(false);
    }
    if(!registrationEnabled){
      Debug.debug("Disabling registration", 2);
       registerSetEnabled(false);
    }
        
    ep.addPropertyChangeListener(new PropertyChangeListener(){
      public void propertyChange(PropertyChangeEvent event) {
        Debug.debug("Property changed: "+event.getPropertyName(), 3);
        if(event.getPropertyName().equalsIgnoreCase("document") ||
            event.getPropertyName().equalsIgnoreCase("page")){
          statusBar.setLabel("Page loaded");
          Debug.debug("Page loaded", 2);
        }
      }
    });
    
    panel.validate();
    sp.validate();
    ep.validate();
    pack();
    
    Dimension dim = new Dimension(panel.getPreferredSize().width + (jBox==null?50:300),
        panel.getPreferredSize().height);

    setSize(dim);
    setVisible(true);
    
    // Close window with ctrl+w
    jtFilter.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            cancel();
        }
      }
    });
    
    currentUrlBox.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            cancel();
        }
      }
    });
    
    ep.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!e.isControlDown()){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            cancel();
        }
      }
    });

    if(url!=null && !url.equals("")){
      setUrl(url);
    }

  }
  
  /**
   * Download a single file.
   */
  private void downloadFile(final String url){
    final File dir = Util.getDownloadDir(this);      
    if(dir==null){
      return;
    }
    //MyThread t = (new MyThread(){
      //public void run(){
        Debug.debug("Getting file : "+url+" -> "+dir.getAbsolutePath(), 3);
        try{
          statusBar.setLabel("Downloading "+url);
          TransferControl.download(url, dir, ep);
          statusBar.setLabel("Download done");
        }
        catch(Exception ioe){
          statusBar.setLabel("Download failed");
          ioe.printStackTrace();
          showError("Could not download "+url+" : "+ioe.getMessage());
        }
        try{
          ep.getDocument().putProperty(
              Document.StreamDescriptionProperty, null);
          setDisplay(thisUrl);
        }
        catch(Exception ioe){
          ioe.printStackTrace();
        }
      //}
    //});     
    //SwingUtilities.invokeLater(t);
  }
  
  private void showError(String str){
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    String title = "Browser error";
    try {
      confirmBox.getConfirm(title,
          str, new Object[] {"OK"});
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Choose a dataset and an LFN
   */
  private String [] selectLFNAndDS(String pfn, final DBPluginMgr dbPluginMgr,
      boolean disableNameField){
    // Construct suggestion for LFN
    String lfn = pfn.replaceFirst(".*/([^/]+)","$1");
    String confirmString =
      "Please choose the dataset in which you want to register the file(s);\n" +
      "then, optionally, type a name (logical file name ) to use to " +
      "identify the file(s) in the dataset.\n" +
      "If none is given, the name of the name of the physical file is used.";
    final JPanel jPanel = new JPanel(new GridBagLayout());
    jPanel.add(new JLabel("<html>"+confirmString.replaceAll("\n", "<br>")+"</html>"),
        new GridBagConstraints(0, 0, 2, 2, 0.0, 0.0,
            GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
            new Insets(5, 5, 5, 5), 0, 0));
    final JPanel dsRow = new JPanel(new BorderLayout());
    final JButton jbLookup = new JButton("Look up");
    dsRow.add(new JLabel("Dataset: "), BorderLayout.WEST);
    dsRow.add(dsField, BorderLayout.CENTER);
    dsRow.add(jbLookup, BorderLayout.EAST);
    dsRow.updateUI();
    jPanel.add(dsRow, new GridBagConstraints(0, 4, 1, 1, 0.0, 0.0,
        GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
        new Insets(10, 10, 10, 10), 0, 0));
    jPanel.updateUI();
    jbLookup.addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        if(e.getButton()!=MouseEvent.BUTTON1){
          return;
        }
        String idField = Util.getIdentifierField(dbPluginMgr.getDBName(), "dataset");
        String nameField = Util.getNameField(dbPluginMgr.getDBName(), "dataset");
        String str = Util.getJTextOrEmptyString(dsField);
        if(str==null || str.equals("")){
          return;
        }
        DBResult dbRes = dbPluginMgr.select("SELECT "+nameField+" FROM dataset" +
                (str!=null&&!str.equals("")?" WHERE "+nameField+" CONTAINS "+str:""),
            idField, false);
        dsField = new JExtendedComboBox();
        for(int i=0; i<dbRes.values.length; ++i){
          ((JExtendedComboBox) dsField).addItem(dbRes.getValue(i, nameField));
        }
        ((JExtendedComboBox) dsField).setEditable(true);
        dsField.updateUI();
        dsRow.removeAll();
        dsRow.add(new JLabel("Dataset: "), BorderLayout.WEST);
        dsRow.add(dsField, BorderLayout.CENTER);
        dsRow.add(jbLookup, BorderLayout.EAST);
        dsRow.updateUI();
        dsRow.add(dsField, BorderLayout.CENTER);
        dsRow.updateUI();
        dsRow.validate();
        jPanel.updateUI();
        jPanel.validate();
      }
    });
    jbLookup.setToolTipText("Search results for this request");
    
    JTextField lfnField = new JTextField(TEXTFIELDWIDTH);
    lfnField.setText(lfn);
    JPanel lfnRow = new JPanel(new BorderLayout());
    lfnRow.add(new JLabel("Logical file name: "), BorderLayout.WEST);
    lfnRow.add(lfnField, BorderLayout.CENTER);
    jPanel.add(lfnRow, new GridBagConstraints(0, 5, 1, 1, 0.0, 0.0,
        GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
        new Insets(10, 10, 10, 10), 0, 0));
    jPanel.validate();
    
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    
    if(disableNameField){
      lfnField.setEnabled(false);
    }
    
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Register file in dataset",
          jPanel, new Object[] {"OK", "Cancel"});
    }
    catch(Exception e){
      e.printStackTrace();
      return null;
    }
    if(choice!=0){
      return null;
    }
    return new String [] {Util.getJTextOrEmptyString(dsField), lfnField.getText()};
  }
  
  
  /**
   * Register a single file.
   */
  private void registerFile(final String url, final String dbName){
    if(registering){
      return;
    }
    MyThread t = (new MyThread(){
      public void run(){
        registering = true;
        try{
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
          String [] dsLfn = selectLFNAndDS(url, dbPluginMgr, false);
          if(dsLfn==null || dsLfn[0]==null){
            Debug.debug("Not registering", 2);
            registering = false;
            return;
          }
          String datasetName = dsLfn[0];
          String datasetID = dbPluginMgr.getDatasetID(datasetName);
          String lfn = null;
          if(dsLfn[1]==null || dsLfn[1].equals("")){
            // Use the physical file name (strip off the first ...://.../.../
            lfn = url.replaceFirst(".*/([^/]+)$", "$1");
          }
          else{
            lfn = dsLfn[1];
          }
          String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
          statusBar.setLabel("Registering "+url+" in "+dbName);
          // get the size if possible
          String size = null;
          if(lastSizesList!=null){
            for(int i=0; i<lastSizesList.length; ++i){
              if(lastUrlList[i].equals(url)){
                size = lastSizesList[i];
                break;
              }
            }
          }
          dbPluginMgr.registerFileLocation(
              datasetID, datasetName, uuid, lfn, url, size, null, false);
          statusBar.setLabel("Registration done");
          registering = false;
        }
        catch(Exception ioe){
          registering = false;
          statusBar.setLabel("Registration failed");
          ioe.printStackTrace();
        }
        try{
          ep.getDocument().putProperty(
              Document.StreamDescriptionProperty, null);
          setDisplay(thisUrl);
        }
        catch(Exception ioe){
          ioe.printStackTrace();
        }
      }
    });     
    SwingUtilities.invokeLater(t);
  }
  
  /**
   * Set the text of the navigation label or input field
   */
  private void setUrl(String url){
    try{
      url = URLDecoder.decode(url, "utf-8");
    }
    catch (UnsupportedEncodingException e){
      e.printStackTrace();
    }
    String newUrl = url.trim();
    GridPilot.getClassMgr().removeUrl("");
    Vector urlList = GridPilot.getClassMgr().getUrlList();
    newUrl = newUrl.replaceAll("\\\\", "/");
    newUrl = newUrl.replaceAll("file:C", "file:/C");
    
    // check if url is already in history and add if not
    boolean refresh = false;
    if(!urlList.contains(newUrl)){
      refresh = true;
      Debug.debug("Adding URL to history: "+newUrl, 2);
      if(urlList.size()>HISTORY_SIZE){
        Debug.debug("History size exceeded, removing first, "+
            urlList.size()+">"+HISTORY_SIZE, 2);
        GridPilot.getClassMgr().removeUrl(urlList.iterator().next().toString());
      }
      GridPilot.getClassMgr().addUrl(newUrl);
      Debug.debug("urlSet is now: "+Util.arrayToString(
          urlList.toArray(), " : "), 2);
    }

    if(refresh || currentUrlBox.getItemCount()==0 && urlList.size()>0){
      currentUrlBox.removeAllItems();
      urlList = GridPilot.getClassMgr().getUrlList();
      for(ListIterator it=urlList.listIterator(urlList.size()-1); it.hasPrevious();){
        currentUrlBox.addItem(it.previous().toString());
      }
      currentUrlBox.updateUI();
    }

    addUrlKeyListener();
    currentUrlString = url;
    if(withNavigation){
      currentUrlBox.setSelectedItem(url);
    }
    else{
      currentUrlLabel.setText(url);
    }
  }
  
  /**
   * Set the text of the navigation label or input field
   */
  private String getUrl(){
    return currentUrlString;
  }
  
  /**
   * Displays the URL, using the method corresponding to
   * the protocol of the URL.
   */
  private void setDisplay(String url) throws Exception{
    try{
      lastUrlList = null;
      lastSizesList = null;
      if(url.startsWith("file:/~")){
        url = "file:"+System.getProperty("user.home")+url.substring(7);
      }
      else if(url.startsWith("file:~")){
        url = "file:"+System.getProperty("user.home")+url.substring(6);
      }
      Debug.debug("Checking URL, "+url, 3);
      // browse remote web directory
      if((url.startsWith("http://") ||
          //url.startsWith("https://") ||
          url.startsWith("ftp://")) &&
         url.endsWith("/")){
        //setHttpDirDisplay(url);
        setHtmlDisplay(url);
      }
      // local directory
      else if((url.startsWith("/") || url.matches("\\w:.*") ||
          url.startsWith("file:")) &&
          url.endsWith("/")){
        setLocalDirDisplay(url);
      }
      // remote directory
      else if(url.startsWith("gsiftp://") &&
          url.endsWith("/")){
        setRemoteDirDisplay(url, gsiftpFileTransfer, "gsiftp");
      }
      else if(url.startsWith("https://") &&
          url.endsWith("/")){
        try{
          setRemoteDirDisplay(url, httpsFileTransfer, "https");
        }
        catch(Exception ee){
          ee.printStackTrace();
          //setHttpDirDisplay(url);
          setHtmlDisplay(url);
        }
      }
      else if(url.startsWith("sss://") &&
          url.endsWith("/")){
        setRemoteDirDisplay(url, sssFileTransfer, "sss");
      }
      // remote gsiftp text file
      else if(url.startsWith("gsiftp://") &&
          !url.endsWith("/") && /*!url.endsWith("htm") &&
          !url.endsWith("html") &&*/ !url.endsWith("gz") &&
          url.indexOf(".root")<0){
        if(!setRemoteTextEdit(url, gsiftpFileTransfer)){
          setRemoteFileConfirmDisplay(url, gsiftpFileTransfer);
        }
      }
      // remote https text file
      else if(url.startsWith("https://") &&
          !url.endsWith("/") && !url.endsWith("htm") &&
          !url.endsWith("html") && !url.endsWith("gz") &&
          url.indexOf(".root")<0){
        if(!setRemoteTextEdit(url, httpsFileTransfer)){
          setRemoteFileConfirmDisplay(url, httpsFileTransfer);
        }
      }
      // remote 3s text file
      else if(url.startsWith("sss://") &&
          !url.endsWith("/") && /*!url.endsWith("htm") &&
          !url.endsWith("html") &&*/ !url.endsWith("gz") &&
          url.indexOf(".root")<0){
        if(!setRemoteTextEdit(url, sssFileTransfer)){
          setRemoteFileConfirmDisplay(url, sssFileTransfer);
        }
      }
      // html document
      else if((url.endsWith("htm") ||
          url.endsWith("html")) &&
          (url.startsWith("http://") || url.startsWith("https://") ||
              url.startsWith("file:"))){
        setHtmlDisplay(url);
      }
      // tarball on disk or web server
      else if(url.endsWith("gz") &&
          (url.startsWith("http://") || url.startsWith("file:"))){
        setFileConfirmDisplay(url);
      }
      // tarball on gridftp server
      else if(url.endsWith("gz") &&
          (url.startsWith("gsiftp:/"))){
        setRemoteFileConfirmDisplay(url, gsiftpFileTransfer);
      }
      // tarball on https server
      else if(url.endsWith("gz") &&
          (url.startsWith("https:/"))){
        setRemoteFileConfirmDisplay(url, httpsFileTransfer);
      }
      // tarball on 3s server
      else if(url.endsWith("gz") &&
          (url.startsWith("sss:/"))){
        setRemoteFileConfirmDisplay(url, sssFileTransfer);
      }
      // text document on disk or web server
      else if(!url.endsWith("htm") &&
         !url.endsWith("html") &&
         !url.endsWith("/") &&
         (url.startsWith("http://") || url.startsWith("https://"))){
        try{
          setHttpTextDisplay(url);
        }
        catch(Exception e){
          //setHttpDirDisplay(url);
          setHtmlDisplay(url);
        }
      }
      // text document on disk
      else if(!url.endsWith("htm") &&
              !url.endsWith("html") &&
              !url.endsWith("/") &&
              (url.startsWith("file:") || url.startsWith("/") || url.matches("^\\w:.*"))
              ){
        if(url.matches("^\\w:.*")){
          Debug.debug("Fixing URL "+url, 2);
          url = "file:"+url;
        }
        if(!setLocalTextEdit(url)){
          setFileConfirmDisplay(url);
        }
      }
      // confirm that file exists
      else if(url.startsWith("http:") || url.startsWith("file:")){
        setFileConfirmDisplay(url);
      }
      else if(url.startsWith("https:")){
        setRemoteFileConfirmDisplay(url, httpsFileTransfer);
      }
      else if(url.startsWith("gsiftp:")){
        setRemoteFileConfirmDisplay(url, gsiftpFileTransfer);
      }
      else if(url.startsWith("sss:")){
        setRemoteFileConfirmDisplay(url, sssFileTransfer);
      }
      // blank page
      else if(url.equals("") && withNavigation){
        Debug.debug("Setting blank page", 2);
        ep.setEditable(false);
      }
      // unknown protocol
      else{
        throw(new IOException("Unknown protocol for "+url));
      }
      // reset cursor to default
      if(url.endsWith("/")){
        ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      }
      else if(url.equals("") || url.matches(" *")){
        ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        statusBar.setLabel(" ");
      }
    }
    catch(Exception e){
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      String msg = "Could not initialize panel with URL "+url+". "+e.getMessage();
      //showError(msg);
      Debug.debug(msg, 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display the text page file url.
   * The page can then be edited and saved (when clicking bSave).
   * Returns true if all is ok, false if the file is too big to
   * be edited directly.
   */
  private boolean setRemoteTextEdit(String url, FileTransfer ft) throws IOException,
     FTPException{
    Debug.debug("setRemoteTextEdit "+url, 3);
    jtFilter.setEnabled(false);
    File tmpFile = null;
    tmpFile = File.createTempFile("GridPilot-", ".txt");
    Debug.debug("Created temp file "+tmpFile, 3);
    try{
      long bytes = -1;
      try{
        bytes = ft.getFileBytes(new GlobusURL(url));
      }
      catch(Exception e){
        // Some gridftp servers (glite) list modification date as second field.
        // This will cause an exception when trying to parse as long.
        // Just assume the file is large.
        e.printStackTrace();
      }   
      if(bytes==-1 || bytes>MAX_FILE_EDIT_BYTES){
        //throw new IOException("File too big "+ft.getFileBytes(new GlobusURL(url)));
        tmpFile.delete();
        return false;
      }
      ft.getFile(new GlobusURL(url), tmpFile, statusBar);
    }
    catch(Exception e){
      Debug.debug("Could not read "+url, 1);
      e.printStackTrace();
      String error = "ERROR!\n\nThe file "+url+" could not be read. "+
      "\n\nIf it is a directory, please end with a /. "+
      e.getMessage();
      ep.setText(error);
      throw new IOException(error);
    }
    pButton.updateUI();
    try{
      Debug.debug("Reading temp file "+tmpFile.getAbsolutePath(), 3);
      BufferedReader in = new BufferedReader(
        new InputStreamReader((new URL("file:"+tmpFile.getAbsolutePath())).openStream()));
      String text = "";
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        text += line+"\n";
      }
      in.close();
      ep.setUI(new BasicEditorPaneUI());
      ep.setContentType("text/plain");
      ep.setText(text);
      ep.setEditable(true);
      //With this cursor, the caret doesn't blink...
      //ep.setCursor(new Cursor(Cursor.TEXT_CURSOR));
      ep.setCursor(new Cursor(Cursor.DEFAULT_CURSOR));
      ep.getCaret().setBlinkRate(500);
      ep.updateUI();
      pButton.updateUI();
      
      bSave.setEnabled(true);
      bOk.setEnabled(ok);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
      
      tmpFile.delete();

      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
      lastUrlList = new String [] {thisUrl};
      setUrl(thisUrl);
      statusBar.setLabel(" ");
    }
    catch(IOException e){
      Debug.debug("Could not set text editor for url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
    return true;
  }

  /**
   * Set the EditorPane to display the text page file url.
   * The page can then be edited and saved (when clicking bSave).
   * Returns true if all is ok, false if the file is too big to
   * be edited directly.
   */
  private boolean setLocalTextEdit(String url) throws IOException{
    Debug.debug("setTextEdit "+url, 2);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(true);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
      BufferedReader in = new BufferedReader(
        new InputStreamReader((new URL(url)).openStream()));
      String text = "";
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        if(lineNumber>3000){
          //throw new IOException("File too big");
          return false;
        }
        text += line+"\n";
      }
      in.close();
      ep.setUI(new BasicEditorPaneUI());
      ep.setContentType("text/plain");
      ep.setPage(url);
      ep.setText(text);
      ep.setEditable(true);
      // With this cursor, the caret doesn't blink...
      //ep.setCursor(new Cursor(Cursor.TEXT_CURSOR));
      ep.setCursor(new Cursor(Cursor.DEFAULT_CURSOR));
      ep.getCaret().setBlinkRate(500);
      ep.updateUI();
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
      setUrl(thisUrl);
      lastUrlList = new String [] {thisUrl};
      statusBar.setLabel(" ");
    }
    catch(IOException e){
      Debug.debug("Could not set text editor for url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
    return true;
  }

  /**
   * Set the EditorPane to display the text page web url.
   * The page cannot be edited.
   */
  private void setHttpTextDisplay(String url) throws IOException{
    Debug.debug("setHttpTextDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
      BufferedReader in = new BufferedReader(
        new InputStreamReader((new URL(url)).openStream()));
      String text = "";
      String line;
      int lineNumber = 0;
      while((line=in.readLine())!=null){
        ++lineNumber;
        if(lineNumber>3000){
          throw new IOException("File too big");
        }
        text += line+"\n";
      }
      in.close();
      ep.setPage(url);
      ep.setText(text);
      ep.setEditable(false);
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+url, 3);
      thisUrl = url;
      setUrl(thisUrl);
      lastUrlList = new String [] {thisUrl};
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    }
    catch(IOException e){
      Debug.debug("Could not set text editor for url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display the HTML page url.
   */
  private void setHtmlDisplay(String url) throws IOException{
    Debug.debug("setHtmlDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
      ep.setPage(url);
      ep.setEditable(false);
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+thisUrl, 3);
      thisUrl = url;
      setUrl(thisUrl);
      lastUrlList = new String [] {thisUrl};
    }
    catch(IOException e){
      Debug.debug("Could not set html display of url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

  /**
   * Set the EditorPane to display a confirmation or disconfirmation
   * of the existence of the URL url.
   */
  private void setFileConfirmDisplay(String url) throws IOException{
    Debug.debug("setFileConfirmDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
      ep.setEditable(false);
      try {
        URLConnection connection = (new URL(url)).openConnection();
        DataInputStream dis;
        dis = new DataInputStream(connection.getInputStream());
              //while ((inputLine = dis.readLine()) != null){
              //    Debug.debug(inputLine, 3);
              //}
        dis.close();
        ep.setText("File found");
        Debug.debug("Setting thisUrl, "+thisUrl, 3);
        thisUrl = url;
      }
      catch(MalformedURLException me){
        me.printStackTrace();
        ep.setText("File "+url+" NOT found");
      }
      catch(IOException ioe){
        ioe.printStackTrace();
        ep.setText("File "+url+" NOT found");
      }
      pButton.updateUI();
    }
    catch(Exception e){
      Debug.debug("Could not set directory display of url "+url+". "+
         e.getMessage(), 1);
      //throw e;
    }
  }

  /**
   * Set the EditorPane to display a confirmation or disconfirmation
   * of the existence of the URL url.
   */
  private void setRemoteFileConfirmDisplay(String url, FileTransfer ft) throws IOException{
    Debug.debug("setRemoteFileConfirmDisplay "+url, 3);
    jtFilter.setEnabled(false);
    
    bSave.setEnabled(false);
    bNew.setEnabled(false);
    bUpload.setEnabled(false);
    bDownload.setEnabled(false);
    bRegister.setEnabled(false);
    ep.setEditable(false);

    try{
      try{
        long bytes = -1;
        try{
          bytes = ft.getFileBytes(new GlobusURL(url));
        }
        catch(Exception e){
          // Some gridftp servers (glite) list modification date as second field.
          // This will cause an exception when trying to parse as long.
          // Just assume the file is large.
          e.printStackTrace();
        }   
        if(bytes==0){
          throw new IOException("File is empty");
        }
        ep.setText("File found");
        Debug.debug("Setting thisUrl, "+url, 3);
        thisUrl = url;
        setUrl(thisUrl);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Could not read "+url, 1);
        ep.setText("ERROR!\n\nThe file "+url+" could not be read. "+
            e.getMessage());
      }
      pButton.updateUI();
      lastUrlList = new String [] {thisUrl};
    }
    catch(Exception e){
      Debug.debug("Could not set confirm display of "+url+". "+
         e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }
  
  // TODO: this method can be deleted
  private void setGsiftpConfirmDisplay(String url, FileTransfer ft) throws IOException{
    String localPath = null;
    String host = null;
    String hostAndPath = null;
    String port = "2811";
    String hostAndPort = null;
    String localDir = null;
    Debug.debug("Host+path: "+hostAndPath, 3);
    hostAndPort = hostAndPath.substring(0, hostAndPath.indexOf("/"));
    hostAndPath = url.substring(9);
    int colonIndex=hostAndPort.indexOf(":");
    if(colonIndex>0){
      host = hostAndPort.substring(0, hostAndPort.indexOf(":"));
      port = hostAndPort.substring(hostAndPort.indexOf(":")+1);
    }
    else{
      host = hostAndPort;
      port = "2811";
    }
    localPath = hostAndPath.substring(hostAndPort.length(), hostAndPath.length());
    localPath = localPath.replaceFirst("/[^\\/]*/\\.\\.", "");
    int lastSlash = localPath.lastIndexOf("/");
    if(lastSlash>0){
      localDir = localPath.substring(0, lastSlash);
    }
    else{
      localDir = "/";
    }
    Debug.debug("Host: "+host, 3);
    Debug.debug("Port: "+port, 3);
    Debug.debug("Path: "+localPath, 3);
    Debug.debug("Directory: "+localDir, 3);
    try{
      try{
        if(ft.getFileBytes(new GlobusURL(url))==0){
          throw new IOException("File is empty");
        }
        ep.setText("File found");
        Debug.debug("Setting thisUrl, "+url, 3);
        thisUrl = url;
        setUrl(thisUrl);
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Could not read "+localPath, 1);
        ep.setText("ERROR!\n\nThe file "+localPath+" could not be read. "+
            e.getMessage());
      }
      pButton.updateUI();
      lastUrlList = new String [] {thisUrl};
    }
    catch(Exception e){
      Debug.debug("Could not set confirm display of "+localPath+". "+
         e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Set the EditorPane to display a directory listing
   * of the shell path fsPath.
   */
  private void setLocalDirDisplay(String fsPath) throws IOException{
    jtFilter.setEnabled(true);
    String localPath = "";
    if(fsPath.startsWith("file://")){
      localPath = fsPath.substring(6);
    }
    else if(fsPath.startsWith("file:")){
      localPath = fsPath.substring(5);
    }
    else if(fsPath.matches("(\\w:\\\\).*")){
      localPath = fsPath.substring(2);
    }
    else{
      localPath = fsPath;
    }
    localPath = localPath.replaceFirst("/[^\\/]*/\\.\\.", "");
    localPath = localPath.replaceFirst("^/(\\w):", "$1:");
    localPath = localPath.replaceFirst("^file:(\\w):", "$1:");
    localPath = localPath.replaceFirst("^/(\\w):", "$1:");
    try{
      localPath = URLDecoder.decode(localPath, "utf-8");
    }
    catch (UnsupportedEncodingException e){
      e.printStackTrace();
    }
    Debug.debug("setLocalDirDisplay "+localPath, 3);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(true);
      bUpload.setEnabled(true);
      bRegister.setEnabled(false);
      String htmlText = "";
      try{
        String [] text = LocalStaticShellMgr.listFiles(localPath);
        int directories = 0;
        int files = 0;
        Vector textVector = new Vector();
        String filter = jtFilter.getText();
        if(filter==null || filter.equals("")){
          filter = "*";
        }
        statusBar.setLabel("Filtering...");
        filter = filter.replaceAll("\\.", "\\\\.");
        filter = filter.replaceAll("\\*", ".*");
        Debug.debug("Filtering with "+filter, 3);
        Vector lastUrlVector = new Vector();
        for(int j=0; j<text.length; ++j){
          if((jcbFilter.isSelected() ||
              !text[j].substring(localPath.length()).matches("^\\.[^\\.].+")) &&
              text[j].substring(localPath.length()).matches(filter)){
            if(LocalStaticShellMgr.isDirectory(text[j])){
              ++directories;
            }
            else{
              ++files;
            }
            textVector.add("<a href=\"file:"+text[j]+"\">" + 
                (((text[j].matches("(\\w:\\\\).*") ||
                    text[j].matches("\\w:/.*")) &&
                    !localPath.matches("(\\w:\\\\).*") &&
                    !localPath.matches("\\w:/.*")) ? 
                    text[j].substring(localPath.length()+2) :
                      text[j].substring(localPath.length())) +  "</a>");
            lastUrlVector.add("file:"+text[j]);
          }
        }
        lastUrlList = new String [lastUrlVector.size()];
        for(int j=0; j<lastUrlList.length; ++j){
          lastUrlList[j] = lastUrlVector.get(j).toString();
        }
        ep.setContentType("text/html");
        htmlText = "<html>\n";
        if(!localPath.equals("/")){
          htmlText += "<a href=\"file:"+localPath+"../\">"/*+localPath*/+"../</a><br>\n";
        }
        htmlText += Util.arrayToString(textVector.toArray(), "<br>\n");
        htmlText += "\n</html>";
        ep.setText(htmlText);
        ep.setEditable(false);
        // if we don't get an exception, the directory got read...
        Debug.debug("Directory "+localPath, 2);
        Debug.debug("Setting thisUrl, "+localPath, 3);
        thisUrl = (new File(localPath)).toURI().toURL().toExternalForm();
        setUrl(thisUrl);
        statusBar.setLabel(directories+" director"+(directories==1?"y":"ies")+", " +
            files+" file"+(files==1?"":"s"));
        bDownload.setEnabled(files>0);
        return;
      }
      catch(Exception e){
        e.printStackTrace();
        Debug.debug("Could not read "+localPath, 1);
      }
      // if we got here we did not read the directory
      bSave.setEnabled(false);
      bOk.setEnabled(false);
      ep.setText("ERROR!\n\nThe directory "+localPath+" could not be read.");
      pButton.updateUI();
    }
    catch(Exception e){
      Debug.debug("Could not set directory display of "+localPath+". "+
          e.getMessage(), 1);
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }
  
   /**
   * Set the EditorPane to display a directory listing
   * of the URL url.
   */
  private void setRemoteDirDisplay(String url, FileTransfer ft, String protocol) throws Exception{
    Debug.debug("setRemoteDirDisplay "+url, 3);
    
    listedUrls = new Vector();
    listedSizes = new Vector();
    jtFilter.setEnabled(true);
    String filter = jtFilter.getText();
    String htmlText = "";
    String href = null;

    try{
      bSave.setEnabled(false);
      bNew.setEnabled(true);
      bUpload.setEnabled(true);

      url = url.replaceFirst("/[^\\/]*/\\.\\.", "");
      GlobusURL globusUrl = new GlobusURL(url);
      Debug.debug("Opening directory on remote server\n"+globusUrl.toString(), 3);
      String localPath = "/";
      if(globusUrl.getPath()!=null){
        localPath = globusUrl.getPath();
        if(!localPath.startsWith("/")){
          localPath = "/" + localPath;
        }
      }
      String host = globusUrl.getHost();
      int port = globusUrl.getPort();
      
      Vector textVector = ft.list(globusUrl, filter,
          this.statusBar);

      String text = "";
      // TODO: reconsider max entries and why listing more is so slow...
      // display max 500 entries
      // TODO: make this configurable
      int maxEntries = 500;
      int length = textVector.size()<maxEntries ? textVector.size() : maxEntries;
      String name = null;
      String bytes = null;
      String longName = null;
      String [] nameAndBytes = null;
      lastUrlList = new String [length];
      lastSizesList = new String [length];
      int directories = 0;
      int files = 0;
      for(int i=0; i<length; ++i){
        nameAndBytes = null;
        longName = textVector.get(i).toString();
        try{
          nameAndBytes = Util.split(longName);
        }
        catch(Exception e){
        }
        if(nameAndBytes!=null && nameAndBytes.length>0){
          name = nameAndBytes[0];
          if(nameAndBytes.length>1){
            bytes = nameAndBytes[1];
          }
          else{
            bytes = "";
          }
        }
        else{
          name = longName;
          bytes = "";
        }
        if(!jcbFilter.isSelected() && name.matches("^\\.[^\\.].+")){
          continue;
        }
        href = protocol+"://"+host+(port>-1?":"+port:"")+localPath+name;
        if(name.endsWith("/")){
          ++directories;
        }
        else{
          ++files;
          listedUrls.add(href);
          listedSizes.add(bytes);
        }
        text += "<a href=\""+href+"\">"+
        /*"gsiftp://"+host+":"+port+localPath+*/name+"</a> "+bytes;
        if(i<length-1){
          text += "<br>\n";
        }
        lastUrlList[i] = protocol+"://"+host+":"+port+localPath+name;
        lastSizesList[i] = bytes;
        Debug.debug(textVector.get(i).toString(), 3);
      }
      ep.setContentType("text/html");
      htmlText = "<html>\n";
      
      if(!localPath.equals("/")){
        htmlText += "<a href=\""+protocol+"://"+host+":"+port+localPath+"../\">"+
        /*"gsiftp://"+host+":"+port+localPath+*/"../</a><br>\n";
      }
      htmlText += text;
      if(textVector.size()>maxEntries){
        htmlText += "<br>\n...<br>\n...<br>\n...<br>\n";
      }
      htmlText += "\n</html>";
      Debug.debug("done parsing, setting text", 3);
      ep.setText(htmlText);
      ep.setEditable(false);
      // if we don't get an exception, the directory got read...
      //thisUrl = (new File(localPath)).toURL().toExternalForm();
      thisUrl = url;
      statusBar.setLabel(directories+" director"+(directories==1?"y":"ies")+", " +
          files+" file"+(files==1?"":"s"));
      bDownload.setEnabled(listedUrls!=null && listedUrls.size()>0);
      bRegister.setEnabled(allowRegister && listedUrls!=null && listedUrls.size()>0);
      setUrl(thisUrl);
    }
    catch(Exception e){
      bSave.setEnabled(false);
      bOk.setEnabled(false);
      ep.setText("ERROR!\n\nThe directory could not be read.");
      pButton.updateUI();
      throw e;
    }
  }

  /**
   * Set the EditorPane to display a directory listing
   * of the URL url.
   */
  private void setHttpDirDisplay(final String url) throws IOException{
    Debug.debug("setHttpDirDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
     
      MyThread t = new MyThread(){
        String res = null;
        MyUrlCopy urlCopy = null;
        public void run(){
          try{
            ep.setPage(url);
          }
          catch(Exception e){
            e.printStackTrace();
            this.setException(e);
            return;
          }
          res = urlCopy.getResult();
          Debug.debug("List result: ", 2);
        }
        public String getStringRes(){
          return res;
        }
      };
      t.start();
      if(!Util.waitForThread(t, "https", HTTP_TIMEOUT, "list", new Boolean(true))){
        if(statusBar!=null){
          statusBar.setLabel("List cancelled");
        }
        throw new IOException("List timed out");
      }
      
      // workaround for bug in java < 1.5
      // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4492274
      // Doesn't work...
      /*if (!ep.getPage().equals(url))
      {
        ep.getDocument().
          putProperty(Document.StreamDescriptionProperty, url);
      }*/
      ep.setEditable(false);
      pButton.updateUI();
      Debug.debug("Setting thisUrl, "+thisUrl, 3);
      thisUrl = url;
      setUrl(thisUrl);
    }
    catch(IOException e){
      Debug.debug("Could not set directory display of url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
  }

  public Dimension getPreferredSize(){
    return panel.getPreferredSize();
  }

  public Dimension getMinimumSize(){
    return panel.getPreferredSize();
  }

  //Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e){
    if (e.getID()==WindowEvent.WINDOW_CLOSING){
      cancel();
    }
    super.processWindowEvent(e);
  }
  
  //Close the dialog
  void cancel(){
    if(thisUrl==null || thisUrl.endsWith("/")){
      //lastURL = Util.urlDecode(origUrl);
      lastURL = null;
      lastUrlList = null;
      saveHistory();
      dispose();
    }
    else{
      try{
        String newUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/")+1);
        //thisUrl = newUrl;
        //setUrl(thisUrl);
        setDisplay(newUrl);
      }
      catch(Exception ioe){
        ioe.printStackTrace();
        lastUrlList = null;
        //lastURL = Util.urlDecode(origUrl);
        lastURL = null;
        dispose();
      }
    }
  }

  //Set lastURL and close the dialog
  void exit(){
    //GridPilot.lastURL = ep.getPage();
    Debug.debug("Setting lastURL, "+thisUrl, 3);
    lastURL = Util.urlDecode(thisUrl);
    saveHistory();
    dispose();
  }
  
  void saveHistory(){
    Debug.debug("Saving history", 3);
    if(saveUrlHistory){
      try{
        LocalStaticShellMgr.writeFile(GridPilot.browserHistoryFile,
           Util.arrayToString(GridPilot.getClassMgr().getUrlList().toArray(),"\n"), false);
      }
      catch(Exception e){
        Debug.debug("WARNING: could not write history file. "+e.getMessage(), 1);
        e.printStackTrace();
      }
    }
  }

  /**
   * Write file or directory fsPath in local file system containing text.
   */
  private void localWriteFile(String fsPath, String text) throws IOException {
    if(fsPath.endsWith("/") && (text==null || text.equals(""))){
      fsPath = fsPath.substring(0, fsPath.length()-1);
      if(!LocalStaticShellMgr.mkdirs(fsPath)){
        throw new IOException("Could not make directory "+fsPath);
      }
    }
    else if(!fsPath.endsWith("/")){
      LocalStaticShellMgr.writeFile(fsPath, text, false);
    }
    else{
      throw new IOException("ERROR: Cannot write text to a directory.");
    }
    Debug.debug("File or directory "+fsPath+" written", 2);
  }

  /**
   * Delete file or directory fsPath in local file system.
   */
  private void localDeleteFile(String fsPath) throws IOException{
    if(!LocalStaticShellMgr.deleteFile(fsPath)){
      throw new IOException(fsPath+" could not be deleted.");
    }
    Debug.debug("File or directory "+fsPath+" deleted", 2);
  }
  
  /**
   * Create an empty file or directory in the directory fsPath.
   * Ask for the name.
   * If a name ending with a / is typed in, a directory is created.
   * (this path must match the URL url).
   */
  private String localCreate(String fsPath) throws IOException {
    String fileName = Util.getName("File name (end with a / to create a directory)", "");   
    if(fsPath==null || fileName==null){
      return null;
    }  
    if(!fsPath.endsWith("/") && !fileName.startsWith("/")){
      fsPath = fsPath+"/";
    }
    localWriteFile(fsPath+fileName, "");
    return fileName;
  }

  /**
   * Delete file or directory.
   * Ask for the name.
   */
  private void deleteFile(String url){
    if(url==null){
      return;
    }
    String msg = "Are you sure you want to delete the file "+url+"?";
    ConfirmBox confirmBox = new ConfirmBox(JOptionPane.getRootFrame());
    try{
      int choice = confirmBox.getConfirm("Confirm delete",
          msg, new Object[] {"OK", "Cancel"});
      if(choice!=0){
        return;
      }
    }
    catch(Exception e){
      e.printStackTrace();
      return;
    }
    String baseUrl = url.replaceFirst("/$", "").replaceFirst("(.*/)[^/]+$", "$1");
    Debug.debug("baseUrl: "+baseUrl, 3);
    try{
      if(url.startsWith("file:") || url.startsWith("/")){
        Debug.debug("Deleting file "+url, 3);
        localDeleteFile(url);
        statusBar.setLabel(url+" deleted");
      }
      else if(url.startsWith("gsiftp://") || url.startsWith("https://") ||
          url.startsWith("sss://")){
        GlobusURL globusUrl = new GlobusURL(url);
        if(url.startsWith("gsiftp://")){
          gsiftpFileTransfer.deleteFiles(new GlobusURL [] {globusUrl});
        }
        else if(url.startsWith("https://")){
          httpsFileTransfer.deleteFiles(new GlobusURL [] {globusUrl});
        }
        else if(url.startsWith("sss://")){
          sssFileTransfer.deleteFiles(new GlobusURL [] {globusUrl});
        }
        statusBar.setLabel(globusUrl.getPath()+" deleted");
      }
      else{
        throw(new IOException("Unknown protocol for "+thisUrl));
      }
      try{
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay(baseUrl);
        //setDisplay(thisUrl);
      }
      catch(Exception ioe){
        Debug.debug("WARNING: could not display "+thisUrl, 1);
        ioe.printStackTrace();
      }
    }
    catch(Exception e){
      Debug.debug("ERROR: could not delete "+url+". "+e.getMessage(), 1);
      e.printStackTrace();
      ep.setText("ERROR!\n\nThe file "+url+" could not be deleted.\n\n"+
          //"If it is a directory, delete all files within first.\n\n"+
          e.getMessage());
      statusBar.setLabel("ERROR: The file "+url+" could not be deleted.");
      //pButton.updateUI();
    }
  }
  
  /**
   * Creates a file on a URL.
   */
  private String createNew() throws Exception{
    if(!thisUrl.endsWith("/")){
      throw new IOException("ERROR: URL "+thisUrl+" does not end with /.");
    }
    String ret = null;
    try{
      Debug.debug("Creating file in "+thisUrl, 3);
      // local directory
      if(thisUrl.startsWith("file:")){
        String fsPath = thisUrl;
        fsPath = fsPath.replaceFirst("^file://", "/");
        fsPath = fsPath.replaceFirst("^file:/", "/");
        fsPath = fsPath.replaceFirst("^file:", "");
        fsPath = fsPath.replaceFirst("^/(\\w):", "$1:");
        Debug.debug("Creating file in "+fsPath, 3);
        ret = localCreate(fsPath);
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay((new URL("file:"+fsPath)).toExternalForm());
      }
      // remote gsiftp directory
      else if(thisUrl.startsWith("gsiftp://")){
        Debug.debug("Creating file in "+thisUrl, 3);
        GlobusURL globusUrl = new GlobusURL(thisUrl);
        ret = gsiftpFileTransfer.create(globusUrl);
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay(thisUrl);
      }
      // remote https/webdav directory
      else if(thisUrl.startsWith("https://")){
        Debug.debug("Creating file in "+thisUrl, 3);
        GlobusURL globusUrl = new GlobusURL(thisUrl);
        ret = httpsFileTransfer.create(globusUrl);
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay(thisUrl);
      }
      // remote Amazon s3 directory
      else if(thisUrl.startsWith("sss://")){
        Debug.debug("Creating file in "+thisUrl, 3);
        GlobusURL globusUrl = new GlobusURL(thisUrl);
        sssFileTransfer.write(globusUrl, "");
        ret = thisUrl;
        ep.getDocument().putProperty(
            Document.StreamDescriptionProperty, null);
        setDisplay(thisUrl);
      }
      else{
        throw(new IOException("Unknown protocol for "+thisUrl));
      }
    }
    catch(IOException e){
      Debug.debug("Could not save to URL "+thisUrl+". "+e.getMessage(), 1);
      throw e;
    }
    return ret;
  }

  //Close the dialog on a button event
  public void actionPerformed(ActionEvent e){
    try{
      if(e.getSource()==bOk){
        exit();
      }
      else if(e.getSource()==bCancel){
        cancel();
      }
      else if(e.getSource()==bSave){
        if(thisUrl.startsWith("file:") || thisUrl.startsWith("/")){
          String fsPath = ep.getPage().toExternalForm();
          fsPath = fsPath.replaceFirst("^file://", "/");
          fsPath = fsPath.replaceFirst("^file:/", "/");
          fsPath = fsPath.replaceFirst("^file:", "");
          fsPath = fsPath.replaceFirst("^/(\\w):", "$1:");
          String text = ep.getText();
          localWriteFile(fsPath, text);
        }
        else if(thisUrl.startsWith("gsiftp://")){
          GlobusURL globusUrl = new GlobusURL(thisUrl);
          gsiftpFileTransfer.write(globusUrl, ep.getText());
        }
        else if(thisUrl.startsWith("https://")){
          GlobusURL globusUrl = new GlobusURL(thisUrl);
          httpsFileTransfer.write(globusUrl, ep.getText());
        }
        else if(thisUrl.startsWith("sss://")){
          GlobusURL globusUrl = new GlobusURL(thisUrl);
          sssFileTransfer.write(globusUrl, ep.getText());
        }
        else{
          throw(new IOException("Unknown protocol for "+thisUrl));
        }
        statusBar.setLabel(thisUrl+" saved");
      }
      else if(e.getSource()==bNew){
        String newFileOrDir = createNew();
        statusBar.setLabel(/*thisUrl+*/newFileOrDir+" created");
      }
      else if(e.getSource()==bUpload){
        File file = getInputFile();
        if(file==null){
          return;
        }
        TransferControl.upload(file, thisUrl, ep);
        try{
          ep.getDocument().putProperty(
              Document.StreamDescriptionProperty, null);
          setDisplay(thisUrl);
        }
        catch(Exception ioe){
          ioe.printStackTrace();
        }
        statusBar.setLabel(thisUrl+" uploaded");
      }
      else if(e.getSource()==bDownload){
        final File dir = Util.getDownloadDir(this);      
        if(dir==null){
          return;
        }
        MyThread t = (new MyThread(){
          public void run(){
            String href = null;
            for(Iterator it=listedUrls.iterator(); it.hasNext();){
              href = (String) it.next();
              Debug.debug("Getting file : "+href+" -> "+dir.getAbsolutePath(), 3);
              try{
                //TransferControl.download(href, dir, ep);
                // Use the physical file name (strip off the first ...://.../.../
                TransferControl.startCopyFiles(new GlobusURL [] {new GlobusURL(href)},
                    new GlobusURL [] {new GlobusURL("file:///"+
                        (new File(dir, href.replaceFirst(".*/([^/]+)$", "$1"))))});
              }
              catch(Exception e){
                e.printStackTrace();
              }
            }
          }
        });
        SwingUtilities.invokeLater(t);
      }
      else if(e.getSource()==bRegister){
        bmiRegister.show(this, 0, 0);
        bmiRegister.show(bRegister, -bmiRegister.getWidth(),
            -bmiRegister.getHeight() + bRegister.getHeight());
      }
    }
    catch(Exception ex){
      statusBar.setLabel("ERROR: "+ex.getMessage());
      GridPilot.getClassMgr().getLogFile().addMessage("ERROR: ", ex);
      Debug.debug("ERROR: "+ex.getMessage(), 3);
      ex.printStackTrace();
    }
  }

  private File getInputFile(){
    File file = null;
    JFileChooser fc = new JFileChooser();
    fc.setDialogTitle("Choose file to upload");
    fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
    int returnVal = fc.showOpenDialog(this);
    if (returnVal==JFileChooser.APPROVE_OPTION){
      file = fc.getSelectedFile();
      Debug.debug("Opening: " + file.getName(), 2);
    }
    else{
      Debug.debug("Not opening file", 3);
    }
    return file;
  }
  
  private void registerAll(final String dbName) {
    MyThread t = (new MyThread(){
      public void run(){
        try{
          DBPluginMgr dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
          String [] dsLfn = selectLFNAndDS((String) listedUrls.get(0), dbPluginMgr, true);
          String datasetName = dsLfn[0];
          String datasetID = dbPluginMgr.getDatasetID(datasetName);
          if(datasetName==null || datasetID==null || datasetID.equals("") ||
              datasetID.equals("-1")){
            throw new Exception("No dataset");
          }
          String href = null;
          String lfn = null;
          String size = null;
          Iterator itt=listedSizes.iterator();
          for(Iterator it=listedUrls.iterator(); it.hasNext();){
            href = (String) it.next();
            size = (String) itt.next();
            Debug.debug("Registering file : "+href+" in "+datasetName, 3);
            String uuid = UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
            statusBar.setLabel("Registering "+href+" in "+dbName);
            lfn = href.replaceFirst(".*/([^/]+)$", "$1");
            dbPluginMgr.registerFileLocation(
                datasetID, datasetName, uuid, lfn, href, size, null, false);
          }
          statusBar.setLabel("Registration done");
          registering = false;
        }
        catch(Exception ioe){
          statusBar.setLabel("Registration failed");
          registering = false;
          ioe.printStackTrace();
        }
      }
    });
    SwingUtilities.invokeLater(t);
  }

  
}