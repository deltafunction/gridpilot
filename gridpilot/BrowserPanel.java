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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Vector;

import org.globus.ftp.exception.FTPException;
import org.globus.util.GlobusURL;
import org.safehaus.uuid.UUIDGenerator;

import gridfactory.common.ConfirmBox;
import gridfactory.common.DBResult;
import gridfactory.common.Debug;
import gridfactory.common.FileTransfer;
import gridfactory.common.LocalStaticShell;
import gridfactory.common.ResThread;
import gridfactory.common.StatusBar;
import gridfactory.common.TransferInfo;

import gridpilot.ftplugins.gsiftp.GSIFTPFileTransfer;
import gridpilot.ftplugins.https.HTTPSFileTransfer;
import gridpilot.ftplugins.srm.SRMFileTransfer;
import gridpilot.ftplugins.sss.SSSFileTransfer;


/**
 * Box showing URL.
 */
public class BrowserPanel extends JDialog implements ActionListener{

  private static final long serialVersionUID = 1L;
  private JPanel panel = new JPanel(new BorderLayout());
  private JButton bOk;
  private JButton bNew;
  private JButton bUpload;
  private JButton bDownload;
  private JButton bRegister;
  private JButton bSave;
  private JButton bCancel;
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
  private String lastURL = null;
  private String [] lastUrlsList = null;
  private String [] lastSizesList = null;
  private String currentUrlString = "";
  private JComboBox currentUrlBox = null;
  private GSIFTPFileTransfer gsiftpFileTransfer = null;
  private HTTPSFileTransfer httpsFileTransfer = null;
  private SSSFileTransfer sssFileTransfer = null;
  private SRMFileTransfer srmFileTransfer = null;
  private boolean ok = true;
  private boolean saveUrlHistory = false;
  private boolean doingSearch = false;
  private JComponent jBox = null;
  private boolean localFS = false;
  private JPopupMenu popupMenu = new JPopupMenu();
  private JMenuItem miDownload = new JMenuItem("Download file");
  private JMenuItem miDelete = new JMenuItem("Delete file");
  private JMenu miRegister = new JMenu("Register file");
  // Menu to pop-up when clicking "Register all"
  private JPopupMenu bmiRegister = new JPopupMenu("Register all files");
  // File registration semaphore
  private boolean registering = false;
  private JComponent dsField = new JTextField(TEXTFIELDWIDTH);
  // Keep track of which files we are listing.
  private Vector<String> listedUrls = null;
  private Vector<String> listedSizes = null;
  private boolean allowRegister = true;
  private HashSet<String> excludeDBs = new HashSet<String>();
  private Vector<String> urlList;
  
  private static int MAX_FILE_EDIT_BYTES = 100000;
  private static int TEXTFIELDWIDTH = 32;
  //private static int HTTP_TIMEOUT = 10000;
  private static final int OPEN_TIMEOUT = 30000;
  private static final int UPLOAD_TIMEOUT = 30000;
  private static final String FILE_FOUND_TEXT = "File found. Size: ";
  private static final String DIR_FILTER = "^*/$|";
  private static final int MAX_TEXT_EDIT_LINES = 1000;

  public static int HISTORY_SIZE = 15;

  public BrowserPanel(Window parent, String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS) throws Exception{
    super(parent);
    urlList = GridPilot.getClassMgr().getBrowserHistoryList();
    init(parent, title, url, _baseUrl, modal, _withFilter, _withNavigation, _jBox, _filter,
        _localFS, true, true);
  }
  
  public BrowserPanel(Window parent, String title, String url, 
      String _baseUrl, boolean modal, boolean _withFilter,
      boolean _withNavigation, JComponent _jBox, String _filter,
      boolean _localFS, boolean cancelEnabled, boolean registrationEnabled) throws Exception{
    urlList = GridPilot.getClassMgr().getBrowserHistoryList();
    init(parent, title, url, _baseUrl, modal, _withFilter, _withNavigation, _jBox, _filter,
        _localFS, cancelEnabled, registrationEnabled);
  }

  private void init(Window parent, String title, String url, 
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
      srmFileTransfer = (SRMFileTransfer) GridPilot.getClassMgr().getFTPlugin("srm");
    }
    
    readBrowserHistory();
    
    currentUrlBox = new JExtendedComboBox();
    currentUrlBox.setEditable(true);
    Dimension d = currentUrlBox.getPreferredSize();
    currentUrlBox.setPreferredSize(new Dimension(320, d.height));
    setUrl(url);
    Debug.debug("initializing browser window", 3);
    try{
      initGUI(parent, title, url, cancelEnabled, registrationEnabled);
    }
    catch(IOException e){
      //e.printStackTrace();
      throw e;
    }
  }
  
  public String getLastURL(){
    return lastURL;
  }
  
  public String[] getLastURLs(){
    return lastUrlsList;
  }
  
  public String[] getLastSizes(){
    return lastSizesList;
  }
  
  public String getFilter(){
    return jtFilter.getText();
  }
  
  public void setCancelButtonEnabled(boolean enabled){
    bCancel.setEnabled(enabled);
  }
  
  private void readBrowserHistory(){
    String urlHistory = null;
    Debug.debug("browser history file: "+GridPilot.BROWSER_HISTORY_FILE, 2);
    if(GridPilot.BROWSER_HISTORY_FILE!=null && !GridPilot.BROWSER_HISTORY_FILE.equals("")){
      if(GridPilot.BROWSER_HISTORY_FILE.startsWith("~")){
        String homeDir = System.getProperty("user.home");
        if(!homeDir.endsWith(File.separator)){
          homeDir += File.separator;
        }
        GridPilot.BROWSER_HISTORY_FILE = homeDir+GridPilot.BROWSER_HISTORY_FILE.substring(1);
      }
      try{
        if(!LocalStaticShell.existsFile(GridPilot.BROWSER_HISTORY_FILE)){
          Debug.debug("trying to create file", 2);
          LocalStaticShell.writeFile(GridPilot.BROWSER_HISTORY_FILE, "", false);
        }
        urlHistory = LocalStaticShell.readFile(GridPilot.BROWSER_HISTORY_FILE);
        saveUrlHistory = true;
      }
      catch(Exception e){
        Debug.debug("WARNING: could not use "+GridPilot.BROWSER_HISTORY_FILE+
            " as history file.", 1);
        GridPilot.BROWSER_HISTORY_FILE = null;
        urlHistory = null;
      }
    }
    
    if((urlList==null || urlList.size()==0) &&
        urlHistory!=null && !urlHistory.equals("")){
      BufferedReader in = null;
      try{
        Debug.debug("Reading file "+GridPilot.BROWSER_HISTORY_FILE, 3);
        in = new BufferedReader(
          new InputStreamReader((new URL("file:"+GridPilot.BROWSER_HISTORY_FILE)).openStream()));
      }
      catch(IOException ioe){
        Debug.debug("WARNING: could not use "+GridPilot.BROWSER_HISTORY_FILE+
            " as history file.", 1);
        GridPilot.BROWSER_HISTORY_FILE = null;
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
          addUrl(line);
          Debug.debug("URL: "+line, 3);
        }
        in.close();
      }
      catch(IOException ioe){
        Debug.debug("WARNING: could not use "+GridPilot.BROWSER_HISTORY_FILE+
            " as history file.", 1);
        GridPilot.BROWSER_HISTORY_FILE = null;
        urlHistory = null;
      }
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
            ResThread t = (new ResThread(){
              public void run(){
                try{
                  setDisplay(/*currentUrlBox.getSelectedItem().toString()*/MyUtil.getJTextOrEmptyString(currentUrlBox));
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
            //SwingUtilities.invokeLater(t);
            t.start();
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
            ResThread t = (new ResThread(){
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
            //SwingUtilities.invokeLater(t);
            t.start();
          }
        }
      }
    });
  }
  
  private void initButtons(){
    bOk = MyUtil.mkButton("ok.png", "OK", "OK");
    bNew = MyUtil.mkButton("file_new.png", "New", "Create new file or folder");
    bUpload =  MyUtil.mkButton("put.png", "Put", "Upload file");
    bDownload = MyUtil.mkButton("get_all.png", "Get all", "Download all file(s) in this directory");
    bRegister = MyUtil.mkButton("register.png", "Register all", "Register all file(s) in this directory");
    bSave = MyUtil.mkButton("save.png", "Save", "Save this document");
    bCancel = MyUtil.mkButton("cancel.png", "Cancel", "Cancel");
    bOk.addActionListener(this);
    bNew.addActionListener(this);
    bUpload.addActionListener(this);
    bDownload.addActionListener(this);
    bRegister.addActionListener(this);
    bSave.addActionListener(this);
    bCancel.addActionListener(this);
  }
  
  HyperlinkListener hl = new HyperlinkListener(){
    public void hyperlinkUpdate(final HyperlinkEvent e){
      if(e.getEventType()==HyperlinkEvent.EventType.ACTIVATED){
        Debug.debug("click detected --> "+ep.getText(), 3);
        if(isDLWindow()){
          Debug.debug("isDLWindow", 3);
          String url;
          if(e.getURL()!=null){
            url = e.getURL().toExternalForm();
          }
          else{
            url = e.getDescription();
          }
          download(url, null);
          //statusBar.setLabel(url+" downloaded");
          ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          MyUtil.showMessage(SwingUtilities.getWindowAncestor(getThis()),
             "Download ok", url+" downloaded.");
        }
        else if(e instanceof HTMLFrameHyperlinkEvent){
          ((HTMLDocument) ep.getDocument()).processHTMLFrameHyperlinkEvent(
              (HTMLFrameHyperlinkEvent) e);
         }
        else{
          //setUrl(e.getDescription());
          ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          statusBar.setLabel("Opening URL...");
          ResThread t = new ResThread(){
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
          }; 
          t.start();
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
          for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
            if(excludeDBs.contains(Integer.toString(i))){
              continue;
            }
            Debug.debug("addActionListener "+MyUtil.arrayToString(excludeDBs.toArray())+":"+i, 3);
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
            miDownload.setText("Download directory");
          }
          miDownload.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent ev){
              download(url, null);
              MyUtil.showMessage(SwingUtilities.getWindowAncestor(getThis()),
                 "Download ok", url+" downloaded.");
            }
          });
          miDelete.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent ev){
              deleteFileOrDir(url);
              statusBar.setLabel(url+" deleted.");
            }
          });
          popupMenu.add(miDownload);
          popupMenu.add(miDelete);
        }
      }
      else if(e.getEventType()==HyperlinkEvent.EventType.EXITED){
        statusBar.setLabel(" ");
        try{
          if(GridPilot.DB_NAMES!=null){
            int ii = 0;
            for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
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
            miDownload.setText("Download file");
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
  };
  
  private BrowserPanel getThis(){
    return this;
  }
  
  // TODO: cleanup this monster
  /**
   * Component initialization.
   * If parent is not null, it gets its cursor set to the default after loading url.
   * If cancelEnabled is false, the cancel button is disabled.
   */
  private void initGUI(final Window parent, String title, final String url,
      boolean cancelEnabled, boolean registrationEnabled) throws Exception{
    
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    this.getContentPane().setLayout(new BorderLayout());
 
    Debug.debug("Creating BrowserPanel with baseUrl "+baseUrl, 3);
    
    requestFocusInWindow();
    this.setTitle(title);
    
    initButtons();

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
    
    if(GridPilot.DB_NAMES!=null){
      DBPluginMgr dbPluginMgr = null;
      for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
        dbPluginMgr = null;
        try{
          dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(GridPilot.DB_NAMES[i]);
          if(dbPluginMgr==null || !dbPluginMgr.isFileCatalog()){
            throw new Exception();
          }
        }
        catch(Exception e){
          excludeDBs.add(Integer.toString(i));
          continue;
        }
        miRegister.add(new JMenuItem(GridPilot.DB_NAMES[i]));
      }
      JMenuItem [] jmiRegisterAll = new JMenuItem[GridPilot.DB_NAMES.length];
      for(int i=0; i<GridPilot.DB_NAMES.length; ++i){
        if(excludeDBs.contains(Integer.toString(i))){
          continue;
        }
        jmiRegisterAll[i] = new JMenuItem(GridPilot.DB_NAMES[i]);
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

    panel.setPreferredSize(new Dimension(580+MyUtil.BUTTON_DISPLAY*60, 400));
    setResizable(true);
    
    JPanel topPanel = new JPanel(new GridBagLayout()); 
    
    if(withNavigation){
      JButton bHome = MyUtil.mkButton1("home.png", "Go to home URL", "Home");
      bHome.setPreferredSize(new java.awt.Dimension(22, 22));
      bHome.setSize(new java.awt.Dimension(22, 22));
      bHome.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent me){
          ResThread t = (new ResThread(){
            public void run(){
              try{
                statusBar.setLabel("Opening URL...");
                setDisplay(GridPilot.GRID_HOME_URL);
              }
              catch(Exception ee){
                statusBar.setLabel("ERROR: could not open "+GridPilot.GRID_HOME_URL+
                    ". "+ee.getMessage());
                Debug.debug("ERROR: could not open "+GridPilot.GRID_HOME_URL+
                    ". "+ee.getMessage(), 1);
                ee.printStackTrace();
              }
              doingSearch = false;
            }
          });     
          //SwingUtilities.invokeLater(t);
          t.start();
        }
      });
      JButton bEnter = MyUtil.mkButton1("key_enter.png", "Go to URL", "Go");
      bEnter.setPreferredSize(new java.awt.Dimension(22, 22));
      bEnter.setSize(new java.awt.Dimension(22, 22));
      bEnter.addMouseListener(new MouseAdapter(){
        public void mouseClicked(MouseEvent me){
          statusBar.setLabel("Opening URL...");
          ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          ResThread t = (new ResThread(){
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
          //SwingUtilities.invokeLater(t);
          t.start();
        }
      });

      JPanel jpNavigation = new JPanel(new GridBagLayout());
      if(!GridPilot.IS_FIRST_RUN){
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
      addUrl("");
      addUrlKeyListener();
    }
    else{
      topPanel.add(currentUrlLabel,
          new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
              GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
              new Insets(0, 5, 0, 5), 0, 0));
    }
    
    if(withFilter){
      JPanel jpFilter = new JPanel(new GridBagLayout());      
      /*if(!withNavigation){
        topPanel.add(currentUrlLabel,
            new GridBagConstraints(0, 0, 1, 1, 1.0, 1.0,
                GridBagConstraints.WEST, GridBagConstraints.HORIZONTAL,
                new Insets(0, 5, 0, 5), 0, 0));
      }*/
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
    
    //if(withNavigation || withFilter){
      panel.add(topPanel, BorderLayout.NORTH);
    //}
        
    //HTMLDocument d = new HTMLDocument();
    ep.addHyperlinkListener(hl);
    
    ep.addMouseListener(new java.awt.event.MouseAdapter() {
      public void mousePressed(MouseEvent e) {
        if (e.getButton()!=MouseEvent.BUTTON1) // right button
          popupMenu.show(e.getComponent(), e.getX(), e.getY());
      }
    });
    
    statusBar = new StatusBar();
    statusBar.setLabel(" ");
    this.getContentPane().add(statusBar, BorderLayout.SOUTH);
    
    
    //setDisplay(url);
    MyResThread dt = new MyResThread(){
      public void run(){
        try{
          setDisplay(url);
        }
        catch(Exception e){
          Debug.debug("setDisplay0 failed, trying parent", 2);
          try{
            if(!url.endsWith("/") && !url.endsWith("\\")){
              setDisplay(getParent(url));
            }
          }
          catch(Exception e1){
            e1.printStackTrace();
          }
        }
      }
    };
         
    dt.start();
    
    if(withNavigation && url==null || url.trim().equals("")){
      statusBar.setLabel("Type in URL and hit return");
    }
    
    // Fix up things if this was e.g. called from a wizard.
    if(parent!=null && (url==null || url.trim().equals(""))){
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
        //Debug.debug("Property changed: "+event.getPropertyName(), 3);
        //Debug.debug("Content type: "+ep.getContentType(), 3);
        //Debug.debug("Text: "+ep.getText(), 3);
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
    try{
      setVisible(true);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    
    // Close window with ctrl+w
    jtFilter.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!MyUtil.isModifierDown(e)){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            thisUrl = null;
            cancel();
        }
      }
    });
    
    currentUrlBox.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!MyUtil.isModifierDown(e)){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            thisUrl = null;
            cancel();
        }
      }
    });
    
    ep.addKeyListener(new KeyAdapter(){
      public void keyPressed(KeyEvent e){
        if(!MyUtil.isModifierDown(e)){
          return;
        }
        switch(e.getKeyCode()){
          case KeyEvent.VK_W:
            thisUrl = null;
            cancel();
        }
      }
    });
    
    if(url!=null && !url.equals("")){
      setUrl(url);
    }

  }
  
  private String getParent(String url) {
    String newUrl = url.replaceAll("\\\\", "/");
    newUrl = url.substring(0, newUrl.lastIndexOf("/")+1);
    return newUrl;
  }

  /**
   * Download a file or directory.
   */
  private void download(final String url, File _dir){
    final File dir = _dir!=null?_dir:MyUtil.getDownloadDir(this);      
    if(dir==null){
      return;
    }
    MyResThread rt = new MyResThread(){
      public void run(){
        if(url.endsWith("/")){
          downloadDir(url, dir);
        }
        else{
          downloadFile(url, dir);
        }
      }
    };
    rt.start();
  }

  /**
   * Download directory including all files and subdirs.
   */
  private void downloadDir(final String url, File dir){
    Debug.debug("Downloading to "+dir.getAbsolutePath(), 3);
    Debug.debug("Listing files in directory "+url, 2);
    String [] allFileAndDirUrls = null;
    try{
      statusBar.setLabel("Listing in directory "+url);
      String filter = jtFilter.getText();
      allFileAndDirUrls = MyTransferControl.findAllFilesAndDirs(url, filter)[0];
    }
    catch(Exception e){
      String error = "Could not download "+url;
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      showError(error+" : "+e.getMessage());
    }
    // Unqualified destination path
    String destFile;
    // Name of directory
    String dirName = url.replaceFirst(".*/([^/]+)/$", "$1");
    Debug.debug("Downloading whole directory "+dirName, 2);
    // Destination dir
    File destDir = new File(dir, dirName);
    // List of directories to be created
    HashSet<String> newDirs = new HashSet<String>();
    try{
      statusBar.setLabel("Queuing downloads... "+url);
      GlobusURL srcUrl;
      GlobusURL destUrl;
      Vector<TransferInfo> transfers = new Vector<TransferInfo>();
      for(int i=0; i<allFileAndDirUrls.length; ++i){
        srcUrl = new GlobusURL(allFileAndDirUrls[i]);
        destFile = allFileAndDirUrls[i].replaceFirst(url, "");
        destUrl = new GlobusURL("file:///"+(new File(destDir, destFile)).getAbsolutePath());
        Debug.debug("Will download "+destFile+": "+srcUrl.getURL()+"-->"+destUrl.getURL(), 2);
        if(srcUrl.getURL().endsWith("/")){
          newDirs.add(destUrl.getPath());
        }
        else{
          transfers.add(new TransferInfo(srcUrl, destUrl));
        }
      }
      GridPilot.getClassMgr().getTransferControl().queue(transfers);
      statusBar.setLabel("Creating directories...");
      createLocalDirs(newDirs);
      Debug.debug("Queuing done, "+url, 2);
      statusBar.setLabel("Queuing done");
      ep.getDocument().putProperty(Document.StreamDescriptionProperty, null);
      setDisplay(thisUrl);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("Could not download directory.", e);
      return;
    }
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel(MonitoringPanel.TAB_INDEX_TRANSFERS);
  }
  
  private void createLocalDirs(HashSet<String> newDirs) throws IOException {
    String newDir;
    for(Iterator<String> it=newDirs.iterator(); it.hasNext();){
      newDir = it.next();
      if(!LocalStaticShell.mkdirs(newDir)){
        throw new IOException("Could not create directory "+newDir);
      }
    }
  }

  /**
   * Find all files and their sizes in a given directory.
   * @param url
   * @return a 2xn array of the form {{url1, url2, ...}, {size1, size2, ...}}
   * @throws NullPointerException
   * @throws Exception
   */
  /*private String[][] listAllFiles(String url) throws NullPointerException, Exception {
    GlobusURL globusUrl = new GlobusURL(url);
    String filter = jtFilter.getText();
    Vector<String> allFilesAndDirs = GridPilot.getClassMgr().getFTPlugin(globusUrl.getProtocol()).find(globusUrl, filter);
    String line;
    String file;
    String size;
    String[] lineArr;
    Vector<String> allFiles = new Vector<String>();
    Vector<String> allFileSizes = new Vector<String>();
    for(Iterator<String> it=allFilesAndDirs.iterator(); it.hasNext();){
      line = it.next();
      lineArr = MyUtil.split(line);
      file = lineArr[0];
      size = lineArr[1];
      if(!file.endsWith("/")){
        allFiles.add(file);
        allFileSizes.add(size);
      }
    }
    return new String[][] {allFiles.toArray(new String[allFiles.size()]),
        allFileSizes.toArray(new String[allFileSizes.size()])};
  }*/

  /**
   * Download a single file.
   */
  private void downloadFile(final String url, File dir){
    Debug.debug("Getting file : "+url+" -> "+dir.getAbsolutePath(), 3);
    try{
      statusBar.setLabel("Downloading "+url);
      GridPilot.getClassMgr().getTransferControl().download(url, dir);
      Debug.debug("Download done, "+url, 2);
    }
    catch(Exception e){
      String error = "Could not download "+url;
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      showError(error+" : "+e.getMessage());
    }
    try{
      ep.getDocument().putProperty(
          Document.StreamDescriptionProperty, null);
      setDisplay(thisUrl);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  
  private void showError(String str){
    ConfirmBox confirmBox = new ConfirmBox(this);
    String title = "Browser error";
    try{
      confirmBox.getConfirm(title, str, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane())});
    }
    catch(Exception e){
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
    final JButton jbLookup = MyUtil.mkButton("search.png", "Look up", "Search results for this request");
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
        String idField = MyUtil.getIdentifierField(dbPluginMgr.getDBName(), "dataset");
        String nameField = MyUtil.getNameField(dbPluginMgr.getDBName(), "dataset");
        String str = MyUtil.getJTextOrEmptyString(dsField);
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
    
    JTextField lfnField = new JTextField(TEXTFIELDWIDTH);
    lfnField.setText(lfn);
    JPanel lfnRow = new JPanel(new BorderLayout());
    lfnRow.add(new JLabel("Logical file name: "), BorderLayout.WEST);
    lfnRow.add(lfnField, BorderLayout.CENTER);
    jPanel.add(lfnRow, new GridBagConstraints(0, 5, 1, 1, 0.0, 0.0,
        GridBagConstraints.NORTH, GridBagConstraints.HORIZONTAL,
        new Insets(10, 10, 10, 10), 0, 0));
    jPanel.validate();
    
    ConfirmBox confirmBox = new ConfirmBox(this);
    
    if(disableNameField){
      lfnField.setEnabled(false);
    }
    
    int choice = -1;
    try{
      choice = confirmBox.getConfirm("Register file in dataset",
          jPanel, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                                MyUtil.mkCancelObject(confirmBox.getOptionPane())});
    }
    catch(Exception e){
      e.printStackTrace();
      return null;
    }
    if(choice!=0){
      return null;
    }
    return new String [] {MyUtil.getJTextOrEmptyString(dsField), lfnField.getText()};
  }
  
  
  /**
   * Register a single file.
   */
  private void registerFile(final String url, final String dbName){
    if(registering){
      return;
    }
    ResThread t = (new ResThread(){
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
              if(lastUrlsList[i].equals(url)){
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
    //SwingUtilities.invokeLater(t);
    t.start();
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
    Debug.debug("checking history", 3);
    removeUrl("");
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
        removeUrl(urlList.iterator().next().toString());
      }
      addUrl(newUrl);
      Debug.debug("urlSet is now: "+MyUtil.arrayToString(
          urlList.toArray(), " : "), 2);
    }

    if(refresh || currentUrlBox.getItemCount()==0 && urlList.size()>0){
      currentUrlBox.removeAllItems();
      for(ListIterator<String> it=urlList.listIterator(urlList.size()-1); it.hasPrevious();){
        currentUrlBox.addItem((String) it.previous());
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
  private void setDisplay(final String url) throws Exception{
    ResThread t = new ResThread(){
      public void run(){
        try{
          setDisplay0(url);
        }
        catch(Exception e){
          //e.printStackTrace();
          setException(e);
          return;
        }
      }
    };
    ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    t.start();
    //SwingUtilities.invokeLater(t);
    if(!MyUtil.myWaitForThread(t, "setDisplay0", OPEN_TIMEOUT, "list", true) ||
        t.getException()!=null){
      if(statusBar!=null){
        statusBar.setLabel("setDisplay0 failed.");
      }
      ep.setContentType("text/plain");
      ep.setText("Could not open URL "+url);
      throw new IOException("setDisplay failed "+t.getException());
    }
  }

  private void setDisplay0(String url) throws Exception{
    try{
      lastUrlsList = null;
      lastSizesList = null;
      if( url.startsWith("~")){
        url = "file:"+System.getProperty("user.home")+url.substring(1);
      }
      else if(url.startsWith("file:/~") || url.startsWith("~")){
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
        setRemoteDirDisplay1(url, gsiftpFileTransfer, "gsiftp");
      }
      else if(url.startsWith("https://") &&
          url.endsWith("/")){
        try{
          setRemoteDirDisplay1(url, httpsFileTransfer, "https");
        }
        catch(Exception ee){
          ee.printStackTrace();
          //setHttpDirDisplay(url);
          setHtmlDisplay(url);
        }
      }
      else if(url.startsWith("sss://") &&
          url.endsWith("/")){
        setRemoteDirDisplay1(url, sssFileTransfer, "sss");
      }
      // remote gsiftp text file
      else if(url.startsWith("gsiftp://") &&
          !url.endsWith("/") && /*!url.endsWith("htm") &&
          !url.endsWith("html") &&*/ !url.endsWith(".gz") &&
          !url.endsWith(".tgz") && url.indexOf(".root")<0){
        if(!setRemoteTextEdit(url, gsiftpFileTransfer)){
          setRemoteFileConfirmDisplay(url, gsiftpFileTransfer);
        }
      }
      // remote https text file
      else if(url.startsWith("https://") &&
          !url.endsWith("/") && !url.endsWith("htm") &&
          !url.endsWith("html") && !url.endsWith(".gz") &&
          !url.endsWith(".tgz") && !url.endsWith(GridPilot.APP_EXTENSION) &&
          url.indexOf(".root")<0){
        if(!setRemoteTextEdit(url, httpsFileTransfer)){
          setRemoteFileConfirmDisplay(url, httpsFileTransfer);
        }
      }
      // remote s3 text file
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
      else if((url.endsWith(".gz") || url.endsWith(".tgz") || url.endsWith(GridPilot.APP_EXTENSION)) &&
          (url.startsWith("http://") || url.startsWith("file:"))){
        setFileConfirmDisplay(url);
      }
      // tarball on gridftp server
      else if(url.endsWith("gz") &&
          (url.startsWith("gsiftp:/"))){
        setRemoteFileConfirmDisplay(url, gsiftpFileTransfer);
      }
      // tarball on https server
      else if((url.endsWith(".gz") || url.endsWith(".tgz") || url.endsWith(GridPilot.APP_EXTENSION)) &&
          (url.startsWith("https:/"))){
        setRemoteFileConfirmDisplay(url, httpsFileTransfer);
      }
      // tarball on s3 server
      else if((url.endsWith(".gz") || url.endsWith(".tgz") || url.endsWith(GridPilot.APP_EXTENSION)) &&
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
        catch(FileTooBigException e){
          setRemoteFileConfirmDisplay(url, httpsFileTransfer);
        }
        catch(Exception e){
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
      else if(url.startsWith("srm:")){
        setRemoteFileConfirmDisplay(url, srmFileTransfer);
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
      ep.setContentType("text/plain");
      ep.setText("Could not open URL "+url);
      Debug.debug(msg, 1);
      throw e;
    }
  }

  private void setRemoteDirDisplay1(String url, FileTransfer fileTransfer, String string) throws Exception {
    try{
      String filter = jtFilter.getText();
      if(url.replaceFirst(":443/", "/").startsWith(GridPilot.APP_STORE_URL.replaceFirst(":443/", "/")) &&
          filter.equals(GlobalFrame.GPA_FILTER) && (thisUrl==null || !thisUrl.endsWith(GridPilot.APP_INDEX_FILE))
          ){
        try{
          long bytes = httpsFileTransfer.getFileBytes(new GlobusURL(url+GridPilot.APP_INDEX_FILE));
          if(bytes>0){
            setHtmlDisplay(url+GridPilot.APP_INDEX_FILE);
            return;
          }
        }
        catch(Exception ee){
        }
      }
      bOk.setEnabled(ok && isModal());
      bSave.setEnabled(false);
      bNew.setEnabled(true);
      bUpload.setEnabled(true);
      setRemoteDirDisplay(url, fileTransfer, string);
    }
    catch(Exception e){
      if(url.startsWith("https://") &&
          url.endsWith("/")){
        Debug.debug("Could not list directory contents with propfind, trying with get", 2);
        setHtmlDisplay(url);
        return;
      }
      e.printStackTrace();
      bSave.setEnabled(false);
      bOk.setEnabled(false);
      ep.setText("ERROR!\n\nThe directory could not be read.");
      pButton.updateUI();
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
    long bytes = -1;
    try{
      try{
        bytes = ft.getFileBytes(new GlobusURL(url));
      }
      catch(Exception ee){
      }
      if(bytes>MAX_FILE_EDIT_BYTES){
        //throw new IOException("File too big "+ft.getFileBytes(new GlobusURL(url)));
        return false;
      }
      if(bytes==-1){
        GridPilot.getClassMgr().getLogFile().addInfo("WARNING: File size not found.");
      }
      ft.getFile(new GlobusURL(url), tmpFile);
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
    finally{
      ep.setCursor(new Cursor(Cursor.DEFAULT_CURSOR));
      statusBar.setLabel("");
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
      
      bSave.setEnabled(withNavigation);
      bOk.setEnabled(ok);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);

      Debug.debug("Setting thisUrl, "+url+":"+bytes, 3);
      thisUrl = url;
      lastUrlsList = new String [] {thisUrl};
      lastSizesList = new String [] {Long.toString(bytes)};
      setUrl(thisUrl);
      statusBar.setLabel(" ");
    }
    catch(IOException e){
      Debug.debug("Could not set text editor for url "+url+". "+
         e.getMessage(), 1);
      throw e;
    }
    finally{
      tmpFile.delete();
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
      bSave.setEnabled(withNavigation);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
      BufferedReader in = new BufferedReader(
        new InputStreamReader((new URL(url)).openStream()));
      String text = "";
      String line;
      int lineNumber = 0;
      Debug.debug("Reading file "+url, 3);
      while((line=in.readLine())!=null){
        ++lineNumber;
        if(lineNumber>MAX_TEXT_EDIT_LINES){
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
      lastUrlsList = new String [] {thisUrl};
      lastSizesList = new String [] {Long.toString(LocalStaticShell.getSize(url))};
      statusBar.setLabel("");
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
  private void setHttpTextDisplay(String url) throws Exception{
    Debug.debug("setHttpTextDisplay "+url, 3);
    jtFilter.setEnabled(false);
    bSave.setEnabled(false);
    bNew.setEnabled(false);
    bUpload.setEnabled(false);
    bDownload.setEnabled(false);
    bRegister.setEnabled(false);
    URL readURL = new URL(url);
    String contentType = "";
    try{
      URLConnection conn = readURL.openConnection();
      contentType = conn.getContentType();
      Debug.debug("getContentType: "+conn.getContentType(), 2);
    }
    catch(Exception ee){
      ee.printStackTrace();
    }
    if(contentType.toLowerCase().startsWith("text/html")){
      throw new IOException("Content-type text/html will not be displayed as text.");
    }
    BufferedReader in = new BufferedReader(new InputStreamReader(readURL.openStream()));
    String text = "";
    String line;
    int lineNumber = 0;
    while((line=in.readLine())!=null){
      ++lineNumber;
      if(lineNumber>MAX_TEXT_EDIT_LINES){
        throw new FileTooBigException("File too big");
      }
      Debug.debug("-->"+line, 3);
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
    lastUrlsList = new String [] {thisUrl};
    lastSizesList = new String [] {Integer.toString(text.getBytes().length)};
    ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
  }

  /**
   * Set the EditorPane to display the HTML page url.
   */
  private void setHtmlDisplay(String url) throws Exception{
    Debug.debug("setHtmlDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);

      // This is necessary. If not done, ep thinks
      // this is a reload and does nothing...
      //ep.setPage("file:///");
      // Update: see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4412125
      ep.getEditorKit().createDefaultDocument();
      ep.getDocument().putProperty(Document.StreamDescriptionProperty, null);
      //
      ep.setPage(url);
      
      if(thisUrl!=null && thisUrl.endsWith(GridPilot.APP_INDEX_FILE) &&
          GlobalFrame.GPA_FILTER.equals(jtFilter.getText())){
      }
      
      ep.setEditable(false);
      pButton.updateUI();
      thisUrl = url;
      Debug.debug("Setting thisUrl, "+thisUrl, 3);
      setUrl(thisUrl);
      lastUrlsList = new String [] {thisUrl};
    }
    catch(Exception e){
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
      try{
        URLConnection connection = (new URL(url)).openConnection();
        //DataInputStream dis;
        //dis = new DataInputStream(connection.getInputStream());
              //while ((inputLine = dis.readLine()) != null){
              //    Debug.debug(inputLine, 3);
              //}
        //dis.close();
        long size = getFileSize(connection);
        ep.setText(FILE_FOUND_TEXT+size+" bytes.");
        Debug.debug("Setting thisUrl, "+url, 3);
        thisUrl = url;
        setUrl(thisUrl);
        lastUrlsList = new String [] {thisUrl};
        lastSizesList = new String [] {Long.toString(size)};
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

  private long getFileSize(URLConnection connection) {
    long size = connection.getContentLength();
    String contentLength = connection.getHeaderField("Content-Length");
    if(size<0 && contentLength!=null){
      size = Long.parseLong(contentLength);
    }
    return size;
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

    long bytes = -1;
    try{
      try{
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
        ep.setContentType("text/html");
        String filter = jtFilter.getText();
        Debug.debug("Remote file: "+url+" > "+withFilter+" > "+filter+" > "+GlobalFrame.GPA_FILTER, 2);
        if(url.endsWith(GridPilot.APP_EXTENSION) && !withFilter && filter!=null && filter.equals(GlobalFrame.GPA_FILTER)){
          String fileName = url.replaceFirst("^.*/([^/]+)$", "$1");
          ep.setText(FILE_FOUND_TEXT+bytes+" bytes.<br><br>" +
              "<i>Click \"OK\" to import the application/dataset</i> <b>"+fileName+".</b></html>");
        }
        else{
          ep.setText(FILE_FOUND_TEXT+bytes+" bytes.<br>" +
              "<a href=\""+url+"\">Click here to download</a>.</html>");
        }
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
      lastUrlsList = new String [] {thisUrl};
      lastSizesList = new String [] {Long.toString(bytes)};
    }
    catch(Exception e){
      Debug.debug("Could not set confirm display of "+url+". "+
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
      bOk.setEnabled(ok && isModal());
      bSave.setEnabled(false);
      bNew.setEnabled(true);
      bUpload.setEnabled(true);
      bRegister.setEnabled(false);
      String htmlText = "";
      try{
        String [] text = LocalStaticShell.listFiles(localPath);
        Debug.debug("found files "+text.length, 3);
        int directories = 0;
        int files = 0;
        Vector<String> textVector = new Vector<String>();
        String filter = jtFilter.getText();
        if(filter==null || filter.equals("")){
          filter = "*";
        }
        statusBar.setLabel("Filtering...");
        Debug.debug("Filtering with "+filter, 3);
        Vector<String> lastUrlVector = new Vector<String>();
        Vector<String> lastSizesVector = new Vector<String>();
        String bytes;
        for(int j=0; j<text.length; ++j){
          if(!jcbFilter.isSelected() &&
              text[j].substring(localPath.length()).matches("^\\.[^\\.].+")){
            continue;
          }
          if(!LocalStaticShell.isDirectory(text[j]) &&
              !MyUtil.filterMatches(text[j].substring(localPath.length()), filter)){
            continue;
          }
          if(LocalStaticShell.isDirectory(text[j])){
            ++directories;
          }
          else{
            ++files;
          }
          bytes = Long.toString(LocalStaticShell.getSize(text[j]));
          textVector.add("<a href=\"file:"+text[j]+"\">" + 
              (((text[j].matches("(\\w:\\\\).*") ||
                  text[j].matches("\\w:/.*")) &&
                  !localPath.matches("(\\w:\\\\).*") &&
                  !localPath.matches("\\w:/.*")) ? 
                  text[j].substring(localPath.length()+2) :
                    text[j].substring(localPath.length())) +  "</a> "+
                    bytes);
          lastUrlVector.add("file:"+text[j]);
          lastSizesVector.add(bytes);
        }
        lastUrlsList = new String [lastUrlVector.size()];
        lastSizesList = new String [lastUrlVector.size()];
        for(int j=0; j<lastUrlsList.length; ++j){
          lastUrlsList[j] = (String) lastUrlVector.get(j);
          lastSizesList[j] = (String) lastSizesVector.get(j);
        }
        ep.setContentType("text/html");
        htmlText = "<html>\n";
        if(!localPath.equals("/")){
          htmlText += "<a href=\"file:"+localPath+"../\">"/*+localPath*/+"../</a><br>\n";
        }
        htmlText += MyUtil.arrayToString(textVector.toArray(), "<br>\n");
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
    
    listedUrls = new Vector<String>();
    listedSizes = new Vector<String>();
    jtFilter.setEnabled(true);
    String filter = jtFilter.getText();
    String htmlText = "";
    String href = null;
    String sssBucketMatchPattern = "(?i)^sss:/+([^/]+/)$";

    url = url.replaceFirst("/[^\\/]*/\\.\\.", "");
    GlobusURL globusUrl = new GlobusURL(url);
    String host = globusUrl.getHost();
    int port = globusUrl.getPort();
    Debug.debug("Opening directory on remote server\n"+globusUrl.toString(), 3);
    String localPath = "/";
    if(globusUrl.getPath()!=null){
      localPath = globusUrl.getPath();
      if(!localPath.startsWith("/")){
        localPath = "/" + localPath;
      }
    }
    // URLs of the form sss://atlas_images/ will have getPath() null. Take care of these too...
    else if(globusUrl.getURL().matches(sssBucketMatchPattern)){
      localPath = globusUrl.getURL().replaceFirst(sssBucketMatchPattern, "$1");
      if(!localPath.startsWith("/")){
        localPath = "/" + localPath;
      }
      host = "";
    }
    Vector<String> textVector = ft.list(globusUrl,
        /*always list directories*/DIR_FILTER+
        (filter.equals("")?"*":filter));

    String text = "";
    // TODO: reconsider max entries and why listing more is so slow...
    // display max 500 entries
    // TODO: make this configurable
    int maxEntries = 500;
    boolean maxExceeded = textVector.size()>maxEntries;
    int length = textVector.size()<maxEntries ? textVector.size() : maxEntries;
    String name = null;
    String bytes = null;
    String longName = null;
    String [] nameAndBytes = null;
    lastUrlsList = new String [length];
    lastSizesList = new String [length];
    int directories = 0;
    int files = 0;
    String [] nameParts;
    for(int i=0; i<length; ++i){
      nameAndBytes = null;
      longName = textVector.get(i).toString();
      try{
        nameAndBytes = MyUtil.split(longName);
      }
      catch(Exception e){
      }
      if(nameAndBytes!=null && nameAndBytes.length>0){
        if(nameAndBytes.length>1){
          nameParts = nameAndBytes.clone();
          nameParts[nameParts.length-1] = "";
          name = MyUtil.arrayToString(nameParts).trim();
          bytes = nameAndBytes[nameAndBytes.length-1];
        }
        else{
          name = nameAndBytes[0];
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
      text += "<a href=\""+href+"\">"+MyUtil.urlDecode(name)+"</a> "+bytes;
      if(i<length-1){
        text += "<br>\n";
      }
      lastUrlsList[i] = protocol+"://"+host+(port>0?":"+port:"")+localPath+name;
      lastSizesList[i] = bytes;
      Debug.debug(textVector.get(i).toString(), 3);
    }
    ep.setContentType("text/html");
    htmlText = "<html>\n";
    
    if(!localPath.matches("/+")){
      htmlText += "<a href=\""+protocol+"://"+host+
      (port>0?(":"+port):"")+localPath+"../\">../</a><br>\n";
    }
    htmlText += text;
    if(maxExceeded){
      htmlText += "<br>\n...<br>\n...<br>\n...<br>\n";
    }
    htmlText += "\n</html>";
    Debug.debug("done parsing, setting text", 3);
    ep.setText(htmlText);
    ep.setEditable(false);
    // if we don't get an exception, the directory got read...
    //thisUrl = (new File(localPath)).toURL().toExternalForm();
    thisUrl = url;
    statusBar.setLabel(directories+(maxExceeded?"+":"")+" director"+(directories==1?"y":"ies")+", " +
        files+(maxExceeded?"+":"")+" file"+(files==1?"":"s"));
    bDownload.setEnabled(listedUrls!=null && listedUrls.size()>0);
    bRegister.setEnabled(allowRegister && listedUrls!=null && listedUrls.size()>0);
    setUrl(thisUrl);
  }

  /**
   * Set the EditorPane to display a directory listing
   * of the URL url.
   */
  /*private void setHttpDirDisplay(final String url) throws IOException{
    Debug.debug("setHttpDirDisplay "+url, 3);
    jtFilter.setEnabled(false);
    try{
      bSave.setEnabled(false);
      bNew.setEnabled(false);
      bUpload.setEnabled(false);
      bDownload.setEnabled(false);
      bRegister.setEnabled(false);
     
      ResThread t = new ResThread(){
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
      if(!MyUtil.myWaitForThread(t, "https", HTTP_TIMEOUT, "list", new Boolean(true))){
        if(statusBar!=null){
          statusBar.setLabel("List cancelled");
        }
        throw new IOException("List timed out");
      }
      
      // workaround for bug in java < 1.5
      // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4492274
      // Doesn't work...
      //if (!ep.getPage().equals(url))
      //{
      //  ep.getDocument().
      //    putProperty(Document.StreamDescriptionProperty, url);
      //}
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
  }*/

  public Dimension getPreferredSize(){
    return panel.getPreferredSize();
  }

  public Dimension getMinimumSize(){
    return panel.getPreferredSize();
  }

  // Overridden so we can exit when window is closed
  protected void processWindowEvent(WindowEvent e){
    if (e.getID()==WindowEvent.WINDOW_CLOSING){
      thisUrl = null;
      cancel();
    }
    super.processWindowEvent(e);
  }
  
  private boolean isDLWindow(){
    boolean ret = ep.getContentType().equalsIgnoreCase("text/html") &&
       ep.getText().matches("(?i)(?s)<html>.*<head>.*</head>.*<body>\\s*"+
       FILE_FOUND_TEXT+".*</body>.*</html>\\s*");
    return ret;
  }
  
  // Close the dialog
  private void cancel(){
    if(thisUrl==null || thisUrl.endsWith("/") /*|| isDLWindow()*/){
      //lastURL = Util.urlDecode(origUrl);
      lastURL = null;
      lastUrlsList = null;
      saveHistory();
      dispose();
    }
    else{
      try{
        String newUrl = thisUrl.substring(0, thisUrl.lastIndexOf("/")+1);
        if(MyUtil.isLocalFileName(thisUrl) && MyUtil.onWindows()){
          newUrl = thisUrl.substring(0, thisUrl.lastIndexOf(File.separator))+"/";
        }
        //thisUrl = newUrl;
        //setUrl(thisUrl);
        setDisplay0(newUrl);
      }
      catch(Exception ioe){
        ioe.printStackTrace();
        lastUrlsList = null;
        //lastURL = Util.urlDecode(origUrl);
        lastURL = null;
        dispose();
      }
    }
  }

  //Set lastURL and close the dialog
  void exit(){
    //GridPilot.lastURL = ep.getPage();
    Object currentUrl;
    if(withNavigation){
      currentUrl = currentUrlBox.getSelectedItem();
    }
    else{
      currentUrl = currentUrlLabel.getText();
    }
    if(currentUrl!=null && (
        thisUrl==null || thisUrl.equals("") || 
        !currentUrl.toString().equals(thisUrl))){
      thisUrl = currentUrl.toString();
    }
    Debug.debug("Setting lastURL, "+thisUrl, 2);
    lastURL = MyUtil.urlDecode(thisUrl);
    saveHistory();
    dispose();
  }
  
  void saveHistory(){
    Debug.debug("Saving history", 3);
    if(saveUrlHistory){
      try{
        LocalStaticShell.writeFile(GridPilot.BROWSER_HISTORY_FILE,
           MyUtil.arrayToString(urlList.toArray(),"\n"), false);
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
      if(!LocalStaticShell.mkdirs(fsPath)){
        throw new IOException("Could not make directory "+fsPath);
      }
    }
    else if(!fsPath.endsWith("/")){
      LocalStaticShell.writeFile(fsPath, text, false);
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
    if(!LocalStaticShell.deleteFile(fsPath)){
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
    String fileName = MyUtil.getName("File name (end with a / to create a directory)", "");   
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
  private void deleteFileOrDir(String url){
    if(url==null){
      return;
    }
    String msg = "Are you sure you want to delete "+url+(url.endsWith("/")?
        " and all contained files":"") +
    		"?";
    ConfirmBox confirmBox = new ConfirmBox(this);
    try{
      int choice = confirmBox.getConfirm("Confirm delete",
          msg, new Object[] {MyUtil.mkOkObject(confirmBox.getOptionPane()),
                             MyUtil.mkCancelObject(confirmBox.getOptionPane())});
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
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
      Debug.debug("Deleting file or dir "+url, 3);
      if(url.startsWith("file:") || url.startsWith("/")){
        localDeleteFile(url);
      }
      else{
        remoteDeleteFile(url);
      }
      statusBar.setLabel(url+" deleted");
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
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
    }
    catch(Exception e){
      ep.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
      Debug.debug("ERROR: could not delete "+url+". "+e.getMessage(), 1);
      e.printStackTrace();
      ep.setText("ERROR: "+url+" could not be deleted.\n\n"+
          //"If it is a directory, delete all files within first.\n\n"+
          e.getMessage());
      statusBar.setLabel("ERROR: The file "+url+" could not be deleted.");
      //pButton.updateUI();
    }
  }
  
  private void remoteDeleteFile(String url) throws Exception {
    String [] filesAndDirs = MyTransferControl.findAllFilesAndDirs(url, "")[0];
    Arrays.sort(filesAndDirs);
    GlobusURL globusUrl;
    for(int i=filesAndDirs.length-1; i>=0; --i){
      globusUrl = new GlobusURL(filesAndDirs[i]);
      GridPilot.getClassMgr().getTransferControl().deleteFiles(new GlobusURL [] {globusUrl});
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
        ret = sssFileTransfer.create(globusUrl);
        //ret = thisUrl;
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
        else if(thisUrl.startsWith("srm://")){
          GlobusURL globusUrl = new GlobusURL(thisUrl);
          srmFileTransfer.write(globusUrl, ep.getText());
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
        File fileOrDir = getInputFileOrDir();
        if(fileOrDir==null){
          return;
        }
        upload(fileOrDir, thisUrl);
        try{
          statusBar.setLabel("uploading "+thisUrl);
          ep.getDocument().putProperty(Document.StreamDescriptionProperty, null);
          setDisplay(thisUrl);
        }
        catch(Exception ioe){
          ioe.printStackTrace();
        }
        statusBar.setLabel(fileOrDir+" uploaded to "+thisUrl);
      }
      else if(e.getSource()==bDownload){
        final File dir = MyUtil.getDownloadDir(this);      
        if(dir==null){
          return;
        }
        downloadAll(dir);
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
  
  private void downloadAll(final File dir){
    ResThread t = (new ResThread(){
      public void run(){
        String href = null;
        for(Iterator<String> it=listedUrls.iterator(); it.hasNext();){
          href = (String) it.next();
          Debug.debug("Getting: "+href+" -> "+dir.getAbsolutePath(), 2);
          try{
            //TransferControl.download(href, dir, ep);
            // Use the physical file name (strip off the first ...://.../.../
            //GridPilot.getClassMgr().getTransferControl().startCopyFiles(new GlobusURL [] {new GlobusURL(href)},
            //    new GlobusURL [] {new GlobusURL("file:///"+
            //        (new File(dir, href.replaceFirst(".*/([^/]+)$", "$1"))))});
            download(href, dir);
          }
          catch(Exception e){
            GridPilot.getClassMgr().getLogFile().addMessage("Could not download file.", e);
          }
        }
        //statusBar.setLabel("All files downloaded");
        MyUtil.showMessage(SwingUtilities.getWindowAncestor(getThis()),
            "Download ok", "All file(s) downloaded.");
      }
    });
    //SwingUtilities.invokeLater(t);
    t.start();
  }

  /**
   * Upload a file or directory.
   */
  private void upload(final File fileOrDir, final String url){
    if(fileOrDir==null){
      return;
    }
    MyResThread rt = new MyResThread(){
      public void run(){
        if(fileOrDir.isDirectory()){
          uploadDir(fileOrDir, url);
        }
        else{
          uploadFile(fileOrDir, url);
        }
      }
    };
    rt.start();
    ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
    MyUtil.waitForThread(rt, "upload", UPLOAD_TIMEOUT, "upload", GridPilot.getClassMgr().getLogFile());
    ep.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
  }
  
  /**
   * Upload a single file.
   */
  private void uploadFile(File file, final String url){
    Debug.debug("Putting file : "+file.getAbsolutePath()+" -> "+url, 3);
    try{
      statusBar.setLabel("Uploading "+url);
      GridPilot.getClassMgr().getTransferControl().upload(file, url);
      Debug.debug("Upload done, "+url, 2);
      statusBar.setLabel("");
    }
    catch(Exception e){
      String error = "Could not upload "+url;
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      showError(error+" : "+e.getMessage());
    }
  }

  /**
   * Upload directory including all files and subdirs.
   */
  private void uploadDir(File dir, final String url){
    Debug.debug("Uploading to "+url, 3);
    Debug.debug("Listing files in directory "+dir.getAbsolutePath(), 2);
    String[] allFilesAndDirs;
    try{
      statusBar.setLabel("Listing files in directory "+dir.getAbsolutePath());
      String filter = jtFilter.getText();
      //allFileAndDirs = LocalStaticShell.listFilesAndDirsRecursively(dir.getAbsolutePath());
      allFilesAndDirs = MyTransferControl.findAllFilesAndDirs(dir.getAbsolutePath()+File.separator, filter)[0];
      Debug.debug("Found: "+allFilesAndDirs.length+" files and/or dirs", 2);
    }
    catch(Exception e){
      String error = "Could not upload "+dir.getAbsolutePath();
      GridPilot.getClassMgr().getLogFile().addMessage(error, e);
      showError(error+" : "+e.getMessage());
      return;
    }
    // Src file
    String srcFile;
    // Unqualified src path
    String srcFileStr;
    // Name of directory
    String dirName = dir.getName();
    Debug.debug("Uploading whole directory "+dirName, 2);
    statusBar.setLabel("Queuing uploads to "+url);
    GlobusURL srcUrl;
    GlobusURL destUrl;
    Vector<TransferInfo> transfers = new Vector<TransferInfo>();
    // List of directories to be created
    HashSet<GlobusURL> newDirUrls = new HashSet<GlobusURL>();
    try{
      for(int i=0; i<allFilesAndDirs.length; ++i){
        Debug.debug("Queuing "+allFilesAndDirs[i], 3);
        srcFile = MyUtil.clearFile(allFilesAndDirs[i]);
        srcUrl = new GlobusURL("file:///"+srcFile);
        srcFileStr = srcFile.replaceFirst(dir.getAbsolutePath(), "");
        destUrl = new GlobusURL((url+dirName+srcFileStr).replaceAll("\\\\", "/"));
        Debug.debug("Will upload "+srcFileStr+": "+srcUrl.getURL()+"-->"+destUrl.getURL(), 2);
        if(srcUrl.getURL().endsWith(File.separator) || LocalStaticShell.existsFile(srcFile) &&
            LocalStaticShell.isDirectory(srcFile)){
          newDirUrls.add(destUrl);
        }
        else{
          transfers.add(new TransferInfo(srcUrl, destUrl));
        }
      }
      GridPilot.getClassMgr().getTransferControl().queue(transfers);
      createRemoteDirs(newDirUrls);
    }
    catch(Exception e){
      GridPilot.getClassMgr().getLogFile().addMessage("Could not upload directory "+dir.getAbsolutePath(), e);
      statusBar.setLabel("Queuing failed");
      return;
    }
    Debug.debug("Queuing done, "+url, 2);
    statusBar.setLabel("Queuing done");
    GridPilot.getClassMgr().getGlobalFrame().showMonitoringPanel(MonitoringPanel.TAB_INDEX_TRANSFERS);
  }

  private void createRemoteDirs(HashSet<GlobusURL> newDirs) throws Exception {
    GlobusURL newDir;
    long dirSize = -1;
    for(Iterator<GlobusURL> it=newDirs.iterator(); it.hasNext();){
      newDir = it.next();
      dirSize = -1;
      try{
        dirSize = GridPilot.getClassMgr().getTransferControl().getFileBytes(newDir);
      }
      catch(Exception e){
        e.printStackTrace();
      }
      Debug.debug("Checking dir: "+newDir.getURL()+"-->"+dirSize, 3);
      if(dirSize<0){
        Debug.debug("Creating dir: "+newDir.getURL(), 3);
        GridPilot.getClassMgr().getTransferControl().mkDir(newDir);
      }
    }
  }

  private File getInputFileOrDir(){
    File file = null;
    JFileChooser fc = new JFileChooser();
    fc.setDialogTitle("Choose file or directory to upload");
    //fc.setFileSelectionMode(JFileChooser.FILES_ONLY);
    fc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
    int returnVal = fc.showOpenDialog(this);
    if(returnVal==JFileChooser.APPROVE_OPTION){
      file = fc.getSelectedFile();
      Debug.debug("Opening: " + file.getName(), 2);
    }
    else{
      Debug.debug("Not opening file", 3);
    }
    return file;
  }
  
  private void registerAll(final String dbName) {
    ResThread t = (new ResThread(){
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
          Iterator<String> itt=listedSizes.iterator();
          for(Iterator<String> it=listedUrls.iterator(); it.hasNext();){
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
    //SwingUtilities.invokeLater(t);
    t.start();
  }

  private void addUrl(String url){
    synchronized(urlList){
      if(urlList==null){
        Debug.debug("urlList null", 3);
      }
      urlList.add(url);
    }
  }

  private void removeUrl(String url){
    synchronized(urlList){
      if(urlList==null){
        Debug.debug("urlList null", 3);
      }
      urlList.remove(url);
    }
  }

  /*private void clearUrls(String url){
    synchronized(urlList){
      if(urlList==null){
        Debug.debug("urlList null", 3);
      }
      urlList.removeAllElements();
    }
  }*/
  
}