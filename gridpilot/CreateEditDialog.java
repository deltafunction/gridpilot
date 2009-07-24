package gridpilot;

import gridfactory.common.Debug;

import java.awt.*;

import javax.swing.*;

import java.awt.event.*;
import java.net.URL;

public class CreateEditDialog extends GPFrame /*implements ComponentListener*/{

  private static final long serialVersionUID = 1L;
  private static final int BCLOSE = 0;
  private static final int BCREATEUPDATE = 1;
  private static final int BCLEAR = 2;
  private static final int BSAVE_SETTINGS = 3;
  private JPanel pCreateEdit = new JPanel(new BorderLayout());
  private CreateEditPanel createEditPanel;
  private boolean editing;
  private JButton bClose;
  private JButton bCreateUpdate = null;
  private JButton bSaveSettings;
  private JButton bClear;
  private JCheckBox cbShowResults = new JCheckBox("Confirm before writing", true);
  private boolean showDetailsCheckBox = false;
  private boolean showButtons = false;
  private boolean showSaveSettings = false;
  private JCheckBox cbShowDetails = null;
  private JPanel buttonPanel = new JPanel();

  public CreateEditDialog(CreateEditPanel _panel, boolean _editing,
      boolean _showDetailsCheckBox, boolean _showButtons, boolean _showSaveSettings,
      boolean visible){
    
    super();
    
    _panel.statusBar = this.statusBar;
    
    showDetailsCheckBox = _showDetailsCheckBox;
    showButtons = _showButtons;
    createEditPanel = _panel;
    editing = _editing;
    showSaveSettings = _showSaveSettings;
    
    this.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
    this.addWindowListener(new WindowAdapter(){
      public void windowClosing(WindowEvent we){
        createEditPanel.windowClosing();
        we.getWindow().setVisible(false);
        Debug.debug("Thwarted user attempt to close window.", 3);
      }
    });

    initButtons();
    
    try{
      this.getContentPane().add(pCreateEdit, BorderLayout.CENTER);
      initGUI();
      pack();
      requestFocusInWindow();
      // Doesn't seem to make any difference...
      //setAlwaysOnTop(false);
      this.setVisible(visible);
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  
  private void initButtons(){
    URL imgURL;
    ImageIcon imgIcon;
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "stop.png");
      imgIcon = new ImageIcon(imgURL);
      bClose = new JButton(imgIcon);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + "stop.png", 3);
      bClose = new JButton("Cancel");
    }
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "clear.png");
      imgIcon = new ImageIcon(imgURL);
      bClear = new JButton(imgIcon);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + "clear.png", 3);
      bClear = new JButton("Clear");
    }
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "ok.png");
      imgIcon = new ImageIcon(imgURL);
      bCreateUpdate = new JButton(imgIcon);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + "ok.png", 3);
      bCreateUpdate = new JButton(editing?"Update":"Create");
    }
    try{
      imgURL = GridPilot.class.getResource(GridPilot.ICONS_PATH + "save.png");
      imgIcon = new ImageIcon(imgURL);
      bSaveSettings = new JButton(imgIcon);
    }
    catch(Exception e){
      Debug.debug("Could not find image "+ GridPilot.ICONS_PATH + "save.png", 3);
      bSaveSettings = new JButton("Save values");
    }
    bClose.setToolTipText("Cancel");
    bClear.setToolTipText("Clear fields");
    bCreateUpdate.setToolTipText((editing?"Update":"Create")+" record(s)");
    bSaveSettings.setToolTipText("Save the values of all fields");
  }
  
  public void initGUI() throws Exception{
    
    // buttons initialisation
    bClose.setMnemonic(BCLOSE);
    bClose.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bClear.setMnemonic(BCLEAR);
    bClear.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bCreateUpdate.setMnemonic(BCREATEUPDATE);
    bCreateUpdate.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    bSaveSettings.setMnemonic(BSAVE_SETTINGS);
    bSaveSettings.addActionListener(new java.awt.event.ActionListener(){
      public void actionPerformed(ActionEvent e){
        button_actionPerformed(e);
      }
    });

    pCreateEdit.add(createEditPanel, BorderLayout.CENTER);

    if(showButtons){
      if(showDetailsCheckBox){
        cbShowDetails = new JCheckBox("Show details", false);
        cbShowDetails.addActionListener(new ActionListener(){
          public void actionPerformed(ActionEvent e){
            try{
              createEditPanel.showDetails(cbShowDetails.isSelected());
              pack();
            }
            catch(Exception ex){
              Debug.debug("Could not show details", 2);
              ex.printStackTrace();
            }
          }
        });    
        buttonPanel.add(cbShowDetails);
      }
      buttonPanel.add(cbShowResults);
      buttonPanel.add(bClose);
      buttonPanel.add(bClear);
      buttonPanel.add(new JLabel("|"));
      buttonPanel.add(bCreateUpdate);
      if(showSaveSettings){
        buttonPanel.add(bSaveSettings);
      }

      pCreateEdit.add(buttonPanel, BorderLayout.SOUTH);
    }

    // center in window
    //
    //Dimension  scrnSize = Toolkit.getDefaultToolkit().getScreenSize();
    //setLocation((scrnSize.width / 2) - 150, (scrnSize.height / 2) - 50);
    //this.setSize(new Dimension(400, 0));

    // Initialize CreateEditPanel
    createEditPanel.initGUI();
    
    pack();
  }
  
  public void activate() throws Exception{
    createEditPanel.activate();
  }
  
  public void setBCreateUpdateEnabled(boolean ok){
    bCreateUpdate.setEnabled(ok);
  }

  /**
   * Called when a button is clicked
   */
  void button_actionPerformed(ActionEvent e){
    MyResThread rt;
    switch(((JButton)e.getSource()).getMnemonic()){
      case BCLOSE :
        createEditPanel.windowClosing();
        this.setVisible(false);
        break;

      case BCREATEUPDATE :
        rt = new MyResThread(){
          public void run(){
            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            createEditPanel.create(cbShowResults.isSelected(), editing);
            setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
          }
        };
        rt.start();
        break;

      case BSAVE_SETTINGS :
        rt = new MyResThread(){
          public void run(){
            createEditPanel.saveSettings();
          }
        };
        rt.start();
        break;

      case BCLEAR :
        rt = new MyResThread(){
          public void run(){
            createEditPanel.clearPanel();
          }
        };
        rt.start();
        break;
    }
  }
}
