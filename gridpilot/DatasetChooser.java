package gridpilot;

import gridfactory.common.DBResult;

import java.awt.BorderLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class DatasetChooser extends JPanel {

  private static final long serialVersionUID = 1L;
  private static int TEXTFIELDWIDTH = 32;
  private JComponent dsField = new JTextField(TEXTFIELDWIDTH);
  private DBPluginMgr dbPluginMgr;
  private JPanel jPanel;
  
  public DatasetChooser(DBPluginMgr _dbPluginMgr, JPanel _jPanel){
    this.setLayout(new BorderLayout());
    dbPluginMgr = _dbPluginMgr;
    jPanel = _jPanel;
    createDatasetChooser();
  }
  
  public DatasetChooser(JPanel _jPanel) {
    this.setLayout(new BorderLayout());
    jPanel = _jPanel;
    createDatasetChooser();
  }

  public DatasetChooser() {
  }

  private void createDatasetChooser() {
    final JButton jbLookup = MyUtil.mkButton("search.png", "Look up", "Search results for this request");
    add(dsField, BorderLayout.WEST);
    add(jbLookup, BorderLayout.EAST);
    updateUI();
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
        removeAll();
        add(dsField, BorderLayout.WEST);
        add(jbLookup, BorderLayout.EAST);
        updateUI();
        add(dsField, BorderLayout.CENTER);
        updateUI();
        validate();
        if(jPanel!=null){
          jPanel.updateUI();
          jPanel.validate();
        }
      }
    });
  }
  
  public String getText(){
    return MyUtil.getJTextOrEmptyString(dsField);
  }

  public void setText(String text){
    MyUtil.setJText(dsField, text);
  }
  
  public void setDB(String dbName){
    dbPluginMgr = GridPilot.getClassMgr().getDBPluginMgr(dbName);
  }

}
