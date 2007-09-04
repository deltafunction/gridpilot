package gridpilot;

import javax.swing.*;
import javax.swing.text.*;
import java.awt.*;
import java.awt.event.*;

public class LogViewerPanel extends JTextPane{

  private static final long serialVersionUID = 1L;
  private JPopupMenu popupMenu = new JPopupMenu();
  boolean showInfoMessages = true;
  boolean showHeader = true;

  SimpleAttributeSet attrBlack = new SimpleAttributeSet();
  SimpleAttributeSet attrRed = new SimpleAttributeSet();
  DefaultStyledDocument doc;

  public LogViewerPanel(){
    Debug.debug("New LogViewerPanel", 3);
    doc = new DefaultStyledDocument();
    setDocument(doc);
    GridPilot.getClassMgr().getLogFile().addActionOnMessage(new LogFile.ActionOnMessage(){
      public void newMessage(String head, String cont, boolean isError){
        Debug.debug("Adding message: "+cont, 3);
        addLogMessage(head, cont, isError);
      }
    });
    setEditable(false);

    StyleConstants.setForeground(attrBlack, Color.black);
    StyleConstants.setForeground(attrRed, Color.red);

    createDefaultEditorKit();

    createMenu();
  }

  private void createMenu(){
    JMenuItem miClear = new JMenuItem("Clear");
    final JCheckBoxMenuItem cbmiShowInfoMessages = new JCheckBoxMenuItem("Show new information messages");
    final JCheckBoxMenuItem cbmiShowHeader = new JCheckBoxMenuItem("Show headers for new messages");

    miClear.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        setText("");
      }
    });
    cbmiShowInfoMessages.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        showInfoMessages = cbmiShowInfoMessages.isSelected();
      }
    });
    cbmiShowHeader.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        showHeader = cbmiShowHeader.isSelected();
      }
    });


    popupMenu.add(miClear);
    popupMenu.add(cbmiShowInfoMessages);
    popupMenu.add(cbmiShowHeader);

    cbmiShowHeader.setSelected(showHeader);
    cbmiShowInfoMessages.setSelected(showInfoMessages);

    this.addMouseListener(new java.awt.event.MouseAdapter() {
      public void mousePressed(MouseEvent e) {
        if (e.getButton()!=MouseEvent.BUTTON1) // right button
          popupMenu.show(e.getComponent(), e.getX(), e.getY());
      }
    });
  }

  private void addLogMessage(String header, String cont, boolean isError) {

    if(!isError && !showInfoMessages){
      return;
    }
    try{
      doc.setParagraphAttributes(doc.getLength(), 0, isError ? attrRed : attrBlack, true);
      if(showHeader){
        doc.insertString(doc.getLength(),
                         header +
                         (header.endsWith("\n") ? "" : "\n") +
                         "_____________\n", null);
      }
      doc.insertString(doc.getLength(),
                       cont +
                       (cont.endsWith("\n") ? "" : "\n"),
                       null);

      doc.insertString(doc.getLength(),
                       "______________________________________________________\n",
                       attrBlack);
      setCaretPosition(doc.getLength());
    }
    catch(BadLocationException ble){
      ble.printStackTrace();
    }
  }
}