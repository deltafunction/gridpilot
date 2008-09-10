package gridpilot;

import gridfactory.common.Debug;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FontMetrics;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JScrollPane;
import javax.swing.plaf.basic.BasicComboPopup;

/**
 * ComboBox with Resizable pop up.
 **/
public class JExtendedComboBox extends JComboBox{
  
  private static final long serialVersionUID = 1L;

  public JExtendedComboBox(){
    super();
  }
  
  public JExtendedComboBox(Vector vector){
    super(vector);
  }
  
  public void updateUI(){
    super.updateUI();
    resizeComboPopup();
  }
  
  private void resizeComboPopup(){
    FontMetrics fm = getFontMetrics(getFont());
    BasicComboPopup popup = (BasicComboPopup)getUI().getAccessibleChild(this, 0);
    if(popup==null){
      return;
    }
    int width = (int)getPreferredSize().getWidth();
    int height = 0;
    int vPad = 2;
    int vOffset = 0;
    if(getItemCount()>0){
      vOffset = 2;
    }
    for(int i=0; i<getItemCount(); i++){
      String str = (String) getItemAt(i);
      if(width<fm.stringWidth(str)){
        width = fm.stringWidth(str);
      }
      height += (fm.getHeight() + vPad);
    }
    Component comp = popup.getComponent(0);
    JScrollPane scrollpane=null;
    int hOffset = 10;
    if(comp instanceof JScrollPane){
      scrollpane = (JScrollPane)comp;
      if(scrollpane.getVerticalScrollBar().isVisible()){
        hOffset += scrollpane.getVerticalScrollBar().getWidth();
      }    
    }
    Debug.debug("Setting preferred height: "+popup.getPreferredSize().height+
        " : "+fm.getHeight() + " : " + getItemCount(), 3);
    popup.setPreferredSize(new Dimension(width+hOffset,
        /*popup.getPreferredSize().height*/height+vOffset));
    popup.setLayout(new BorderLayout());
    popup.add(comp, BorderLayout.CENTER);
  }
}
