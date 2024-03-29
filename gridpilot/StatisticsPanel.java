package gridpilot;

import javax.swing.JPanel;
import javax.swing.border.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;

/**
 * Shows some charts about the jobs/transfers status.
 */
public abstract class StatisticsPanel extends JPanel{

  private static final long serialVersionUID = 1L;

  protected interface painter{
    public void paint(Graphics2D g);
  }

  protected String [] statusNames;
  protected int [] values = null; 
  protected Color [] colors;
  protected Color [] numberColors = {Color.white};
  protected int style =0;
  protected Vector painters = new Vector();

  public StatisticsPanel(String title){
        
    setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED), title));

    addMouseListener(new MouseAdapter(){
      public void mouseClicked(MouseEvent e){
        if(e.getButton()==MouseEvent.BUTTON1){
          style = (style+1) % (painters.size()*2);
        }
        else{
          style = (style==0 ? painters.size()*2-1 : style-1);
        }
        update();
        //repaint();
      }
    });

    painters.add(new painter(){
      public void paint(Graphics2D g){
        paintBarChart(g);
      }
    });

    painters.add(new painter(){
      public void paint(Graphics2D g){
        paintPieChart(g);
      }
    });

  }

  public void update(){
  }


  public void paint(Graphics g){
    //Debug.debug("painting stastistics panel", 3);
    super.paint(g);
    if(values==null){
      update();
    }
    ((painter) painters.get(style%painters.size())).paint((Graphics2D)g);
  }

  private void paintPieChart(Graphics2D g){

    int topMargin = 20;
    int bottomMargin = 10;
    int horMargin = 10;

    FontMetrics metrics = new Canvas().getFontMetrics(g.getFont());
    float maxWidth = 0;
    for(int i=0; i<statusNames.length; ++i){
      maxWidth = Math.max(maxWidth, metrics.stringWidth(values[i] + " " +statusNames[i]));
    }
    int nbColumn = 2;//(int) ((getWidth() - 2*horMargin) / maxWidth );
    if(nbColumn>statusNames.length){
      nbColumn = statusNames.length;
    }

    float ratio = ((getWidth() - 2*horMargin)/nbColumn) / maxWidth;
    if(ratio<1){
      g.setFont(g.getFont().deriveFont(g.getFont().getSize() * ratio ));
      maxWidth *=ratio;
    }

    int bottom = getHeight() - bottomMargin -
                 (int)Math.ceil((double)statusNames.length / nbColumn)*metrics.getAscent();

    int size = Math.min(bottom - topMargin, getWidth() - 2 * horMargin);
    int top = topMargin + (bottom - topMargin - size)/2;
    int rigth = horMargin + (getWidth() - 2* horMargin - size)/2;

    double total = 0;
    for(int i=0; i<values.length; ++i){
      total+=values[i];
    }
    
    double begin= 90;

    int col=0;
    int row = 0;
    int nbRow = (int) Math.ceil((double)statusNames.length / nbColumn);

    for(int i=0; i<statusNames.length; ++i){
      g.setColor(colors[i%colors.length]);
      g.drawString(values[i] + " " + statusNames[i],
                   horMargin + col*(getWidth() - 2*horMargin)/nbColumn,
                   bottom + (row + 1) *metrics.getAscent());
      row = row + 1;
      if(row==nbRow){
        ++col;
        row = 0;
      }
      
      double angle = (values[i] * 360.0 )/ total;
      
      g.fillArc(rigth, top, size, size,
                (int) begin, -(int) Math.ceil(angle));
      begin -=angle;
    }
  }

  private void paintBarChart(Graphics2D g){

    int topMargin = 20;
    int bottomMargin = 5;//20;
    int horMargin = 5;
    FontMetrics metrics = new Canvas().getFontMetrics(g.getFont());
    int columnWitdh = (getWidth() - 2 * horMargin) / statusNames.length;
    int bottom = getHeight() - bottomMargin; //- metrics.getAscent()-1;
    double step = (double)(bottom - topMargin) / maxValues();
    int inset = columnWitdh/4;
    
    //Debug.debug("max possible value: "+maxValues(), 3);

    for(int i=0; i<statusNames.length; ++i){
      /*Debug.debug("column: "+statusNames.length+":"+i, 3);
      Debug.debug("y: "+(bottom -(int)(step*values[i])), 3);
      Debug.debug("value: "+values[i], 3);*/
      
      g.setColor(colors[i%colors.length]);
      g.fill3DRect(horMargin + i*columnWitdh + inset/2,
                   bottom -(int)(step*values[i]),
                   columnWitdh - inset, (int)(step*values[i]), true);

      String value = ""+values[i];

      int valueWitdh = metrics.stringWidth(value);

      g.setColor(numberColors[i%numberColors.length]);
      //g.setFont(g.getFont());
      g.drawString(value, horMargin + i*columnWitdh + (columnWitdh - valueWitdh)/2, bottom-5);
      //Debug.debug(i + ", " + );

      g.setColor(Color.white);

      int nameWitdh = metrics.getHeight();//statusNames[i]);

      int deltaX = horMargin + i*columnWitdh + (columnWitdh - nameWitdh)/2 + metrics.getAscent() -1;
      int deltaY = bottom - metrics.getAscent() - 5;
      g.translate(deltaX, deltaY);
      g.rotate(-Math.PI/2.0);


      g.drawString(statusNames[i], 0, 0);

      g.rotate(Math.PI/2.0);

      g.translate(-deltaX, -deltaY);

    }
  }

  private int maxValues(){
    int max = values[0];
    for(int i=1; i<values.length; ++i){
      if(values[i]>max){
        max = values[i];
        //break;
        //max += values[i];
      }
    }
    return max;
  }
}
