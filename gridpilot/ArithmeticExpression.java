package gridpilot;

import gridpilot.Debug;

/**
 * Evaluates arithmetic expressions with one variable.
 *
 * One instance of this class is create by JobDefCreator each time this object
 * needs to evaluate an arithmetic expression.
 *
 * Evaluate expression respect following priorities : (*,/,%), (+,-), all operators between ()
 * have same priority and are left associatives
 *
 */

class ArithmeticExpression {
  /** Expression to evalutate*/
  private String expression;
  /** Index of the current character */
  private int cursor=0;
  /** Character at index <code>cursor</code> */
  private char currentChar;
  /** Numerical value of this expression */
  private int value;
  /** Name of the variable */
  private char variableName;
  /** Numeric value of the variable */
  private int variableValue;

  /** Allows to detect that the String has been read completely */
  private char endOfExpr = '}';

  /**
   * Constructs a new expression evaluator, based on the specified expression, with this
   * specified variable which has the specified value
   */
  public ArithmeticExpression(String _expression, char _variableName, int _variableValue) throws ArithmeticException{
    Debug.debug("Constr : "+ _expression, 1);
    expression = _expression.trim() ;
    variableName = _variableName;
    variableValue = _variableValue;
    nextChar();
    value = Expr();
    if(currentChar != endOfExpr)
      throw new ArithmeticException(expression+" : misformed expression - "+currentChar+" unexpected");
  }

  /**
   * Returns the value of this expression, evaluated in the constructor
   * @return the numeric value of this expression
   */
  public int getValue(){
      return value;
  }

  /**
   * Sets currentChar to the next significant character
   */
  private void nextChar(){
    if(cursor < expression.length()){

      do{ // skips spaces
        currentChar = expression.charAt(cursor);
        ++cursor;
      }while(currentChar == ' ');
      // no out of bound possible : expression cannot end by space
      // (expression = _expression.trim())
    }
    else
      currentChar = endOfExpr; // Term, Expr or Fact end on this character

    Debug.debug("nextChar : "+currentChar, 2);
  }

  /**
   * Returns the numerical value of the next "sub-expression".
   * &lt;Expr> ::= &lt;Term&gt; {+ &lt;Term&gt; | - &lt;Term&gt;}
   *
   * @return the numerical value of the next "sub-expression"
   */
  private int Expr () throws ArithmeticException{
    Debug.debug("Expr ("+currentChar+")", 2);
   int res = Term();
   while (currentChar == '+' || currentChar == '-') {
      if (currentChar == '+') { nextChar(); res += Term(); }
      else          { nextChar(); res -= Term(); }
   }
   return res;
  }

  /**
   * Returns the numerical value of the next "sub-term" .
   * &lt;Term> ::= &lt;Fact&gt; {* &lt;Fact&gt; | / &lt;Fact&gt; | % &lt;Fact&gt;}
   *
   * @return the numerical value of the next "sub-Term"
   */
  private int Term () throws ArithmeticException {
    Debug.debug("Term ("+currentChar+")", 2);

   int res = Fact();
   while (currentChar == '*' || currentChar == '/' || currentChar == '%') {
      if (currentChar == '*') { nextChar(); res *= Fact(); }
      else if(currentChar == '/'){ nextChar(); res /= Fact(); }
      else { nextChar(); res %= Fact(); }
   }
   return res;
  }

  /**
   * Returns the numerical value of the next "sub-factor".
   * &lt;Fact&gt; ::= (&lt;Expr&gt;) | &lt;number&gt; | &lt;var&gt;
   * &lt;number&gt; ::= &lt;digit&gt;{&lt;digit&gt;}
   * &lt;digit&gt; ::= 0-9 
   * &lt;var&gt; ::= a-zA-Z
   *
   * @return the numerical value of the next "sub-factor"
   */
  private int Fact () throws ArithmeticException{
    Debug.debug("Fact ("+currentChar+")", 2);
    int res = 0;
    if (currentChar == '('){ // parenthesised expression
      nextChar();
      res = Expr();
      if(currentChar != ')')
        throw new ArithmeticException(expression+" : misformed expression - ) expected");
      nextChar();
    }
    else if (Character.isDigit(currentChar)) { // Number
      do {
        res = 10*res + Character.getNumericValue(currentChar);
        nextChar();
      }while (Character.isDigit(currentChar));
    }
    else if (currentChar == variableName){ //variable
      res = variableValue;
      nextChar();
    }
    else{   //error
      throw new ArithmeticException(expression+" : misformed expression - " + currentChar +" unexpected");
    }
    return res;
  }
}


