package org.apache.lucene.queryparser.flexible.standard.processors;

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

public class OpenRangeQueryNodeProcessor extends QueryNodeProcessorImpl {
  
  final public static String OPEN_RANGE_TOKEN = "*";
  
  public OpenRangeQueryNodeProcessor() {}
  
  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    
    if (node instanceof TermRangeQueryNode) {
      TermRangeQueryNode rangeNode = (TermRangeQueryNode) node;
      FieldQueryNode lowerNode = (FieldQueryNode) rangeNode.getLowerBound();
      FieldQueryNode upperNode = (FieldQueryNode) rangeNode.getUpperBound();
      CharSequence lowerText = lowerNode.getText();
      CharSequence upperText = upperNode.getText();
      
      
      if (OPEN_RANGE_TOKEN.equals(upperNode.getTextAsString())
          && (!(upperText instanceof UnescapedCharSequence) || !((UnescapedCharSequence) upperText)
              .wasEscaped(0))) {
        upperText = "";
      }
      
      if (OPEN_RANGE_TOKEN.equals(lowerNode.getTextAsString())
          && (!(lowerText instanceof UnescapedCharSequence) || !((UnescapedCharSequence) lowerText)
              .wasEscaped(0))) {
        lowerText = "";
      }
      
      lowerNode.setText(lowerText);
      upperNode.setText(upperText);
      
    }
    
    return node;
    
  }
  
  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {
    return node;
  }
  
  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {
    return children;
  }
  
}
