package org.apache.lucene.util.automaton.fst;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;

import java.io.IOException;

/** Can next() and advance() through the terms in an FST
  * @lucene.experimental
*/

public class IntsRefFSTEnum<T> {
  private final FST<T> fst;

  private IntsRef current = new IntsRef(10);
  @SuppressWarnings("unchecked") private FST.Arc<T>[] arcs = (FST.Arc<T>[]) new FST.Arc[10];
  // outputs are cumulative
  @SuppressWarnings("unchecked") private T[] output = (T[]) new Object[10];

  private boolean lastFinal;
  private boolean didEmpty;
  private final T NO_OUTPUT;
  private final InputOutput<T> result = new InputOutput<T>();

  public static class InputOutput<T> {
    public IntsRef input;
    public T output;
  }
  
  public IntsRefFSTEnum(FST<T> fst) {
    this.fst = fst;
    result.input = current;
    NO_OUTPUT = fst.outputs.getNoOutput();
  }

  public void reset() {
    lastFinal = false;
    didEmpty = false;
    current.length = 0;
    result.output = NO_OUTPUT;
  }

  /** NOTE: target must be >= where we are already
   *  positioned */
  public InputOutput<T> advance(IntsRef target) throws IOException {

    assert target.compareTo(current) >= 0;

    //System.out.println("    advance len=" + target.length + " curlen=" + current.length);

    // special case empty string
    if (current.length == 0) {
      if (target.length == 0) {
        final T output = fst.getEmptyOutput();      
        if (output != null) {
          if (!didEmpty) {
            current.length = 0;
            lastFinal = true;
            result.output = output;
            didEmpty = true;
          }
          return result;
        } else {
          return next();
        }
      }
      
      if (fst.noNodes()) {
        return null;
      }
    }

    // TODO: possibly caller could/should provide common
    // prefix length?  ie this work may be redundant if
    // caller is in fact intersecting against its own
    // automaton

    // what prefix does target share w/ current
    int idx = 0;
    while (idx < current.length && idx < target.length) {
      if (current.ints[idx] != target.ints[target.offset + idx]) {
        break;
      }
      idx++;
    }

    //System.out.println("  shared " + idx);

    FST.Arc<T> arc;
    if (current.length == 0) {
      // new enum (no seek/next yet)
      arc = fst.readFirstArc(fst.getStartNode(), getArc(0));
      //System.out.println("  new enum");
    } else if (idx < current.length) {
      // roll back to shared point
      lastFinal = false;
      current.length = idx;
      arc = arcs[idx];
      if (arc.isLast()) {
        if (idx == 0) {
          return null;
        } else {
          return next();
        }
      }
      arc = fst.readNextArc(arc);
    } else if (idx == target.length) {
      // degenerate case -- seek to term we are already on
      assert target.equals(current);
      return result;
    } else {
      // current is a full prefix of target
      if (lastFinal) {
        arc = fst.readFirstArc(arcs[current.length-1].target, getArc(current.length));
      } else {
        return next();
      }
    }

    lastFinal = false;

    assert arc == arcs[current.length];
    int targetLabel = target.ints[target.offset+current.length];

    while(true) {
      //System.out.println("    cycle len=" + current.length + " target=" + ((char) targetLabel) + " vs " + ((char) arc.label));
      if (arc.label == targetLabel) {
        grow();
        current.ints[current.length] = arc.label;
        appendOutput(arc.output);
        current.length++;
        grow();
        if (current.length == target.length) {
          result.output = output[current.length-1];
          if (arc.isFinal()) {
            // target is exact match
            if (fst.hasArcs(arc.target)) {
              // target is also a proper prefix of other terms
              lastFinal = true;
              appendFinalOutput(arc.nextFinalOutput);
            }
          } else {
            // target is not a match but is a prefix of
            // other terms
            current.length--;
            push();
          }
          return result;
        } else if (!fst.hasArcs(arc.target)) {
          // we only match a prefix of the target
          return next();
        } else {
          targetLabel = target.ints[target.offset+current.length];
          arc = fst.readFirstArc(arc.target, getArc(current.length));
        }
      } else if (arc.label > targetLabel) {
        // we are now past the target
        push();
        return result;
      } else if (arc.isLast()) {
        if (current.length == 0) {
          return null;
        }
        return next();
      } else {
        arc = fst.readNextArc(getArc(current.length));
      }
    }
  }

  public InputOutput<T> current() {
    return result;
  }

  public InputOutput<T> next() throws IOException {
    //System.out.println("  enum.next");

    if (current.length == 0) {
      final T output = fst.getEmptyOutput();
      if (output != null) {
        if (!didEmpty) {
          current.length = 0;
          lastFinal = true;
          result.output = output;
          didEmpty = true;
          return result;
        } else {
          lastFinal = false;
        }
      }
      if (fst.noNodes()) {
        return null;
      }
      fst.readFirstArc(fst.getStartNode(), getArc(0));
      push();
    } else if (lastFinal) {
      lastFinal = false;
      assert current.length > 0;
      // resume pushing
      fst.readFirstArc(arcs[current.length-1].target, getArc(current.length));
      push();
    } else {
      //System.out.println("    pop/push");
      pop();
      if (current.length == 0) {
        // enum done
        return null;
      } else {
        current.length--;
        fst.readNextArc(arcs[current.length]);
        push();
      }
    }

    return result;
  }

  private void grow() {
    final int l = current.length + 1;
    current.grow(l);
    arcs = ArrayUtil.grow(arcs, l);
    output = ArrayUtil.grow(output, l);
  }

  private void appendOutput(T addedOutput) {
    T newOutput;
    if (current.length == 0) {
      newOutput = addedOutput;
    } else if (addedOutput == NO_OUTPUT) {
      output[current.length] = output[current.length-1];
      return;
    } else {
      newOutput = fst.outputs.add(output[current.length-1], addedOutput);
    }
    output[current.length] = newOutput;
  }

  private void appendFinalOutput(T addedOutput) {
    if (current.length == 0) {
      result.output = addedOutput;
    } else {
      result.output = fst.outputs.add(output[current.length-1], addedOutput);
    }
  }

  private void push() throws IOException {

    FST.Arc<T> arc = arcs[current.length];
    assert arc != null;

    while(true) {
      grow();
      
      current.ints[current.length] = arc.label;
      appendOutput(arc.output);
      //System.out.println("    push: append label=" + ((char) arc.label) + " output=" + fst.outputs.outputToString(arc.output));
      current.length++;
      grow();

      if (!fst.hasArcs(arc.target)) {
        break;
      }

      if (arc.isFinal()) {
        appendFinalOutput(arc.nextFinalOutput);
        lastFinal = true;
        return;
      }

      arc = fst.readFirstArc(arc.target, getArc(current.length));
    }
    result.output = output[current.length-1];
  }

  private void pop() {
    while (current.length > 0 && arcs[current.length-1].isLast()) {
      current.length--;
    }
  }

  private FST.Arc<T> getArc(int idx) {
    if (arcs[idx] == null) {
      arcs[idx] = new FST.Arc<T>();
    }
    return arcs[idx];
  }
}
