package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.search.sampling.Sampler;
import org.apache.lucene.facet.search.sampling.Sampler.SampleResult;

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

/**
 * Wrap any Facets Accumulator with sampling.
 * <p>
 * Note: Sampling accumulation (Accumulation over a sampled-set of the results),
 * does not guarantee accurate values for
 * {@link FacetResult#getNumValidDescendants()} &
 * {@link FacetResultNode#getResidue()}.
 * 
 * @lucene.experimental
 */
public class SamplingWrapper extends FacetsAccumulator {

  private FacetsAccumulator delegee;
  private Sampler sampler;

  public SamplingWrapper(FacetsAccumulator delegee, Sampler sampler) {
    super(delegee.searchParams, delegee.indexReader, delegee.taxonomyReader);
    this.delegee = delegee;
    this.sampler = sampler;
  }

  @Override
  public List<FacetResult> accumulate(ScoredDocIDs docids) throws IOException {
    // first let delegee accumulate without labeling at all (though
    // currently it doesn't matter because we have to label all returned anyhow)
    boolean origAllowLabeling = isAllowLabeling();
    setAllowLabeling(false);

    // Replacing the original searchParams with the over-sampled (and without statistics-compute)
    FacetSearchParams original = delegee.searchParams;
    delegee.searchParams = sampler.overSampledSearchParams(original);
    
    SampleResult sampleSet = sampler.getSampleSet(docids);

    List<FacetResult> sampleRes = delegee.accumulate(sampleSet.docids);
    setAllowLabeling(origAllowLabeling);

    List<FacetResult> fixedRes = new ArrayList<FacetResult>();
    for (FacetResult fres : sampleRes) {
      // for sure fres is not null because this is guaranteed by the delegee.
      FacetResultsHandler frh = fres.getFacetRequest().createFacetResultsHandler(taxonomyReader);
      // fix the result of current request
      sampler.getSampleFixer(indexReader, taxonomyReader, searchParams)
          .fixResult(docids, fres); 
      fres = frh.rearrangeFacetResult(fres); // let delegee's handler do any
      
      // Using the sampler to trim the extra (over-sampled) results
      fres = sampler.trimResult(fres);
      
      // final labeling if allowed (because labeling is a costly operation)
      if (isAllowLabeling()) {
        frh.labelResult(fres);
      }
      fixedRes.add(fres); // add to final results
    }

    delegee.searchParams = original; // Back to original params
    
    return fixedRes; 
  }

  /**
   * @see FacetsAccumulator#getComplementThreshold()
   */
  @Override
  public double getComplementThreshold() {
    return delegee.getComplementThreshold();
  }

  /**
   * @param complementThreshold
   * @see FacetsAccumulator#setComplementThreshold(double)
   */
  @Override
  public void setComplementThreshold(double complementThreshold) {
    delegee.setComplementThreshold(complementThreshold);
  }

  @Override
  protected boolean isAllowLabeling() {
    return delegee.isAllowLabeling();
  }

  @Override
  protected void setAllowLabeling(boolean allowLabeling) {
    delegee.setAllowLabeling(allowLabeling);
  }

}
