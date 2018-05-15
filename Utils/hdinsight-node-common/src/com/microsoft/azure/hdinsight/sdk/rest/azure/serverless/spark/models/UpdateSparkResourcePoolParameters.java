/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 *
 * Code generated by Microsoft (R) AutoRest Code Generator.
 */

package com.microsoft.azure.hdinsight.sdk.rest.azure.serverless.spark.models;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Update spark workers.
 */
public class UpdateSparkResourcePoolParameters {
    /**
     * Definition of spark workers.
     */
    @JsonProperty(value = "sparkResourceCollection")
    private List<UpdateSparkResourcePoolItemParameters> sparkResourceCollection;

    /**
     * Get the sparkResourceCollection value.
     *
     * @return the sparkResourceCollection value
     */
    public List<UpdateSparkResourcePoolItemParameters> sparkResourceCollection() {
        return this.sparkResourceCollection;
    }

    /**
     * Set the sparkResourceCollection value.
     *
     * @param sparkResourceCollection the sparkResourceCollection value to set
     * @return the UpdateSparkResourcePoolParameters object itself.
     */
    public UpdateSparkResourcePoolParameters withSparkResourceCollection(List<UpdateSparkResourcePoolItemParameters> sparkResourceCollection) {
        this.sparkResourceCollection = sparkResourceCollection;
        return this;
    }

}
