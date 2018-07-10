package com.github.ywilkof.sparkrestclient.interfaces;

import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.SparkClusterDetailsResponse;

public interface SparkClusterDetailsSpecification {
    /**
     * Get Spark Cluster Details Response
     * @return
     * @throws FailedSparkRequestException
     */
    SparkClusterDetailsResponse getSparkClusterDetails() throws FailedSparkRequestException;
}
