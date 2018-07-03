package com.github.ywilkof.sparkrestclient.interfaces;

import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.SparkClusterDetailsResponse;

public interface SparkClusterDetailsSpecification {

    SparkClusterDetailsResponse getSparkClusterDetails() throws FailedSparkRequestException;
}
