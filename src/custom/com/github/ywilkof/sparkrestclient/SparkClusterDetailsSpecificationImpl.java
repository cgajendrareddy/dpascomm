package com.github.ywilkof.sparkrestclient;


import com.github.ywilkof.sparkrestclient.interfaces.SparkClusterDetailsSpecification;
import org.apache.http.client.methods.HttpGet;

public class SparkClusterDetailsSpecificationImpl implements SparkClusterDetailsSpecification {

    private SparkRestClient sparkRestClient;

    public SparkClusterDetailsSpecificationImpl(SparkRestClient sparkRestClient) {
        this.sparkRestClient = sparkRestClient;
    }

    @Override
    public SparkClusterDetailsResponse getSparkClusterDetails() throws FailedSparkRequestException {
        String url = this.sparkRestClient.getHttpScheme() + "://" + this.sparkRestClient.getMasterUrl() + "/json/" ;
        SparkClusterDetailsResponse response = (SparkClusterDetailsResponse)HttpRequestUtil.executeHttpMethodAndGetResponse(this.sparkRestClient.getClient(), new HttpGet(url), SparkClusterDetailsResponse.class);
        
        if (response == null) {
            throw new FailedSparkRequestException("get Cluster details was not successful."+ response);
        } else {
            return response;
        }
    }
}
