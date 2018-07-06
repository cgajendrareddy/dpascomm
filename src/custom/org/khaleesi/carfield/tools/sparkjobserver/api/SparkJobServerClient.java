package org.khaleesi.carfield.tools.sparkjobserver.api;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;

public class SparkJobServerClient extends SparkJobServerClientImpl {
    String jobServerUrl;
    public SparkJobServerClient(String jobServerUrl) {
        super(jobServerUrl);
        this.jobServerUrl = jobServerUrl;
    }

    public boolean killJob(String jobId) throws SparkJobServerClientException {
        ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(this.jobServerUrl);
        final CloseableHttpClient httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter("http.connection.timeout", new Integer(5000));
        try {
            //TODO add a check for the validation of contextName naming
            if (!isNotEmpty(jobId)) {
                throw new SparkJobServerClientException("The given JobId is null or empty.");
            }
            StringBuffer postUrlBuff = new StringBuffer(this.jobServerUrl);
            postUrlBuff.append("jobs/").append(jobId);

            HttpDelete deleteMethod = new HttpDelete(postUrlBuff.toString());
            HttpResponse response = httpClient.execute(deleteMethod);
            int statusCode = response.getStatusLine().getStatusCode();
            String resContent = getResponseContent(response.getEntity());
            if (statusCode == HttpStatus.SC_OK) {
                return true;
            } else {
                throw new SparkJobServerClientException("status code : "+statusCode+" response : "+resContent);
            }
        } catch (Exception e) {
            processException("Error occurs when trying to delete the target job:", e);
        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new SparkJobServerClientException("Client Connection exception");
            }
        }
        return false;
    }
}
