package com.bluebreezecf.tools.sparkjobserver.api;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientException;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class SparkJobServerClientDeleteJobImpl {
    private static Logger logger = Logger.getLogger(SparkJobServerClientDeleteJobImpl.class);
    private static final int BUFFER_SIZE = 512 * 1024;
    private String jobServerUrl;

    public SparkJobServerClientDeleteJobImpl(String jobServerUrl) {
        if (!jobServerUrl.endsWith("/")) {
            jobServerUrl = jobServerUrl + "/";
        }
        this.jobServerUrl = jobServerUrl;
    }

    public boolean killJob(String jobId) throws SparkJobServerClientException {
        final CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        try {
            //TODO add a check for the validation of contextName naming
            if (!isNotEmpty(jobId)) {
                throw new SparkJobServerClientException("The given JobId is null or empty.");
            }
            StringBuffer postUrlBuff = new StringBuffer(jobServerUrl);
            postUrlBuff.append("jobs/").append(jobId);

            HttpDelete deleteMethod = new HttpDelete(postUrlBuff.toString());
            HttpResponse response = httpClient.execute(deleteMethod);
            int statusCode = response.getStatusLine().getStatusCode();
            String resContent = getResponseContent(response.getEntity());
            if (statusCode == HttpStatus.SC_OK) {
                return true;
            } else {
                logError(statusCode, resContent, false);
            }
        } catch (Exception e) {
            processException("Error occurs when trying to delete the target job:", e);
        } finally {
            close(httpClient);
        }
        return false;
    }

    /**
     * Gets the contents of the http response from the given <code>HttpEntity</code>
     * instance.
     *
     * @param entity the <code>HttpEntity</code> instance holding the http response content
     * @return the corresponding response content
     */
    protected String getResponseContent(HttpEntity entity) {
        byte[] buff = new byte[BUFFER_SIZE];
        StringBuffer contents = new StringBuffer();
        InputStream in = null;
        try {
            in = entity.getContent();
            BufferedInputStream bis = new BufferedInputStream(in);
            int readBytes = 0;
            while ((readBytes = bis.read(buff)) != -1) {
                contents.append(new String(buff, 0, readBytes));
            }
        } catch (Exception e) {
            logger.error("Error occurs when trying to reading response", e);
        } finally {
            closeStream(in);
        }
        return contents.toString().trim();
    }


    /**
     * Closes the given stream.
     *
     * @param stream the input/output stream to be closed
     */
    protected void closeStream(Closeable stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException ioe) {
                logger.error("Error occurs when trying to close the stream:", ioe);
            }
        } else {
            logger.error("The given stream is null");
        }
    }

    /**
     * Handles the given exception with specific error message, and
     * generates a corresponding <code>SparkJobServerClientException</code>.
     *
     * @param errorMsg the corresponding error message
     * @param e the exception to be handled
     * @throws SparkJobServerClientException the corresponding transformed
     *        <code>SparkJobServerClientException</code> instance
     */
    protected void processException(String errorMsg, Exception e) throws SparkJobServerClientException {
        if (e instanceof SparkJobServerClientException) {
            throw (SparkJobServerClientException)e;
        }
        logger.error(errorMsg, e);
        throw new SparkJobServerClientException(errorMsg, e);
    }

    /**
     * Judges the given string value is not empty or not.
     *
     * @param value the string value to be checked
     * @return true indicates it is not empty, false otherwise
     */
    protected boolean isNotEmpty(String value) {
        return value != null && !value.isEmpty();
    }

    /**
     * Logs the response information when the status is not 200 OK,
     * and throws an instance of <code>SparkJobServerClientException<code>.
     *
     * @param errorStatusCode error status code
     * @param msg the message to indicates the status, it can be null
     * @param throwable true indicates throws an instance of <code>SparkJobServerClientException</code>
     *       with corresponding error message, false means only log the error message.
     * @throws SparkJobServerClientException containing the corresponding error message
     */
    private void logError(int errorStatusCode, String msg, boolean throwable) throws SparkJobServerClientException {
        StringBuffer msgBuff = new StringBuffer("Spark Job Server ");
        msgBuff.append(jobServerUrl).append(" response ").append(errorStatusCode);
        if (null != msg) {
            msgBuff.append(" ").append(msg);
        }
        String errorMsg = msgBuff.toString();
        logger.error(errorMsg);
        if (throwable) {
            throw new SparkJobServerClientException(errorMsg);
        }
    }

    private void close(final CloseableHttpClient client) {
        try {
            client.close();
        } catch (final IOException e) {
            logger.error("could not close client" , e);
        }
    }
}
