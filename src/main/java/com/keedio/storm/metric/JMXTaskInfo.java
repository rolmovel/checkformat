package com.keedio.storm.metric;

import backtype.storm.metric.api.IMetricsConsumer;

/**
 * Represents a Task to be published via JMX.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 17/2/15.
 */
public class JMXTaskInfo extends IMetricsConsumer.TaskInfo implements Comparable<JMXTaskInfo> {

    public String dataPointName;

    /**
     * Determines equality between two JMXTaskInfo objects, without taking into account
     * the timestamp.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JMXTaskInfo that = (JMXTaskInfo) o;

        if (srcTaskId != that.srcTaskId) return false;
        if (srcWorkerPort != that.srcWorkerPort) return false;
        if (updateIntervalSecs != that.updateIntervalSecs) return false;
        if (dataPointName != null ? !dataPointName.equals(that.dataPointName) : that.dataPointName != null)
            return false;
        if (srcComponentId != null ? !srcComponentId.equals(that.srcComponentId) : that.srcComponentId != null)
            return false;
        if (srcWorkerHost != null ? !srcWorkerHost.equals(that.srcWorkerHost) : that.srcWorkerHost != null)
            return false;

        return true;
    }

    /**
     * Computes the hashCode of a JMXTaskInfo object, without taking into account
     * the timestamp.
     */
    @Override
    public int hashCode() {
        int result = srcWorkerHost != null ? srcWorkerHost.hashCode() : 0;
        result = 31 * result + srcWorkerPort;
        result = 31 * result + (srcComponentId != null ? srcComponentId.hashCode() : 0);
        result = 31 * result + srcTaskId;
        result = 31 * result + updateIntervalSecs;
        result = 31 * result + (dataPointName != null ? dataPointName.hashCode() : 0);
        return result;
    }

    /**
     * Public constructor.
     */
    public JMXTaskInfo(IMetricsConsumer.TaskInfo taskInfo, String dataPointName) {
        this.dataPointName = dataPointName;
        this.srcWorkerHost = taskInfo.srcWorkerHost;
        this.srcWorkerPort = taskInfo.srcWorkerPort;
        this.srcComponentId = taskInfo.srcComponentId;
        this.srcTaskId = taskInfo.srcTaskId;
        this.timestamp = taskInfo.timestamp;
        this.updateIntervalSecs = taskInfo.updateIntervalSecs;
    }

    /**
     * Compares to JMXTaskInfo objects.
     *
     * @param other the JMXTaskInfo object used as comparison.
     * @return 0 if the two objects are equal, 1 otherwise.
     */
    @Override
    public int compareTo(JMXTaskInfo other) {
        return this.equals(other) ? 0 : 1;
    }

    /**
     * Returns the string representation of this task info object, as:
     *  srcComponentId + "." + srcWorkerHost + "." + srcWorkerPort + "." + srcTaskId + "." + dataPointName
     *
     * @return the string representation of this task info object.
     */
    @Override
    public String toString() {
        return srcComponentId + "." + srcWorkerHost + "." + srcWorkerPort + "." + srcTaskId + "." + dataPointName;
    }
}
