package com.keedio.storm.metric;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.apache.log4j.Logger;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Storm metric consumer publishing numeric consumed metrics via JMX.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 17/2/15.
 */
public class JMXMetricConsumer implements IMetricsConsumer {
    private Logger logger = Logger.getLogger("JMXMetricConsumer");

    public Map<JMXTaskInfo, NumericValue> metrics;
    private MBeanServer mbs;

    /**
     * Public constructor.
     */
    public JMXMetricConsumer() {
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Register the given bean with the platform management factory.
     *
     * @param beanName the name of the bean to be published.
     * @param bean the bean instance to be published.
     */
    private void registerMBean(String beanName, NumericValue bean) {
        try {
            ObjectName metric = new ObjectName("com.keedio.storm.metric:name="+beanName);

            final StandardMBean mBean = new StandardMBean(bean, NumericValueMBean.class);

            mbs.registerMBean(mBean, metric);
        } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            logger.error(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
        metrics = new HashMap<>();
    }

    /**
     * Handles the given data points. For each data point:<br/>
     * <ul>
     *     <li>If the datapoint is not numeric discards it.</li>
     *     <li>If the datapoint has never seen before constructs a new {@see NumericValue} wrapper class and publish it via JMX</li>
     *     <li>If the datapoint is not new updates the content of the wrapper NumericValue published via JMX</li>
     * </ul>
     *
     * @param taskInfo the task info object related to the data points to be handled.
     * @param dataPoints the data points provided by Storm.
     */
    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        for (DataPoint point : dataPoints) {
            JMXTaskInfo key = new JMXTaskInfo(taskInfo, point.name);

            if (!Number.class.isAssignableFrom(point.value.getClass())){
                continue;
            }

            if (!metrics.containsKey(key)) {
                NumericValue value = new NumericValue((Number)point.value);
                registerMBean(key.toString(), value);
                metrics.put(key, value);
            } else {
                metrics.get(key).setNumericValue((Number)point.value);
            }

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup() {
        metrics.clear();
    }
}
