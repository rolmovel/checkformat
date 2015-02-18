package com.keedio.storm.metric;

/**
 * MBean interface for the numeric value wrapper class.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 17/2/15.
 */
public interface NumericValueMBean {

    /**
     * @return the numeric value to be published via JMX.
     */
    public Double getValue();
}
