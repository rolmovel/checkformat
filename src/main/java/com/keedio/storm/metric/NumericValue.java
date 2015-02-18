package com.keedio.storm.metric;

/**
 * Wrapper class for any numeric value to be published via JMX.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 18/2/15.
 */
public class NumericValue implements NumericValueMBean {
    private Number numericValue;

    /**
     * Default constructor.
     */
    public NumericValue(Number numericValue) {
        this.numericValue = numericValue;
    }

    /**
     * Updates the numeric value wrapped by this object.
     *
     * @param n the new numeric value.
     */
    public void setNumericValue(Number n){
        numericValue = n;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Double getValue() {
        if (numericValue != null){
            return numericValue.doubleValue();
        } else {
            return null;
        }
    }
}
