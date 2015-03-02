package com.keedio.storm.metric;

import backtype.storm.metric.api.IReducer;

/**
 * Helper class holding the reducer state.
 */
class ThroughputReducerState {
    public double last_update_time = 0;

}

/**
 * Storm IReducer used to compute the topology throughput.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 17/2/15.
 */
public class ThroughputReducer implements IReducer<ThroughputReducerState> {
    public long counter = 0;
    public long start_time = System.currentTimeMillis();

    @Override
    public ThroughputReducerState init() {
        return new ThroughputReducerState();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ThroughputReducerState reduce(ThroughputReducerState acc, Object input) {
        counter++;
        acc.last_update_time = ((Number)input).doubleValue();
        return acc;
    }

    /**
     * Returns the throughput computed as:<br/>
     * total counted tuples received / (last received timestamp - initial timestamp)
     *
     * @param acc the accumulator.
     * @return Returns the computed throughput.
     */
    @Override
    public Object extractResult(ThroughputReducerState acc) {
        if (acc.last_update_time > start_time) {
            return new Double(counter / ((acc.last_update_time - start_time) / 1000));
        }

        return 0;
    }
}
