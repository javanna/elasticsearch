/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class DeadHostStateTests extends RestClientTestCase {

    public void testInitialDeadHostState() {
        DeadHostState deadHostState = new DeadHostState();
        assertThat(deadHostState.getDeadUntilNanos(), greaterThan(System.nanoTime()));
        assertThat(deadHostState.getFailedAttempts(), equalTo(1));
    }

    public void testDeadHostStateFromPrevious() {
        DeadHostState previous = new DeadHostState();
        int iters = randomIntBetween(5, 20);
        for (int i = 0; i < iters; i++) {
            DeadHostState deadHostState = new DeadHostState(previous);
            assertThat(deadHostState.getDeadUntilNanos(), greaterThan(previous.getDeadUntilNanos()));
            assertThat(deadHostState.getFailedAttempts(), equalTo(previous.getFailedAttempts() + 1));
            previous = deadHostState;
        }
    }

    public void testDeadHostStateTimeouts() {
        DeadHostState initialDeadHostState = new DeadHostState(TEST_TIME_SUPPLIER);
        assertThat(initialDeadHostState.getDeadUntilNanos(), equalTo(TimeUnit.MINUTES.toNanos(1)));
        long[] expectedTimeoutsSeconds = new long[]{84, 120, 169, 240, 339, 480, 678, 960, 1357, 1800};
        DeadHostState previous = initialDeadHostState;
        for (long expectedTimeoutsSecond : expectedTimeoutsSeconds) {
            DeadHostState deadHostState = new DeadHostState(previous, TEST_TIME_SUPPLIER);
            assertThat(TimeUnit.NANOSECONDS.toSeconds(deadHostState.getDeadUntilNanos()), equalTo(expectedTimeoutsSecond));
            previous = deadHostState;
        }
        //check that from here on the timeout does not increase
        int iters = randomIntBetween(1, 50);
        for (int i = 0; i < iters; i++) {
            DeadHostState deadHostState = new DeadHostState(previous, TEST_TIME_SUPPLIER);
            assertThat(TimeUnit.NANOSECONDS.toSeconds(deadHostState.getDeadUntilNanos()),
                    equalTo(expectedTimeoutsSeconds[expectedTimeoutsSeconds.length - 1]));
            previous = deadHostState;
        }
    }

    private static final DeadHostState.TimeSupplier TEST_TIME_SUPPLIER = new DeadHostState.TimeSupplier() {
        @Override
        long getNanoTime() {
            return 0;
        }
    };

    public void testCompareTo() {
        int numObjects = randomIntBetween(5, 20);
        DeadHostState[] deadHostStates = new DeadHostState[numObjects];
        int failedAttempts = randomIntBetween(1, 5);
        for (int i = 0; i < failedAttempts; i++) {
            for (int j = 0; j < numObjects; j++) {
                if (i == 0) {
                    deadHostStates[j] = new DeadHostState();
                } else {
                    deadHostStates[j] = new DeadHostState(deadHostStates[j]);
                }
            }
            for (int k = 1; k < deadHostStates.length; k++) {
                assertThat(deadHostStates[k - 1].getDeadUntilNanos(), lessThan(deadHostStates[k].getDeadUntilNanos()));
                assertThat(deadHostStates[k - 1], lessThan(deadHostStates[k]));
            }
        }
    }
}
