/*
 * Copyright (c) 2017 Okra Scheduler
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package okra;

import okra.async.model.DefaultOkraItem;
import okra.base.async.callback.OkraItemScheduleCallback;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduleTest extends OkraBaseContainerTest {

    private CountDownLatch lock = new CountDownLatch(1);

    @Test
    public void scheduleTest() throws InterruptedException {
        final DefaultOkraItem item = new DefaultOkraItem();
        item.setRunDate(LocalDateTime.now().minusMinutes(5));

        final boolean[] result = {false};
        final Throwable[] resultError = {null};

        getDefaultOkra().schedule(item, new OkraItemScheduleCallback() {

            @Override
            public void onSuccess() {
                result[0] = true;
            }

            @Override
            public void onFailure(final Throwable throwable) {
                resultError[0] = throwable;
            }
        });

        lock.await(5, TimeUnit.SECONDS);

        assertThat(result[0]).isTrue();
        assertThat(resultError[0]).isNull();
    }
}