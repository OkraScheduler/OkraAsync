package okra;

import okra.async.model.DefaultOkraItem;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduleTest extends OkraBaseContainerTest {

    private CountDownLatch lock = new CountDownLatch(1);

    @Test
    public void scheduleTest() throws InterruptedException {
        DefaultOkraItem item = new DefaultOkraItem();
        item.setRunDate(LocalDateTime.now().minusMinutes(5));


        final boolean[] result = {false};
        final Throwable[] resultError = {null};

        getDefaultOkra().schedule(item, (r, throwable) -> {
            result[0] = r;
            resultError[0] = throwable;

        });

        lock.await(5, TimeUnit.SECONDS);

        assertThat(result[0]).isTrue();
        assertThat(resultError[0]).isNull();
    }

}
