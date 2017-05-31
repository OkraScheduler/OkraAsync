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

import com.mongodb.async.client.MongoClient;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import okra.base.async.AbstractOkraAsync;
import okra.base.async.OkraAsync;
import okra.base.async.callback.*;
import okra.base.model.OkraItem;
import okra.base.model.OkraStatus;
import okra.exception.OkraRuntimeException;
import okra.index.IndexCreator;
import okra.serialization.DocumentSerializer;
import okra.util.DateUtil;
import okra.util.QueryUtil;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.time.LocalDateTime;
import java.util.Date;

public class OkraAsyncImpl<T extends OkraItem> extends AbstractOkraAsync<T> implements OkraAsync<T> {

    private final Class<T> itemClass;
    private final long defaultHeartbeatExpirationMillis;

    private final MongoClient client;
    private DocumentSerializer serializer;

    public OkraAsyncImpl(final MongoClient mongo, final String database,
                         final String collection, final Class<T> itemClass,
                         final long defaultHeartbeatExpirationMillis) {
        super(database, collection);
        this.client = mongo;
        this.itemClass = itemClass;
        this.defaultHeartbeatExpirationMillis = defaultHeartbeatExpirationMillis;
        this.serializer = new DocumentSerializer();
        setup();
    }

    @Override
    public void setup() {
        super.setup();
        IndexCreator.ensureIndexes(this, client, getDatabase(), getCollection());
    }

    @Override
    public void peek(final OkraItemCallback<T> callback) {
        final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);

        final Document update = new Document();
        update.put("heartbeat", new Date());
        update.put("status", OkraStatus.PROCESSING.name());

        client.getDatabase(getDatabase())
                .getCollection(getCollection())
                .findOneAndUpdate(QueryUtil.generatePeekQuery(defaultHeartbeatExpirationMillis), update, options,
                        (document, throwable) -> {
                            if (throwable == null) {
                                callback.onSuccess(serializer.fromDocument(itemClass, document));
                            } else {
                                callback.onFailure(throwable);
                            }
                        });
    }

    @Override
    public void poll(final OkraItemCallback<T> callback) {
        peek(new OkraItemCallback<T>() {

            @Override
            public void onSuccess(final T item) {
                delete(item, new OkraItemDeleteCallback() {

                    @Override
                    public void onSuccess(final long deletedCount) {
                        if (deletedCount > 0) {
                            callback.onSuccess(item);
                        }
                    }

                    @Override
                    public void onFailure(final Throwable throwable) {
                        callback.onFailure(throwable);
                    }
                });
            }

            @Override
            public void onFailure(final Throwable throwable) {
                callback.onFailure(throwable);
            }
        });
    }

    @Override
    public void delete(final T item, final OkraItemDeleteCallback callback) {
        client.getDatabase(getDatabase())
                .getCollection(getCollection())
                .deleteOne(new Document("_id", new ObjectId(item.getId())), (result, throwable) -> {
                    if (throwable == null) {
                        callback.onSuccess(result.getDeletedCount());
                    } else {
                        callback.onFailure(throwable);
                    }
                });
    }

    @Override
    public void reschedule(final T item, final OkraItemOperationCallback<T> callback) {
        throw new OkraRuntimeException("Method not implemented yet");
    }

    @Override
    public void heartbeat(final T item, final OkraItemOperationCallback<T> callback) {
        final Document query = serializer.toDocument(item);

        final Document update = new Document();
        update.put("$set", new Document("heartbeat", new Date()));

        final FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);

        client.getDatabase(getDatabase())
                .getCollection(getCollection())
                .findOneAndUpdate(query, update, options, (document, throwable) -> {
                    if (throwable == null) {
                        callback.onSuccess(serializer.fromDocument(itemClass, document));
                    } else {
                        callback.onFailure(throwable);
                    }
                });
    }

    @Override
    public void schedule(final T item, final OkraItemScheduleCallback callback) {
        final Document document = serializer.toDocument(item);

        client.getDatabase(getDatabase())
                .getCollection(getCollection())
                .insertOne(document, (result, throwable) -> {
                    if (throwable == null) {
                        callback.onSuccess();
                    } else {
                        callback.onFailure(throwable);
                    }
                });
    }

    @Override
    public void countByStatus(final OkraStatus status, final OkraCountCallback callback) {
        final Document document = new Document("status", status.name());

        client.getDatabase(getDatabase())
                .getCollection(getCollection())
                .count(document, (result, throwable) -> {
                    if (throwable == null) {
                        callback.onSuccess(result);
                    } else {
                        callback.onFailure(throwable);
                    }
                });
    }

    @Override
    public void countDelayed(final OkraCountCallback callback) {
        final Document document = new Document(
                "runDate",
                new Document("$lt", DateUtil.toDate(LocalDateTime.now()))
        );

        client.getDatabase(getDatabase())
                .getCollection(getCollection())
                .count(document, (result, throwable) -> {
                    if (throwable == null) {
                        callback.onSuccess(result);
                    } else {
                        callback.onFailure(throwable);
                    }
                });
    }
}