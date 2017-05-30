package okra;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.result.DeleteResult;
import okra.base.async.AbstractOkraAsync;
import okra.base.async.OkraAsync;
import okra.base.async.callback.*;
import okra.base.model.OkraItem;
import okra.base.model.OkraStatus;
import okra.index.IndexCreator;
import okra.utils.DateUtils;
import okra.utils.QueryUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class OkraCore<T extends OkraItem> extends AbstractOkraAsync<T> implements OkraAsync<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OkraCore.class);

    private MongoClient mongoClient;

    private final Class<T> scheduleItemClass;
    private final long defaultHeartbeatExpirationMillis;

    public OkraCore(MongoClient mongo, String database, String collection,
                    Class<T> scheduleItemClass, long defaultHeartbeatExpirationMillis) {
        super(database, collection);
        this.mongoClient = mongo;
        this.scheduleItemClass = scheduleItemClass;
        this.defaultHeartbeatExpirationMillis = defaultHeartbeatExpirationMillis;
    }

    public void peek(final OkraItemCallback<T> callback) {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);

        Document update = new Document();
        update.put("heartbeat", new Date());
        update.put("status", OkraStatus.PROCESSING.name());

        SingleResultCallback<Document> mongoCallback = (document, throwable) -> {
            if (throwable == null) {
                callback.onResult(documentToOkraItem(document), null);
            } else {
                callback.onResult(null, throwable);
            }
        };

        mongoClient
                .getDatabase(getDatabase())
                .getCollection(getCollection())
                .findOneAndUpdate(QueryUtils.generatePeekQuery(defaultHeartbeatExpirationMillis), update, options, mongoCallback);
    }

    public void poll(OkraItemCallback<T> callback) {

        OkraItemCallback<T> pollCallback = (item, t) -> {
            if (t == null) {
                delete(item, (item1, t1) -> {
                });
                callback.onResult(item, null);
            } else {
                callback.onResult(null, t);
            }
        };

        peek(pollCallback);
    }

    @Override
    public void setup() {
        IndexCreator.ensureIndexes(this, mongoClient, getDatabase(), getCollection());
    }

    @Override
    public void delete(T item, OkraItemDeleteCallback callback) {
        SingleResultCallback<DeleteResult> mongoCallback = (result, t) -> {
            if (t == null) {
                if (result.getDeletedCount() > 0) {
                    callback.onResult(true, null);
                } else {
                    callback.onResult(false, null);
                }
            } else {
                callback.onResult(false, t);
            }
        };
        mongoClient.getDatabase(getDatabase()).getCollection(getCollection()).deleteOne(new Document("_id", new ObjectId(item.getId())), mongoCallback);
    }

    @Override
    public void reschedule(T item, OkraItemOperationCallback<T> callback) {

    }

    @Override
    public void heartbeat(T item, OkraItemOperationCallback<T> callback) {
        Document query = new Document();
        query.put("_id", new ObjectId(item.getId()));
        query.put("status", OkraStatus.PROCESSING.name());
        query.put("heartbeat", DateUtils.localDateTimeToDate(item.getHeartbeat()));

        Document update = new Document();
        update.put("$set", new Document("heartbeat", new Date()));

        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();
        options.returnDocument(ReturnDocument.AFTER);

        SingleResultCallback<Document> mongoCallback = (result, t) -> {
            if (t == null) {
                if (result != null) {
                    T retrievedItem = documentToOkraItem(result);
                    callback.onResult(retrievedItem, null);
                } else {
                    callback.onResult(null, null);
                }
            } else {
                callback.onResult(null, t);
            }
        };

        mongoClient
                .getDatabase(getDatabase())
                .getCollection(getCollection())
                .findOneAndUpdate(query, update, options, mongoCallback);
    }

    @Override
    public void schedule(T item, OkraItemScheduleCallback callback) {
        Document doc = new Document();
        doc.append("status", OkraStatus.PENDING.name());
        doc.append("runDate", DateUtils.localDateTimeToDate(item.getRunDate()));

        SingleResultCallback<Void> mongoCallback = (result, t) -> {
            if (t == null) {
                callback.onResult(true, null);
            } else {
                callback.onResult(false, t);
            }
        };

        mongoClient.getDatabase(getDatabase()).getCollection(getCollection()).insertOne(doc, mongoCallback);
    }

    @Override
    public void countByStatus(OkraStatus status, OkraCountCallback callback) {
        Document doc = new Document("status", status.name());

        SingleResultCallback<Long> mongoCallback = (result, t) -> {
            if (t == null) {
                callback.onResult(result, null);
            } else {
                callback.onResult(null, t);
            }
        };

        mongoClient.getDatabase(getDatabase()).getCollection(getCollection()).count(doc, mongoCallback);
    }

    @Override
    public void countDelayed(OkraCountCallback callback) {
        Document doc = new Document(
                "runDate", new Document("$lt", DateUtils.localDateTimeToDate(LocalDateTime.now())));

        SingleResultCallback<Long> mongoCallback = (result, t) -> {
            if (t == null) {
                callback.onResult(result, null);
            } else {
                callback.onResult(null, t);
            }
        };

        mongoClient.getDatabase(getDatabase()).getCollection(getCollection()).count(doc, mongoCallback);
    }

    private T documentToOkraItem(Document result) {
        T okraItem = null;
        try {
            okraItem = scheduleItemClass.newInstance();
            ObjectId objId = result.getObjectId("_id");
            okraItem.setId(objId.toString());

            Date heartBeat = result.getDate("heartbeat");
            if (heartBeat != null) {
                okraItem.setHeartbeat(dateToLocalDateTime(heartBeat));
            }

            Date runDate = result.getDate("runDate");
            okraItem.setRunDate(dateToLocalDateTime(runDate));

            String status = result.getString("status");
            okraItem.setStatus(OkraStatus.valueOf(status));
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.error("Error initializing Okra Item instance", e);
        }
        return okraItem;
    }

    private LocalDateTime dateToLocalDateTime(Date heartBeat) {
        return LocalDateTime.ofInstant(heartBeat.toInstant(), ZoneId.systemDefault());
    }
}
