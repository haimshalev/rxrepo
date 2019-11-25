package com.slimgears.rxrepo.sql;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeTokens;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.*;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.slimgears.util.generic.LazyString.lazy;

public class SqlQueryProvider implements QueryProvider {
    private final static Logger log = LoggerFactory.getLogger(SqlQueryProvider.class);
    private final static String aggregationField = "__aggregation";
    private final SqlStatementProvider statementProvider;
    private final SqlStatementExecutor statementExecutor;
    private final SchemaProvider schemaProvider;
    private final ReferenceResolver referenceResolver;

    SqlQueryProvider(SqlStatementProvider statementProvider,
                     SqlStatementExecutor statementExecutor,
                     SchemaProvider schemaProvider,
                     ReferenceResolver referenceResolver) {
        this.statementProvider = statementProvider;
        this.statementExecutor = statementExecutor;
        this.schemaProvider = schemaProvider;
        this.referenceResolver = referenceResolver;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return Optional
                .of(entities)
                .filter(e -> !Iterables.isEmpty(e))
                .map(meta -> schemaProvider.createOrUpdate(metaClass)
                        .doOnSubscribe(d -> log.debug("Beginning creating class {}", lazy(metaClass::simpleName)))
                        .doOnComplete(() -> log.debug("Finished creating class {}", lazy(metaClass::simpleName)))
                        .andThen(Observable.fromIterable(entities)
                                .flatMapSingle(e -> insert(metaClass, e))
                                .ignoreElements()))
                .orElseGet(Completable::complete);
    }

    @Override
    public <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity) {
        return insertOrUpdate(metaClass, PropertyResolver.fromObject(metaClass, entity));
    }

    @Override
    public <K, S> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        SqlStatement statement = statementProvider.forQuery(QueryInfo
                .<K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(key))
                .limit(1L)
                .build());

        return schemaProvider.createOrUpdate(metaClass)
            .andThen(statementExecutor
                .executeQuery(statement)
                .firstElement()
                .flatMap((PropertyResolver pr) -> {
                    S oldObj = pr.toObject(metaClass);
                    return entityUpdater
                            .apply(Maybe.just(oldObj))
                            .map(newObj -> pr.mergeWith(PropertyResolver.fromObject(metaClass, newObj)))
                            .filter(newPr -> !pr.equals(newPr))
                            .flatMap(newPr -> update(metaClass, newPr).toMaybe());
                })
                .switchIfEmpty(Maybe.defer(() -> entityUpdater
                        .apply(Maybe.empty())
                        .flatMap(e -> insert(metaClass, e).toMaybe()))));
    }

    private <K, S> Single<S> update(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver) {
        SqlStatement statement = statementProvider.forUpdate(metaClass, propertyResolver, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    private <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver) {
        SqlStatement statement = statementProvider.forInsertOrUpdate(metaClass, propertyResolver, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    private <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, SqlStatement statement) {
        return schemaProvider.createOrUpdate(metaClass)
                .doOnSubscribe(d -> log.trace("Ensuring class {}", metaClass.simpleName()))
                .doOnError(e -> log.trace("Error when updating class: {}", metaClass.simpleName(), e))
                .doOnComplete(() -> log.trace("Class updated {}", metaClass.simpleName()))
                .andThen(statementExecutor
                        .executeCommandReturnEntries(statement)
                        .map(pr -> pr.toObject(metaClass))
                        .doOnSubscribe(d -> log.trace("Executing statement: {}", statement.statement()))
                        .doOnError(e -> log.trace("Failed to execute statement: {}", statement.statement(), e))
                        .doOnComplete(() -> log.trace("Execution complete: {}", statement.statement()))
                        .doOnNext(obj -> log.trace("Updated {}", obj))
                        .take(1)
                        .singleOrError());
    }

    private <K, S> Single<S> insert(MetaClassWithKey<K, S> metaClass, S entity) {
        SqlStatement statement = statementProvider.forInsert(metaClass, entity, referenceResolver);
        return insertOrUpdate(metaClass, statement);
    }

    @Override
    public <K, S, T> Observable<T> query(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaProvider
                .createOrUpdate(query.metaClass())
                .andThen(statementExecutor
                        .executeQuery(statementProvider.forQuery(query))
                        .compose(toObjects(objectType, query.mapping())));
    }

    @SuppressWarnings("unchecked")
    private <T> ObservableTransformer<PropertyResolver, T> toObjects(TypeToken<? extends T> objectType, ObjectExpression<?, T> mapping) {
        Function<PropertyResolver, Maybe<T>> mapper = Optional
                .ofNullable(mapping)
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .map(PropertyExpression::path)
                .<Function<PropertyResolver, Maybe<T>>>map(path -> pr -> Optional
                        .ofNullable(pr.getProperty(path, TypeTokens.asClass(objectType)))
                        .map(obj -> obj instanceof PropertyResolver
                                ? ((PropertyResolver) obj).toObject(objectType)
                                : (T)obj)
                        .map(Maybe::just)
                        .orElseGet(Maybe::empty))
                .orElse(pr -> Maybe.fromCallable(() -> pr.toObject(objectType)));
        return src -> src.flatMapMaybe(mapper);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        TypeToken<? extends T> objectType = HasMapping.objectType(query);
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeLiveQuery(statementProvider.forQuery(query))
                .map(notification -> notification.map(pr -> pr.toObject(objectType))));
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        TypeToken<T> elementType = HasMapping.objectType(query);
        ObjectExpression<T, R> aggregation = aggregator.apply(CollectionExpression.indirectArg(MoreTypeTokens.collection(elementType)));
        TypeToken<R> resultType = aggregation.reflect().objectType();
        return schemaProvider.createOrUpdate(query.metaClass()).andThen(statementExecutor
                .executeQuery(statementProvider.forAggregation(query, aggregation, aggregationField))
                .map(pr -> {
                    Object obj = pr.getProperty(aggregationField, TypeTokens.asClass(resultType));
                    //noinspection unchecked
                    return (obj instanceof PropertyResolver)
                            ? ((PropertyResolver)obj).toObject(resultType)
                            : (R)obj;
                })
                .firstElement());
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return schemaProvider
                .createOrUpdate(update.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forUpdate(update)));
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> deleteInfo) {
        return schemaProvider
                .createOrUpdate(deleteInfo.metaClass())
                .andThen(statementExecutor.executeCommandReturnCount(statementProvider.forDelete(deleteInfo)));
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDrop(metaClass);
            return statementExecutor.executeCommand(statement);
        });
    }

    @Override
    public Completable dropAll() {
        return Completable.defer(() -> {
            SqlStatement statement = statementProvider.forDrop();
            return statementExecutor.executeCommand(statement);
        });
    }

    private static <S extends HasMetaClass<S>> boolean isEmptyObject(S object) {
        AtomicReference<PropertyMeta<S, ?>> nonNullProperty = new AtomicReference<>();
        if (Streams.fromIterable(object.metaClass().properties())
                .filter(p -> p.getValue(object) != null)
                .peek(nonNullProperty::set)
                .limit(2)
                .count() > 1) {
            return false;
        }

        return Optional.ofNullable(nonNullProperty.get())
                .map(PropertyMetas::isKey)
                .orElse(true);
    }
}
