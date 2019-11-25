package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.CollectionPropertyExpression;
import com.slimgears.rxrepo.filters.Filter;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.rx.Maybes;
import com.slimgears.util.rx.Observables;
import com.slimgears.util.rx.Singles;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.Single;
import io.reactivex.exceptions.CompositeException;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultEntitySet<K, S> implements EntitySet<K, S> {
    private final static Logger log = LoggerFactory.getLogger(DefaultEntitySet.class);
    private final QueryProvider queryProvider;
    private final MetaClassWithKey<K, S> metaClass;
    private final RepositoryConfigModel config;

    private DefaultEntitySet(QueryProvider queryProvider,
                             MetaClassWithKey<K, S> metaClass,
                             RepositoryConfigModel config) {
        this.queryProvider = queryProvider;
        this.metaClass = metaClass;
        this.config = config;
    }

    static <K, S> DefaultEntitySet<K, S> create(
            QueryProvider queryProvider,
            MetaClassWithKey<K, S> metaClass,
            RepositoryConfigModel config) {
        return new DefaultEntitySet<>(queryProvider, metaClass, config);
    }

    @Override
    public MetaClassWithKey<K, S> metaClass() {
        return metaClass;
    }

    @Override
    public EntityDeleteQuery<S> delete() {
        return new EntityDeleteQuery<S>() {
            private final AtomicReference<ObjectExpression<S, Boolean>> predicate = new AtomicReference<>();
            private final DeleteInfo.Builder<K, S> builder = DeleteInfo.builder();

            @Override
            public Single<Integer> execute() {
                return queryProvider.delete(builder
                        .metaClass(metaClass)
                        .predicate(predicate.get())
                        .build())
                    .compose(Singles.backOffDelayRetry(
                        DefaultEntitySet::isConcurrencyException,
                        Duration.ofMillis(config.retryInitialDurationMillis()),
                        config.retryCount()));
            }

            @Override
            public EntityDeleteQuery<S> where(ObjectExpression<S, Boolean> predicate) {
                updatePredicate(this.predicate, predicate);
                return this;
            }

            @Override
            public EntityDeleteQuery<S> limit(long limit) {
                builder.limit(limit);
                return this;
            }

            @Override
            public EntityDeleteQuery<S> where(Filter<S> filter) {
                return Optional.ofNullable(filter)
                        .flatMap(f -> f.<S>toExpression(metaClass.asType()))
                        .map(this::where)
                        .orElse(this);
            }
        };
    }

    @Override
    public EntityUpdateQuery<S> update() {
        return new EntityUpdateQuery<S>() {
            private final AtomicReference<ObjectExpression<S, Boolean>> predicate = new AtomicReference<>();
            private final UpdateInfo.Builder<K, S> builder = UpdateInfo.<K, S>builder().metaClass(metaClass);

            @Override
            public <T extends HasMetaClass<T>, V> EntityUpdateQuery<S> set(PropertyExpression<S, T, V> property, ObjectExpression<S, V> value) {
                builder.propertyUpdatesBuilder().add(PropertyUpdateInfo.create(property, value));
                return this;
            }

            @Override
            public <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> add(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item) {
                return collectionOperation(property, item, CollectionPropertyUpdateInfo.Operation.Add);
            }

            @Override
            public <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> remove(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item) {
                return collectionOperation(property, item, CollectionPropertyUpdateInfo.Operation.Remove);
            }

            @Override
            public Single<Integer> execute() {
                return Single
                        .defer(() -> queryProvider.update(builder
                                .predicate(predicate.get())
                                .build()))
                        .compose(Singles.backOffDelayRetry(
                                DefaultEntitySet::isConcurrencyException,
                                Duration.ofMillis(config.retryInitialDurationMillis()),
                                config.retryCount()));
            }

            @Override
            public EntityUpdateQuery<S> where(ObjectExpression<S, Boolean> predicate) {
                updatePredicate(this.predicate, predicate);
                return this;
            }

            @Override
            public EntityUpdateQuery<S> limit(long limit) {
                builder.limit(limit);
                return this;
            }

            @Override
            public EntityUpdateQuery<S> where(Filter<S> filter) {
                return Optional.ofNullable(filter)
                        .flatMap(f -> f.<S>toExpression(metaClass.asType()))
                        .map(this::where)
                        .orElse(this);
            }

            private <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> collectionOperation(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item, CollectionPropertyUpdateInfo.Operation operation) {
                builder.collectionPropertyUpdatesBuilder()
                        .add(CollectionPropertyUpdateInfo.create(property, item, operation));
                return this;
            }
        };
    }

    @Override
    public SelectQueryBuilder<S> query() {
        return new SelectQueryBuilder<S>() {
            private final ImmutableList.Builder<SortingInfo<S, ?, ? extends Comparable<?>>> sortingInfos = ImmutableList.builder();
            private final AtomicReference<ObjectExpression<S, Boolean>> predicate = new AtomicReference<>();
            private Long limit;
            private Long skip;

            @Override
            public <V extends Comparable<V>> SelectQueryBuilder<S> orderBy(PropertyExpression<S, ?, V> field, boolean ascending) {
                sortingInfos.add(SortingInfo.create(field, ascending));
                return this;
            }

            @Override
            public SelectQuery<S> select() {
                return select(ObjectExpression.arg(metaClass.asType()));
            }

            @Override
            public <T> SelectQuery<T> select(ObjectExpression<S, T> expression, boolean distinct) {
                return new SelectQuery<T>() {
                    private final QueryInfo.Builder<K, S, T> builder = QueryInfo.<K, S, T>builder()
                            .metaClass(metaClass)
                            .predicate(predicate.get())
                            .limit(limit)
                            .skip(skip)
                            .sorting(sortingInfos.build())
                            .mapping(expression)
                            .distinct(distinct);

                    @Override
                    public Maybe<T> first() {
                        QueryInfo<K, S, T> query = builder.limit(1L).build();
                        return queryProvider.query(query).singleElement();
                    }

                    @Override
                    public <R> Maybe<R> aggregate(Aggregator<T, T, R> aggregator) {
                        return queryProvider.aggregate(builder.build(), aggregator);
                    }

                    @Override
                    public SelectQuery<T> properties(Iterable<PropertyExpression<T, ?, ?>> properties) {
                        builder.propertiesAddAll(properties);
                        return this;
                    }

                    @Override
                    public Observable<T> retrieve() {
                        return queryProvider.query(builder.build());
                    }
                };
            }

            @Override
            public LiveSelectQuery<S> liveSelect() {
                return liveSelect(ObjectExpression.arg(metaClass.asType()));
            }

            @Override
            public <T> LiveSelectQuery<T> liveSelect(ObjectExpression<S, T> expression) {
                return new LiveSelectQuery<T>() {
                    private final QueryInfo.Builder<K, S, T> builder = QueryInfo.<K, S, T>builder()
                            .metaClass(metaClass)
                            .predicate(predicate.get())
                            .mapping(expression);

                    @Override
                    public Observable<T> first() {
                        QueryInfo<K, S, T> query = builder.limit(1L).build();
                        return queryProvider
                                .liveQuery(query)
                                .switchMapMaybe(n -> queryProvider.query(query).singleElement());
                    }

                    @Override
                    public Observable<List<T>> toList() {
                        QueryInfo<K, S, T> query = builder.build();
                        return queryProvider
                                .liveQuery(query)
                                .debounce(config.debounceTimeoutMillis(), TimeUnit.MILLISECONDS)
                                .concatMapSingle(n -> queryProvider
                                        .query(query)
                                        .toList());
                    }

                    @Override
                    public LiveSelectQuery<T> properties(Iterable<PropertyExpression<T, ?, ?>> properties) {
                        builder.propertiesAddAll(properties);
                        return this;
                    }

                    @Override
                    public <R> Observable<R> aggregate(Aggregator<T, T, R> aggregator) {
                        QueryInfo<K, S, T> query = builder.build();
                        return queryProvider.aggregate(query, aggregator)
                                .toObservable()
                                .concatWith(queryProvider.liveAggregate(query, aggregator))
                                .distinctUntilChanged();
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public <R> Observable<R> observeAs(QueryTransformer<T, R> queryTransformer) {
                        QueryInfo<K, S, T> sourceQuery = builder.build();

                        QueryInfo<K, S, S> observeQuery = QueryInfo.<K, S, S>builder()
                            .metaClass(sourceQuery.metaClass())
                            .predicate(sourceQuery.predicate())
                            .properties(Optional
                                .ofNullable(sourceQuery.mapping())
                                .<ImmutableList<PropertyExpression<S, ?, ?>>>map(mapping -> sourceQuery.properties()
                                    .stream()
                                    .map(prop -> Expressions.compose(mapping, prop))
                                    .collect(ImmutableList.toImmutableList()))
                                .orElse((ImmutableList<PropertyExpression<S, ?, ?>>)(ImmutableList<?>)sourceQuery.properties()))
                            .build();

                        QueryInfo<K, S, S> retrieveQuery = observeQuery.toBuilder()
                            .limit(limit)
                            .skip(skip)
                            .sortingAddAll(sortingInfos.build())
                            .build();

                        QueryInfo<K, S, T> transformQuery = sourceQuery.toBuilder()
                            .limit(limit)
                            .skip(skip)
                            .sortingAddAll(sortingInfos.build())
                            .build();

                        return queryProvider.aggregate(observeQuery, Aggregator.count())
                                .defaultIfEmpty(0L)
                                .map(AtomicLong::new)
                                .flatMapObservable(count -> {
                                    ObservableTransformer<List<Notification<S>>, R> transformer = queryTransformer
                                        .transformer(transformQuery, count);

                                    return queryProvider
                                            .query(retrieveQuery)
                                            .map(Notification::ofCreated)
                                            .toList()
                                            .toObservable()
                                            .compose(transformer)
                                            .concatWith(queryProvider.liveQuery(observeQuery)
                                                    .doOnNext(n -> updateCount(n, count))
                                                    .compose(Observables.bufferUntilIdle(Duration.ofMillis(config.debounceTimeoutMillis())))
                                                    .compose(transformer));
                                });
                    }

                    private void updateCount(Notification<S> notification, AtomicLong count) {
                        if (notification.isDelete()) {
                            count.decrementAndGet();
                        } else if (notification.isCreate()) {
                            count.incrementAndGet();
                        }
                    }

                    @Override
                    public Observable<Notification<T>> queryAndObserve() {
                        QueryInfo<K, S, T> query = builder.build();
                        return queryProvider
                                .query(query
                                        .toBuilder()
                                        .limit(limit)
                                        .skip(skip)
                                        .build())
                                .map(Notification::ofCreated)
                                .concatWith(queryProvider.liveQuery(query));
                    }

                    @Override
                    public Observable<Notification<T>> observe() {
                        return queryProvider.liveQuery(builder.build());
                    }
                };
            }

            @Override
            public SelectQueryBuilder<S> where(ObjectExpression<S, Boolean> predicate) {
                updatePredicate(this.predicate, predicate);
                return this;
            }

            @Override
            public SelectQueryBuilder<S> limit(long limit) {
                this.limit = limit;
                return this;
            }

            @Override
            public SelectQueryBuilder<S> where(Filter<S> filter) {
                return Optional.ofNullable(filter)
                        .flatMap(f -> f.<S>toExpression(metaClass.asType()))
                        .map(this::where)
                        .orElse(this);
            }

            @Override
            public SelectQueryBuilder<S> skip(long skip) {
                this.skip = skip;
                return this;
            }
        };
    }

    @Override
    public Single<S> update(S entity) {
        return queryProvider.insert(metaClass, Collections.singleton(entity))
                .andThen(Single.just(entity))
                .onErrorResumeNext(e ->
                        isConcurrencyException(e)
                        ? Single.defer(() -> queryProvider.insertOrUpdate(metaClass, entity))
                                .compose(Singles.backOffDelayRetry(
                                        DefaultEntitySet::isConcurrencyException,
                                        Duration.ofMillis(config.retryInitialDurationMillis()),
                                        config.retryCount()))
                        : Single.error(e));
    }

    @Override
    public Single<List<S>> update(Iterable<S> entities) {
        return queryProvider.insert(metaClass, entities)
                .andThen(Single.<List<S>>fromCallable(() -> ImmutableList.copyOf(entities)))
                .onErrorResumeNext(e -> isConcurrencyException(e)
                        ? Single.defer(() -> Observable
                                .fromIterable(entities)
                                .window(100)
                                .observeOn(Schedulers.newThread())
                                .concatMap(w -> w.flatMapSingle(this::update))
                                .toList())
                        : Single.error(e));
    }

    @Override
    public Maybe<S> update(K key, Function<Maybe<S>, Maybe<S>> updater) {
        Function<Maybe<S>, Maybe<S>> filteredUpdater = maybe -> {
            AtomicReference<S> entity = new AtomicReference<>();
            return updater.apply(maybe.doOnSuccess(entity::set))
                    .filter(e -> !Objects.equals(entity.get(), e))
                    .switchIfEmpty(Maybe.fromCallable(entity::get));
        };
        return Maybe.defer(() -> queryProvider.insertOrUpdate(metaClass, key, filteredUpdater))
                .compose(Maybes.backOffDelayRetry(
                        DefaultEntitySet::isConcurrencyException,
                        Duration.ofMillis(config.retryInitialDurationMillis()),
                        config.retryCount()));
    }

    private static boolean isConcurrencyException(Throwable exception) {
        log.debug("Checking exception: {}", exception.getMessage(), exception);
        return exception instanceof ConcurrentModificationException ||
                exception instanceof NoSuchElementException ||
                (exception instanceof CompositeException && ((CompositeException)exception)
                        .getExceptions()
                        .stream()
                        .anyMatch(DefaultEntitySet::isConcurrencyException));
    }

    private static <S> void updatePredicate(AtomicReference<ObjectExpression<S, Boolean>> current, ObjectExpression<S, Boolean> predicate) {
        current.updateAndGet(exp -> Optional
            .ofNullable(exp)
            .<ObjectExpression<S, Boolean>>map(ex -> BooleanExpression.and(ex, predicate))
            .orElse(predicate));
    }
}
