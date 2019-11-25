package com.slimgears.rxrepo.test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.query.*;
import com.slimgears.util.stream.Streams;
import com.slimgears.util.test.AnnotationRulesJUnit;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.CompletableSubject;
import org.junit.*;
import org.junit.rules.MethodRule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.slimgears.rxrepo.test.TestUtils.*;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRepositoryTest {
    @Rule
    public final TestName testNameRule = new TestName();
    @Rule
    public final MethodRule annotationRules = AnnotationRulesJUnit.rule();
    @Rule
    public final Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    private Repository repository;

    @Before
    public void setUp() {
        this.repository = createRepository();
        System.out.println("Starting test: " + testNameRule.getMethodName());
    }

    @After
    public void tearDown() {
        System.out.println("Test finished: " + testNameRule.getMethodName());
        this.repository.clearAndClose();
    }

    protected abstract Repository createRepository();

    @Test
    @Ignore
    public void testLiveSelectThenInsert() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);

        AtomicInteger counter = new AtomicInteger();

        TestObserver<Notification<Product>> productUpdatesTest = productSet
                .query()
                //.where(Product.$.price.greaterThan(110))
                .liveSelect()
                .queryAndObserve()
                .doOnNext(n -> System.out.println("Received notifications: " + counter.incrementAndGet()))
                .doOnSubscribe(d -> System.out.println("Subscribed for live query"))
                .test()
                .assertValueCount(0);

        TestObserver<Long> productCount = productSet
                .query()
                .liveSelect()
                .count()
                .doOnNext(c -> System.out.println("Count: " + c))
                .test()
                .assertValueCount(0);

        productSet.update(Products.createMany(2000))
                .test()
                .await()
                .assertNoErrors();

        productUpdatesTest
                .assertOf(countAtLeast(2000, Duration.ofSeconds(40)));

        productSet.delete().where(Product.$.key.id.betweenExclusive(1000, 1300))
                .execute()
                .test()
                .await()
                .assertNoErrors()
                .assertValue(299);

        productUpdatesTest
                .assertOf(countAtLeast(2200, Duration.ofSeconds(40)))
                .assertValueAt(2199, Notification::isDelete)
                .assertNoErrors();

        productCount
                .assertNoErrors();
    }

    @Test
    public void testAddAndRetrieveByKey() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Product product = Product.builder()
                .name("Product 1")
                .key(UniqueId.productId(1))
                .price(1001)
                .build();

        productSet.update(product).test().await().assertNoErrors();
        Assert.assertEquals(Long.valueOf(1), productSet.query().where(Product.$.key.eq(UniqueId.productId(1))).count().blockingGet());
    }

    @Test
    public void testAddAlreadyExistingObject() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Product product = Product.builder()
                .name("Product 1")
                .key(UniqueId.productId(1))
                .price(1001)
                .build();

        productSet.update(product).test().await().assertNoErrors();
        productSet.update(product).test().await().assertNoErrors();
        Assert.assertEquals(Long.valueOf(1), productSet.query().count().blockingGet());
    }

    @Test
    public void testAddRecursiveInventory() throws InterruptedException {
        Inventory inventory = Inventory.builder()
                .id(UniqueId.inventoryId(1))
                .name("Inventory 1")
                .inventory(Inventory.builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory 2")
                        .build())
                .build();

        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        inventorySet
                .update(inventory)
                .test()
                .await()
                .assertNoErrors();

        inventorySet.findAll()
                .sorted(Comparator.comparing(c -> c.id().id()))
                .test()
                .assertOf(countAtLeast(2))
                .assertValueAt(0, inventory);
    }

    @Test
    public void testAddAndRetrieveSingleEntity() {
        Product product = Products.createOne();
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        productSet.update(product)
                .ignoreElement()
                .blockingAwait();

        productSet.findAll()
                .test()
                .assertOf(countAtLeast(1))
                .assertValue(product);
    }

    @Test
    public void testAddSameInventory() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        List<Product> products = Arrays.asList(
                Product.builder()
                        .name("Product 1")
                        .key(UniqueId.productId(1))
                        .price(1001)
                        .inventory(Inventory
                                .builder()
                                .id(UniqueId.inventoryId(2))
                                .name("Inventory 2")
                                .build())
                        .build(),
                Product.builder()
                        .name("Product 2")
                        .key(UniqueId.productId(2))
                        .price(1002)
                        .inventory(Inventory
                                .builder()
                                .id(UniqueId.inventoryId(2))
                                .name("Inventory 2")
                                .build())
                        .build());

        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValueAt(0, items -> items.size() == 2)
                .assertValueAt(0, items -> items.stream().anyMatch(p -> Objects.equals(p.name(), "Product 1")))
                .assertValueAt(0, items -> items.stream().anyMatch(p -> Objects.equals(p.name(), "Product 2")));

        Assert.assertEquals(Long.valueOf(1), inventorySet.query().count().blockingGet());
    }

    @Test
    public void testInsertThenLiveSelectShouldReturnAdded() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        productSet.update(products).test().await();

        productSet.query()
                .liveSelect()
                .queryAndObserve()
                .doOnNext(System.out::println)
                .test()
                .assertOf(countAtLeast(200))
                .assertValueAt(10, NotificationPrototype::isCreate);
    }

    @Test
    //@UseLogLevel(LogLevel.TRACE)
    public void testInsertThenLiveSelectCountShouldReturnCount() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        productSet.update(products).test().await();

        TestObserver<Long> countObserver = productSet.query()
                .liveSelect()
                .count()
                .test();

        countObserver
                .assertOf(countAtLeast(1))
                .assertValueAt(0, c -> c == 200);

        productSet.delete()
                .where(Product.$.searchText("Product 1"))
                .execute()
                .test()
                .await()
                .assertValue(119);

        countObserver
                .assertOf(countAtLeast(2))
                .assertValueAt(1, 81L);
    }

    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testAtomicUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        CompletableSubject trigger1 = CompletableSubject.create();
        CompletableSubject trigger2 = CompletableSubject.create();

        productSet.update(Products.createMany(1)).test().await().assertNoErrors();

        TestObserver<Product> prodUpdateTester1 = productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name(p.name() + " Updated name #1").build())
                                .delay(trigger1.toFlowable())))
                .test();

        Thread.sleep(500);

        TestObserver<Product> prodUpdateTester2 = productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name(p.name() + " Updated name #2").build())
                                .delay(trigger2.toFlowable())))
                .test();

        Thread.sleep(500);
        trigger1.onComplete();
        prodUpdateTester1.await().assertNoErrors();

        trigger2.onComplete();
        prodUpdateTester2
                .await()
                .assertNoErrors()
                .assertValue(p -> "Product 0 Updated name #1 Updated name #2".equals(p.name()));
    }

    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testFilteredAtomicUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);

        productSet.update(Products.createMany(1)).test().await().assertNoErrors();
        productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name("Product " + p.key().id() + " Updated name").build())))
                .test()
                .await()
                .assertComplete()
                .assertValueCount(1);

        productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name("Product " + p.key().id() + " Updated name").build())))
                .test()
                .await()
                .assertComplete()
                .assertValueCount(1);
    }

    @Test
    public void testInsertThenRetrieve() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        System.out.println("Duration: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        productSet.query()
                .where(Product.$.name.contains("21"))
                .select()
                .count()
                .test()
                .await()
                .assertValue(2L);

        productSet.query()
                .where(Product.$.name.contains("21"))
                .select(Product.$.price)
                .aggregate(Aggregator.sum())
                .test()
                .await()
                .assertValue(212);

        productSet
                .query()
                .where(Product.$.name.contains("131"))
                .select()
                .retrieve(Product.$.key, Product.$.price, Product.$.inventory.id, Product.$.inventory.name)
                .test()
                .await()
                .assertNoErrors()
                .assertValue(p -> p.name() == null)
                .assertValue(p -> p.key().id() == 131)
                .assertValue(p -> p.price() == 151)
                .assertValue(p -> "Inventory 11".equals(requireNonNull(p.inventory()).name()))
                .assertValueCount(1);

        productSet
                .query()
                .where(Product.$.type.in(ProductPrototype.Type.ComputerSoftware, ProductPrototype.Type.ComputeHardware))
                .skip(20)
                .retrieve()
                .test()
                .await()
                .assertValueCount(113);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenSearch() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(100);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .query()
                .where(Product.$.searchText("Product 31 ComputeHardware"))
                .select()
                .retrieve(Product.$.key, Product.$.name, Product.$.price, Product.$.type, Product.$.inventory.id, Product.$.inventory.name)
                .doOnNext(System.out::println)
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1);
    }

    @Test
    public void testQueryWithEmptyMapping() {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(10);
        productSet.update(products).ignoreElement().blockingAwait();
        productSet.query()
                .where(Product.$.name.startsWith("Product"))
                .select(ObjectExpression.arg(Product.class))
                .retrieve()
                .test()
                .assertOf(countExactly(10));

        productSet.query()
                .where(Product.$.name.startsWith("Product"))
                .limit(1)
                .observeAsList()
                .take(1)
                .test()
                .assertOf(countExactly(1))
                .assertValue(l -> l.size() == 1);
    }

    @Test
    @Ignore
    public void testInsertThenUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .update()
                .set(Product.$.name, Product.$.name.concat(" - ").concat(Product.$.inventory.name.asString()))
                .where(Product.$.key.id.betweenExclusive(100, 150))
                .limit(20)
                .execute()
                .test()
                .await()
                .assertNoErrors()
                .assertNoTimeout()
                .assertValue(20);

        productSet
                .query()
                .where(Product.$.key.id.betweenExclusive(100, 150))
                .limit(20)
                .retrieve()
                .test()
                .awaitCount(20)
                .assertNoErrors()
                .assertNoTimeout()
                .assertValueAt(15, pr -> {
                    System.out.println(pr);
                    Matcher matcher = Pattern.compile("Product ([0-9]+) - Inventory ([0-9]+)").matcher(requireNonNull(pr.name()));
                    return matcher.matches() &&
                            Integer.parseInt(matcher.group(1)) == pr.key().id() &&
                            Integer.parseInt(matcher.group(2)) == requireNonNull(pr.inventory()).id().id();
                });
    }

    @Test
    public void testObserveReferencedObjectProperties() {
        repository.entities(Product.metaClass)
                .update(Products.createOne().toBuilder().inventory(null).build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .query()
                .liveSelect()
                .properties(Product.$.inventory.name)
                .observeAs(Notifications.toList())
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 1)
                .assertValue(l -> l.get(0).inventory() == null);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testPartialRetrieve() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(10);
        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .query()
                .where(Product.$.price.lessOrEqual(115))
                .orderBy(Product.$.key.id)
                .orderBy(Product.$.productionDate)
                .retrieve(Product.$.name)
                .doOnNext(System.out::println)
                .test()
                .await()
                .assertNoErrors()
                .assertValueAt(1, p -> "Product 1".equals(p.name()));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEntityWithListOfReferenceField() throws InterruptedException {
        EntitySet<UniqueId, Storage> storages = repository.entities(Storage.metaClass);
        storages.update(Storage.builder()
                .key(UniqueId.storageId(1))
                .productList(ImmutableList.copyOf(Products.createMany(10)))
                .build())
                .test()
                .await()
                .assertNoErrors();

        storages.findAll()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValue(s -> s.productList().size() == 10);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEntityWithStringByReferenceMapField() throws InterruptedException {
        EntitySet<UniqueId, Storage> storages = repository.entities(Storage.metaClass);
        storages.update(Storage.builder()
                .key(UniqueId.storageId(1))
                .productMapByName(Streams
                        .fromIterable(Products.createMany(10))
                        .collect(ImmutableMap.toImmutableMap(Product::name, p -> p)))
                .build())
                .test()
                .await()
                .assertNoErrors();

        storages.findAll()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValue(s -> s.productMapByName().size() == 10);
    }

    @Test
    public void testEntityWithListOfStringField() throws InterruptedException {
        Product product = Product.builder()
                .key(UniqueId.productId(1))
                .name("Product1")
                .inventory(Inventory
                        .builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory2")
                        .build())
                .price(100)
                .type(ProductPrototype.Type.ComputeHardware)
                .aliases(ImmutableList.of("p1", "p2"))
                .build();

        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products
                .update(product)
                .test()
                .await()
                .assertNoErrors();

        products
                .findAll()
                .test()
                .await()
                .assertValue(p -> requireNonNull(p.aliases()).size() == 2);
    }

    @Test
    public void testFilterByNestedCompoundKey() throws InterruptedException {
        repository.entities(Product.metaClass).update(Products.createMany(10)).test().await();
        repository.entities(Product.metaClass).query()
                .where(Product.$.inventory.id.eq(UniqueId.inventoryId(0)))
                .retrieve()
                .test()
                .await()
                .assertValueCount(10);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEntityWithListOfEmbeddedField() throws InterruptedException {
        Product product = Product.builder()
                .key(UniqueId.productId(1))
                .name("Product1")
                .inventory(Inventory
                        .builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory2")
                        .build())
                .price(100)
                .type(ProductPrototype.Type.ComputeHardware)
                .relatedIds(ImmutableList.of(UniqueId.storageId(3), UniqueId.inventoryId(2), UniqueId.productId(1)))
                .build();

        repository.entities(Product.metaClass)
                .update(product)
                .test()
                .await()
                .assertNoErrors();

        repository.entities(Product.metaClass)
                .findAll()
                .take(1)
                .singleElement()
                .test()
                .await()
                .assertValue(p -> p.relatedIds().size() == 3)
                .assertValue(p -> p.relatedIds().get(0).id() == 3);
    }

    @Test
    public void testInsertThenQueryValueIn() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .await();

        repository.entities(Product.metaClass)
                .query()
                .where(Product.$.key.in(UniqueId.productId(2), UniqueId.productId(3)))
                .retrieve()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(2);
    }

    @Test
    public void testFilteredLiveQuery() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);

        products.update(Product
                .builder()
                .key(UniqueId.productId(1))
                .name("Product 1")
                .price(101)
                .build())
                .ignoreElement()
                .blockingAwait();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product 2")
                .price(99)
                .build())
                .ignoreElement()
                .blockingAwait();


        TestObserver<Notification<Product>> productObserver = products
                .query()
                .where(Product.$.price.greaterThan(100))
                .queryAndObserve()
                .doOnNext(System.out::println)
                .test();

        productObserver
                .assertOf(countAtLeast(1))
                .assertValueAt(0, Notification::isCreate)
                .assertValueAt(0, n -> requireNonNull(n.newValue()).price() == 101);

        productObserver
                .assertOf(countLessThan(2))
                .assertTimeout();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product 2")
                .price(102)
                .build())
                .ignoreElement().blockingAwait();

        productObserver
                .assertOf(countExactly(2))
                .assertValueAt(1, Notification::isCreate)
                .assertValueAt(1, n -> requireNonNull(n.newValue()).price() == 102);

        products.update(Product
                .builder()
                .key(UniqueId.productId(1))
                .name("Product 1")
                .price(95)
                .build())
                .ignoreElement().blockingAwait();

        productObserver
                .assertOf(countExactly(3))
                .assertValueAt(2, NotificationPrototype::isDelete)
                .assertValueAt(2, n -> requireNonNull(n.oldValue()).price() == 101);

        products.update(Product
                .builder()
                .key(UniqueId.productId(3))
                .name("Product 3")
                .price(92)
                .build())
                .ignoreElement().blockingAwait();

        productObserver.assertOf(countLessThan(4));
    }

    @Test
    public void testDistinctSelect() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(20)).ignoreElement().blockingAwait();
        products.query()
                .selectDistinct(Product.$.inventory.name)
                .retrieve()
                .doOnNext(System.out::println)
                .toList()
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 2)
                .assertValue(l -> l.contains("Inventory 0"));

        products.query()
                .selectDistinct(Product.$.inventory)
                .retrieve(Inventory.$.name)
                .toList()
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 2);
    }

    @Test
    public void testAggregateMinDate() {
        List<Product> productList = ImmutableList.copyOf(Products.createMany(10));
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(productList).ignoreElement().blockingAwait();
        Date maxDate = products.query()
                .select(Product.$.productionDate)
                .aggregate(Aggregator.max())
                .blockingGet();

        Date expectedMinDate = productList.get(0).productionDate();
        Date expectedMaxDate = productList.get(productList.size() - 1).productionDate();

        Date minDate = products.query()
                .select(Product.$.productionDate)
                .aggregate(Aggregator.min())
                .blockingGet();

        Assert.assertEquals(expectedMaxDate, maxDate);
        Assert.assertEquals(expectedMinDate, minDate);
    }

    @Test
    public void testAggregateAveragePrice() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).ignoreElement().blockingAwait();
        double averagePrice = products.query()
                .select(Product.$.price)
                .aggregate(Aggregator.average())
                .blockingGet();

        Assert.assertEquals(116.2, averagePrice, 0.0001);
    }

    @Test
    public void testFilterByDate() throws InterruptedException {
        List<Product> productList = ImmutableList.copyOf(Products.createMany(10));
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(productList).ignoreElement().blockingAwait();
        products.query()
                .where(Product.$.productionDate.lessOrEqual(productList.get(4).productionDate()))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5);
    }

    @Test
    public void testQueryByNestedEmbeddedObject() throws InterruptedException {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(20)).ignoreElement().blockingAwait();
        UniqueId vendorId = UniqueId.vendorId(2);
        products.query()
                .where(Product.$.vendor.id.eq(vendorId))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> requireNonNull(p.vendor()).id().equals(vendorId));

        products.query()
                .where(Product.$.vendor.id.in(vendorId))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> requireNonNull(p.vendor()).id().equals(vendorId));
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testObserveAsList() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).ignoreElement().blockingAwait();
        TestObserver<List<Product>> productTestObserver = products.query()
                .orderBy(Product.$.name)
                .orderByDescending(Product.$.price)
                .limit(3)
                .skip(2)
                .observeAs(Notifications.toSlidingList())
                .doOnNext(l -> {
                    System.out.println("List received: ");
                    l.forEach(System.out::println);
                })
                .test()
                .assertOf(countAtLeast(1))
                .assertValueAt(0, l -> l.size() == 3)
                .assertValueAt(0, l -> Objects.equals(l.get(0).name(), "Product 2"))
                .assertValueAt(0, l -> Objects.equals(l.get(2).name(), "Product 4"));

        products.update(Arrays.asList(
                Product.builder()
                        .name("Product 3-1")
                        .key(UniqueId.productId(11))
                        .price(100)
                        .type(ProductPrototype.Type.ComputeHardware)
                        .build(),
                Product.builder()
                        .name("Product 1-1")
                        .key(UniqueId.productId(13))
                        .price(100)
                        .type(ProductPrototype.Type.ComputeHardware)
                        .build(),
                Product.builder()
                        .name("Product 5-1")
                        .key(UniqueId.productId(12))
                        .price(100)
                        .type(ProductPrototype.Type.ComputeHardware)
                        .build()))
                .ignoreElement().blockingAwait();

        productTestObserver
                .assertOf(countAtLeast(2))
                .assertValueAt(1, l -> l.size() == 3)
                .assertValueAt(1, l -> Objects.equals(l.get(0).name(), "Product 2"))
                .assertValueAt(1, l -> Objects.equals(l.get(1).name(), "Product 3"))
                .assertValueAt(1, l -> Objects.equals(l.get(2).name(), "Product 3-1"));
    }

    @Test
    public void testObserveAsListWithProperties() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).ignoreElement().blockingAwait();
        products.query()
                .orderBy(Product.$.name)
                .limit(3)
                .skip(2)
                .observeAsList(Product.$.key, Product.$.name)
                .doOnNext(l -> {
                    System.out.println("List received: ");
                    l.forEach(System.out::println);
                })
                .test()
                .assertOf(countAtLeast(1))
                .assertValue(l -> l.size() == 3)
                .assertValue(l -> Objects.isNull(l.get(0).type()))
                .assertValue(l -> Objects.equals(l.get(0).name(), "Product 2"))
                .assertValue(l -> Objects.equals(l.get(2).name(), "Product 4"));
    }

    @Test
    public void testObserveAsListWithPredicate() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        Product product2 = Product.builder()
                .name("Product 2")
                .key(UniqueId.productId(2))
                .price(100)
                .type(ProductPrototype.Type.ComputeHardware)
                .build();

        products.update(product2).ignoreElement().blockingAwait();

        TestObserver<List<Product>> productTestObserver = products.query()
                .where(Product.$.price.eq(100))
                .observeAsList()
                .doOnNext(l -> {
                    System.out.println("List received: ");
                    l.forEach(System.out::println);
                })
                .test()
                .assertOf(countAtLeast(1))
                .assertValueAt(0, l -> l.size() == 1);

        Product product1 = Product.builder()
                .name("Product 1")
                .key(UniqueId.productId(1))
                .price(100)
                .type(ProductPrototype.Type.ComputeHardware)
                .build();

        products.update(product1).ignoreElement().blockingAwait();

        productTestObserver
                .assertOf(countAtLeast(2))
                .assertValueAt(1, l -> l.size() == 2)
                .assertValueAt(1, l -> Objects.equals(l.get(0).name(), "Product 2"))
                .assertValueAt(1, l -> Objects.equals(l.get(1).name(), "Product 1"));

        products.update(product1.toBuilder().name("Product 1-1").build()).ignoreElement().blockingAwait();

        productTestObserver
                .assertOf(countAtLeast(3))
                .assertValueAt(2, l -> l.size() == 2);
    }

    @Test
    public void testRetrieveAsListWithProperties() throws InterruptedException {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).ignoreElement().blockingAwait();
        products.query()
                .orderBy(Product.$.name)
                .limit(3)
                .skip(2)
                .retrieveAsList(Product.$.key, Product.$.name)
                .test()
                .await()
                .assertNoErrors()
                .assertComplete()
                .assertValue(l -> l.size() == 3)
                .assertValue(l -> Objects.isNull(l.get(0).type()))
                .assertValue(l -> Objects.equals(l.get(0).name(), "Product 2"))
                .assertValue(l -> Objects.equals(l.get(2).name(), "Product 4"));
    }

    @Test
    public void testLiveQueryWithProjection() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .awaitCount(10)
                .assertNoErrors();

        TestObserver<Notification<Product>> productNameChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.name)
                .doOnNext(System.out::println)
                .test();

        TestObserver<Notification<Product>> productTypeChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.type)
                .doOnNext(System.out::println)
                .test();

        TestObserver<Notification<Product>> productNameAndTypeChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.name, Product.$.type)
                .doOnNext(System.out::println)
                .test();

        productNameChanges.assertOf(countExactly(10));
        productNameAndTypeChanges.assertOf(countExactly(10));
        productTypeChanges.assertOf(countExactly(10));

        repository.entities(Product.metaClass)
                .update(UniqueId.productId(1), productMaybe -> productMaybe
                        .map(p -> p.toBuilder().name(p.name() + " - new name").build()))
                .test()
                .await()
                .assertValue(p -> "Product 1 - new name".equals(p.name()))
                .assertNoErrors();

        productNameChanges.assertOf(countExactly(11));
        productNameAndTypeChanges.assertOf(countExactly(11));
        productTypeChanges.assertOf(countExactly(10));

        productNameChanges
                .assertValueAt(10, NotificationPrototype::isModify)
                .assertValueAt(10, n -> "Product 1".equals(requireNonNull(n.oldValue()).name()))
                .assertValueAt(10, n -> "Product 1 - new name".equals(requireNonNull(n.newValue()).name()));

        repository.entities(Product.metaClass)
                .update(UniqueId.productId(1), productMaybe -> productMaybe
                        .map(p -> p.toBuilder().type(ProductPrototype.Type.ComputerSoftware).build()))
                .test()
                .await()
                .assertValue(p -> ProductPrototype.Type.ComputerSoftware.equals(p.type()))
                .assertNoErrors();

        productTypeChanges.assertOf(countExactly(11));
        productNameAndTypeChanges.assertOf(countExactly(12));
        productNameChanges.assertValueCount(11);

        productTypeChanges
                .assertValueAt(10, NotificationPrototype::isModify)
                .assertValueAt(10, n -> ProductPrototype.Type.ComputeHardware.equals(requireNonNull(n.oldValue()).type()))
                .assertValueAt(10, n -> ProductPrototype.Type.ComputerSoftware.equals(requireNonNull(n.newValue()).type()));
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testLiveSelectWithMapping() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .await()
                .assertNoErrors();

        TestObserver<List<Inventory>> inventoriesObserver = repository.entities(Product.metaClass)
                .query()
                .liveSelect(Product.$.inventory)
                .observeAs(Notifications.toList())
                .doOnNext(n -> n.forEach(System.out::println))
                .debounce(500, TimeUnit.MILLISECONDS)
                .test()
                .assertSubscribed();

        inventoriesObserver
                .assertOf(countAtLeast(1))
                .assertNoTimeout()
                .assertNoErrors()
                .assertValueAt(0, l -> l.size() == 10)
                .assertValueAt(0, l -> l.get(0) != null);

        repository.entities(Product.metaClass)
                .update(Products.createMany(11))
                .ignoreElement()
                .blockingAwait();

        inventoriesObserver
                .assertOf(countAtLeast(2))
                .assertNoTimeout()
                .assertNoErrors()
                .assertValueAt(1, l -> l.size() == 11)
                .assertValueAt(1, l -> l.get(10) != null);
    }

    @Test
    public void testLiveAggregateWithMapping() {
        TestObserver<Long> inventoriesObserver = repository.entities(Product.metaClass)
                .query()
                .map(Product.$.inventory)
                .observeCount()
                .filter(c -> c > 0)
                .test()
                .assertSubscribed();

        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .awaitCount(10)
                .assertNoErrors();

        inventoriesObserver
                .assertOf(countAtLeast(1))
                .assertNoTimeout()
                .assertNoErrors();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testLiveQueryThenUpdate() {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .ignoreElement()
                .blockingAwait();

        TestObserver<Notification<Product>> productObserver = repository.entities(Product.metaClass)
                .query()
                .where(Product.$.name.lessThan("Product 5"))
                .liveSelect()
                .observe(Product.$.name, Product.$.price)
                .test()
                .assertSubscribed();

        Product product1 = repository.entities(Product.metaClass)
                .find(UniqueId.productId(1))
                .blockingGet();

        Product product8 = repository.entities(Product.metaClass)
                .find(UniqueId.productId(8))
                .blockingGet();

        repository.entities(Product.metaClass)
                .update(product8.toBuilder().name(product8.name() + " - Updated").build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .update(product1.toBuilder().name(product1.name() + " - Updated").build())
                .ignoreElement()
                .blockingAwait();

        productObserver.awaitCount(1)
                .assertValueAt(0, NotificationPrototype::isModify)
                .assertValueAt(0, p -> p.oldValue().name().equals("Product 1"))
                .assertValueAt(0, p -> p.newValue().name().equals("Product 1 - Updated"));

        repository.entities(Product.metaClass)
                .update(product1.toBuilder().productionDate(new Date(product1.productionDate().getTime() + 1)).build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .update(product1.toBuilder().price(product1.price() + 1).build())
                .ignoreElement()
                .blockingAwait();

        productObserver.awaitCount(2)
                .assertValueAt(1, NotificationPrototype::isModify)
                .assertValueAt(1, p -> p.newValue().productionDate().getTime() - p.oldValue().productionDate().getTime() == 1);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testRetrieveWithReferenceProperty() {
        TestObserver<Product> productTestObserver = repository.entities(Product.metaClass)
                .query()
                .limit(1)
                .liveSelect()
                .properties(Product.$.inventory)
                .observe()
                .filter(Notification::isCreate)
                .map(Notification::newValue)
                .take(1)
                .test();

        productTestObserver.assertValueCount(0);

        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .ignoreElement()
                .blockingAwait();

        productTestObserver
                .assertOf(countExactly(1))
                .assertValue(p -> p.inventory() != null);

        repository.entities(Product.metaClass)
                .query()
                .limit(1)
                .select()
                .properties(Product.$.inventory)
                .retrieve()
                .take(1)
                .test()
                .assertOf(countExactly(1))
                .assertValue(p -> p.inventory() != null);
    }

    @Test
    @UseLogLevel(LogLevel.DEBUG)
    public void testLargeUpdate() throws InterruptedException {
        Observable.fromIterable(Products.createMany(2000))
                .flatMapSingle(repository.entities(Product.metaClass)::update)
                .ignoreElements()
                .test()
                .await()
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void testSearchWithSpecialSymbols() throws InterruptedException {
        repository.entities(Product.metaClass).update(Products
                .createOne()
                .toBuilder()
                .name("Product 1 with special symbols, for example: colon, coma & ampersand")
                .build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .query()
                .where(Product.$.searchText("example : colon"))
                .retrieve()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1);
    }

    @Test
    @UseLogLevel(LogLevel.DEBUG)
    public void testUpdatingDeletedObjectShouldNotAddObjectIntoRepo() throws InterruptedException {

        EntitySet<UniqueId, Product> entities = repository.entities(Product.metaClass);
        Product product = Products
                .createOne()
                .toBuilder()
                .name("TestProduct")
                .build();

        // add stub product to the repo
        entities.update(product)
                .ignoreElement()
                .blockingAwait();

        AtomicBoolean deleted = new AtomicBoolean(false);
        entities.update(product.key(), productMaybe -> productMaybe.map(p -> {
            // deleting the object from the repo just before update
            if (deleted.compareAndSet(false, true))
                entities.delete(p.key()).blockingAwait();

            return p.toBuilder().name("updatedName").build();
        }))
                .test()
                .await()
                .assertValueCount(0)
                .assertComplete();

        //verify that there is nothing in the collection
        entities.query()
                .count()
                .test()
                .await()
                .assertValue(0L);
    }
}
