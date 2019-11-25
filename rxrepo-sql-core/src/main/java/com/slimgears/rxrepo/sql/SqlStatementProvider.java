package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface SqlStatementProvider {
    <K, S, T> SqlStatement forQuery(QueryInfo<K, S, T> queryInfo);

    <K, S, T, R> SqlStatement forAggregation(QueryInfo<K, S, T> queryInfo,
                                             ObjectExpression<T, R> aggregation,
                                             String projectedName);

    <K, S> SqlStatement forUpdate(UpdateInfo<K, S> updateInfo);

    <K, S> SqlStatement forDelete(DeleteInfo<K, S> deleteInfo);

    <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass,
                                  PropertyResolver propertyResolver,
                                  ReferenceResolver referenceResolver);

    <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass,
                                          PropertyResolver propertyResolver,
                                          ReferenceResolver referenceResolver);

    <K, S> SqlStatement forUpdate(MetaClassWithKey<K, S> metaClass,
                                  PropertyResolver propertyResolver,
                                  ReferenceResolver referenceResolver);

    <K, S> SqlStatement forDrop(MetaClassWithKey<K, S> metaClass);

    SqlStatement forDrop();

    default <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, ReferenceResolver referenceResolver) {
        return forInsertOrUpdate(metaClass, PropertyResolver.fromObject(metaClass, entity), referenceResolver);
    }

    default <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass, S entity, ReferenceResolver referenceResolver) {
        return forInsert(metaClass, PropertyResolver.fromObject(metaClass, entity), referenceResolver);
    }
}
