package com.slimgears.util.repository.expressions.internal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.slimgears.util.repository.expressions.BinaryOperationExpression;
import com.slimgears.util.repository.expressions.ObjectExpression;
import com.slimgears.util.repository.expressions.StringExpression;

@AutoValue
public abstract class StringBinaryOperationExpression<S, T1, T2> implements BinaryOperationExpression<S, T1, T2, String>, StringExpression<S> {
    @JsonCreator
    public static <S, T1, T2> StringBinaryOperationExpression<S, T1, T2> create(
            @JsonProperty("type") Type type,
            @JsonProperty("left") ObjectExpression<S, T1> left,
            @JsonProperty("right") ObjectExpression<S, T2> right) {
        return new AutoValue_StringBinaryOperationExpression<>(type, left, right);
    }
}
