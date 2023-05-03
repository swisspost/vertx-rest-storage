package org.swisspush.reststorage.util;

public class Result<TOk, TErr> {

    private final Type type;
    private final TOk okValue;
    private final TErr errValue;

    public static <TOk, TErr> Result<TOk, TErr> ok(TOk value) {
        return new Result<>(Type.OK, value, null);
    }

    public static <TOk, TErr> Result<TOk, TErr> err(TErr value) {
        return new Result<>(Type.ERROR, null, value);
    }

    private Result(Type type, TOk okValue, TErr errValue) {
        if (okValue != null && errValue != null) {
            throw new IllegalStateException("A result cannot be ok and error at the same time");
        }
        this.type = type;
        this.okValue = okValue;
        this.errValue = errValue;
    }

    /**
     * See https://doc.rust-lang.org/stable/std/result/enum.Result.html#method.unwrap
     * Returns the ok result or throws if this result is in error state.
     */
    public TOk unwrap() throws RuntimeException {
        if (isOk()) {
            return getOk();
        } else {
            throw new RuntimeException("Got an error result. Error value is '" + getErr() + "'");
        }
    }

    public boolean isOk() {
        return this.type == Type.OK;
    }

    public TOk getOk() throws IllegalStateException {
        if (!isOk()) {
            throw new IllegalStateException("Cannot call this method for results in error state");
        }
        return okValue;
    }

    public boolean isErr() {
        return !isOk();
    }

    public TErr getErr() throws IllegalStateException {
        if (isOk()) {
            throw new IllegalStateException("Cannot call this method for results in ok state");
        }
        return errValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Result<?, ?> result = (Result<?, ?>) o;

        if (type != result.type) return false;
        if (okValue != null ? !okValue.equals(result.okValue) : result.okValue != null) return false;
        return errValue != null ? errValue.equals(result.errValue) : result.errValue == null;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (okValue != null ? okValue.hashCode() : 0);
        result = 31 * result + (errValue != null ? errValue.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        if (isOk()) {
            return "ResultOk{" + okValue + '}';
        } else {
            return "ResultErr{" + errValue + '}';
        }
    }

    private enum Type {
        OK,
        ERROR
    }
}
