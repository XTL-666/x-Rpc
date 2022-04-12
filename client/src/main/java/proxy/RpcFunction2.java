package proxy;

public interface RpcFunction2<T,P1,P2> extends SerializableFunction<T> {
    Object apply(T t,P1 p1,P2 p2);
}


