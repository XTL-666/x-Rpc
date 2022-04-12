package proxy;

import handler.RpcFuture;

public interface RpcService<T,P,FN extends SerializableFunction> {
    RpcFuture call(String funcName, Object... args) throws Exception;

    RpcFuture call(FN fn,Object... args) throws Exception;

}
