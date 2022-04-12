package route.impl;

import com.google.common.hash.Hashing;
import handler.RpcClientHandler;
import protocol.RpcProtocol;
import route.RpcLoadBalance;

import java.util.List;
import java.util.Map;


public class RpcLoadBalanceConsistentHash extends RpcLoadBalance {

    @Override
    public RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey);

        if (addressList != null && addressList.size() > 0) {
            return doRoute(serviceKey,addressList);
        } else {
            throw new Exception("Can not find connection for service:" + serviceKey);
        }
    }

    public RpcProtocol doRoute(String ServiceKey,List<RpcProtocol> addressList) {
        int index = Hashing.consistentHash(ServiceKey.hashCode(),addressList.size());
        return addressList.get(index);
    }
}
