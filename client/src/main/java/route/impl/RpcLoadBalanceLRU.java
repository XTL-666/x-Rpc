package route.impl;

import handler.RpcClientHandler;
import protocol.RpcProtocol;
import route.RpcLoadBalance;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RpcLoadBalanceLRU extends RpcLoadBalance {

    private ConcurrentHashMap<String, LinkedHashMap<RpcProtocol,RpcProtocol>> jobLRUMap = new ConcurrentHashMap<>();
    private long CACHED_VALID_TIME = 0;

    public RpcProtocol doRoute(String serviceKey,List<RpcProtocol> addressList) {
        if(System.currentTimeMillis() > CACHED_VALID_TIME ) {
            jobLRUMap.clear();
            CACHED_VALID_TIME = System.currentTimeMillis() + 1000 * 24 * 60 * 60;
        }

        LinkedHashMap<RpcProtocol,RpcProtocol> lruHashMap = jobLRUMap.get(serviceKey);
        if (lruHashMap == null) {
            lruHashMap = new LinkedHashMap<RpcProtocol,RpcProtocol>(16,0.75f,true) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<RpcProtocol,RpcProtocol> eldest) {
                  if (super.size() > 1000) {
                      return true;
                  }else{
                      return false;
                  }
              }
            };
            jobLRUMap.putIfAbsent(serviceKey,lruHashMap);
        }

        for(RpcProtocol address : addressList) {
            if (!lruHashMap.containsKey(address)) {
                lruHashMap.put(address,address);
            }
        }

        List<RpcProtocol> delKeys = new ArrayList<>();
        for (RpcProtocol exisKey : lruHashMap.keySet()) {
            if (!addressList.contains(exisKey)) {
                delKeys.add(exisKey);
            }
        }

        if (delKeys.size() > 0) {
            for (RpcProtocol delKey : delKeys) {
                lruHashMap.remove(delKey);
            }
        }

        RpcProtocol eldestKey = lruHashMap.entrySet().iterator().next().getKey();
        RpcProtocol eldestValue = lruHashMap.get(eldestKey);
        return eldestValue;
    }


    @Override
    public RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey);

        if (addressList != null && addressList.size() > 0) {
            return doRoute(serviceKey,addressList);
        }else{
            throw new Exception("Can not find connection for service:" + serviceKey);
        }
    }
}
