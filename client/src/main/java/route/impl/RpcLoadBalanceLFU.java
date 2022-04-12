package route.impl;

import handler.RpcClientHandler;
import org.springframework.util.comparator.ComparableComparator;
import protocol.RpcProtocol;
import route.RpcLoadBalance;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RpcLoadBalanceLFU extends RpcLoadBalance {

    private ConcurrentHashMap<String, HashMap<RpcProtocol,Integer>> jobLfuMap = new ConcurrentHashMap<>();
    private long CACHE_VALID_TIME = 0;

    public  RpcProtocol doRoute(String serviceKey,List<RpcProtocol> addressList) {
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            jobLfuMap.clear();
            CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
        }


        HashMap<RpcProtocol,Integer> lfuItemMap = jobLfuMap.get(serviceKey);

        for (RpcProtocol address : addressList) {
            if(!lfuItemMap.containsKey(address) || lfuItemMap.get(address) > 1000000) {
                lfuItemMap.put(address,0);
            }
        }

        List<RpcProtocol> delKeys = new ArrayList<>();
        for (RpcProtocol existKey : lfuItemMap.keySet()) {
            if (!addressList.contains(existKey)) {
                delKeys.add(existKey);
            }
        }

        if(delKeys.size() > 0) {
            for(RpcProtocol delkey : delKeys) {
                lfuItemMap.remove(delkey);
            }
        }

        List<Map.Entry<RpcProtocol,Integer>> lfuItemList = new ArrayList<>(lfuItemMap.entrySet());
        Collections.sort(lfuItemList,new Comparator<Map.Entry<RpcProtocol, Integer>>() {
           @Override
           public int compare(Map.Entry<RpcProtocol,Integer> o1,Map.Entry<RpcProtocol,Integer> o2) {
               return o1.getValue().compareTo(o2.getValue());
           }
        });

        Map.Entry<RpcProtocol,Integer> addressItem = lfuItemList.get(0);
        RpcProtocol minAddress = addressItem.getKey();
        addressItem.setValue(addressItem.getValue() + 1);

        return minAddress;
    }

    @Override
    public RpcProtocol route(String serviceKey, Map<RpcProtocol, RpcClientHandler> connectedServerNodes) throws Exception {
        Map<String, List<RpcProtocol>> serviceMap = getServiceMap(connectedServerNodes);
        List<RpcProtocol> addressList = serviceMap.get(serviceKey);
        if (addressList != null && addressList.size() > 0) {
            return doRoute(serviceKey, addressList);
        } else {
            throw new Exception("Can not find connection for service: " + serviceKey);
        }
    }



}
