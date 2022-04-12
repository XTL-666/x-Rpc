# x-Rpc

![image](https://user-images.githubusercontent.com/63628681/162931993-58fc5b7b-967d-4e83-af04-e3ee4f63a608.png)

<p>通过ZooKeeper实现服务注册和发现功能
<p>异步调用，支持Future机制，支持回调函数callback
<p>客户端使用TCP长连接（在多次调用共享连接）
<p>TCP心跳连接检测
<p>服务端异步多线程处理RPC请求
<p>支持不同的load balance策略
<p>支持不同的序列化/反序列化
