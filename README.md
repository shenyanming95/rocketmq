![](./docs/rocketmq-logo.svg)

# RocketMQ 4.8.0 源码分析

下载好源码目录，执行编译，运行命令：	

```shell script
mvn -Prelease-all -DskipTests clean install -U
```

配置过程：
- 新建一个工作目录`workdir`, 用来存放rocketmq的配置和数据存储目录
- 将源码目录./distribution/conf/下的 `logback_broker.xml` 和 `logback_namesrv.xml` 拷贝到 ./workdir/conf目录下(已完成)
- 再创建两个空目录：`./workdir/store`和`./workdir/store/commitlog`(需手动创建)
- 修改 `org.apache.rocketmq.broker.BrokerStartup` 和 `org.apache.rocketmq.namesrv.NamesrvStartup` 将 `ROCKETMQ_HOME` 指定为`./workdir`目录

启动过程：
- 启动 RocketMQ NameServer(org.apache.rocketmq.broker.BrokerStartup)
- 启动 RocketMQ Broker(org.apache.rocketmq.namesrv.NamesrvStartup)
- 启动 RocketMQ Producer(org.apache.rocketmq.example.quickstart.Producer)
- 启动 RocketMQ Consumer(org.apache.rocketmq.example.quickstart.Consumer)