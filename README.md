## RocketMQ 4.8.0 源码分析：

下载好源码目录, 直接执行:
```shell script
mvn -Prelease-all -DskipTests clean install -U
```
搭建调试环境的过程：
- 启动 RocketMQ NameServer
- 启动 RocketMQ Broker
- 启动 RocketMQ Producer
- 启动 RocketMQ Consumer