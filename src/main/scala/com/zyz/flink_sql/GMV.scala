package com.zyz.flink_sql

import com.zyz.utils.MyPropertiesUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig

object GMV {
  def main(args: Array[String]): Unit = {
    // 创建环境变量
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val properties = MyPropertiesUtil.getProperties("kafka_consumer.properties")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink_consumer_1")

    tableEnv.connect(
      new Kafka()
        .version("universal")
        .topic("abmau.w_order")
        .properties(properties)
    )
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("data", DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))
        .field("es", DataTypes.BIGINT()).rowtime(new Rowtime().watermarksPeriodicBounded(500))
      )
      .createTemporaryTable("w_order_binlog")

    tableEnv.connect(
      new Kafka()
        .version("universal")
        .topic("abmau.w_order_goods")
        .properties(properties)
    )
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("data", DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))
        .field("es", DataTypes.BIGINT()).rowtime(new Rowtime().watermarksPeriodicBounded(500))
      )
      .createTemporaryTable("w_order_goods_binlog")

    val order = tableEnv.sqlQuery(
      """
         select es, id, user_id, status, amount, created_at from (
           select
               es,
               d['id'] as id,
               d['user_id'] as user_id,
               cast(d['status'] as int) as status,
               cast(d['amount'] as decimal(10,2)) as amount,
               d['created_at'] as created_at,
               row_number() over(partition by d['id'] order by es desc) as rn
           from w_order_binlog CROSS JOIN unnest(data) as t (d)
        ) where rn = 1
    """.stripMargin)

    val orderGoods = tableEnv.sqlQuery(
      """
        select es, id, order_id, price, count_coll, market_price from (
           select
               es,
               d['id'] as id,
               d['order_id'] as order_id,
               d['brand_provider_level'] as level,
               cast(d['price'] as decimal(10,2)) as price,
               cast(d['count'] as int) as count_coll,
               cast(d['market_price'] as decimal(10,2)) as market_price,
               row_number() over(partition by d['id'] order by es desc) as rn
           from w_order_goods_binlog CROSS JOIN unnest(data) as t (d)
        ) where rn = 1
    """.stripMargin)

    tableEnv.createTemporaryView("w_order", order)
    tableEnv.createTemporaryView("w_order_goods", orderGoods)

    tableEnv.sqlQuery(
      """
        |select sum(market_price * count_coll), substring(created_at, 1, 10) day_coll
        |from w_order join w_order_goods on w_order.id = w_order_goods.order_id
        |where price > 0 and amount > 0 and status in (1,2,3,4)
        |group by substring(created_at, 1, 10)
      """.stripMargin).toRetractStream[Row].filter(_._1).map(_._2).print().setParallelism(1)


    env.execute("GMV")
  }
}