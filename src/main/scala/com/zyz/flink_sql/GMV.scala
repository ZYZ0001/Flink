package com.zyz.flink_sql

import com.zyz.utils.MyPropertiesUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Schema}
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig

object GMV {
  def main(args: Array[String]): Unit = {
    // 创建环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
          .field("id", DataTypes.INT())
          .field("status", DataTypes.INT())
          .field("amount", DataTypes.DOUBLE())
          .field("paid_at", DataTypes.STRING())
          .field("created_at", DataTypes.STRING())
          .field("closed_at", DataTypes.STRING())
          .field("user_id", DataTypes.INT())
//          .field("data", DataTypes.INT())
      )
      .createTemporaryTable("w_order")

    val table = tableEnv.sqlQuery("select * from w_order")
    val out: DataStream[Row] = tableEnv.toAppendStream[Row](table)
    out.print("data")
  }
}
