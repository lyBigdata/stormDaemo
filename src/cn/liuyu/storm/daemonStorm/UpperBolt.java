package cn.liuyu.storm.daemonStorm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


//bolt组件，逻辑处理单元，接受消息，进行处理
public class UpperBolt extends BaseBasicBolt {
	
	//声明该bolt组件要发送出去的tuple字段
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("upperName"));
	}

	//主要的业务逻辑，接受上一个组件发送来的数据，进行业务处理；数据在tuple中
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		String stringTupleWord = tuple.getString(0);   //获取数据
		
		String upperWord=stringTupleWord.toUpperCase(); //将数据转化成大写
		
		//间数据封装成tuple,进行发送
		
		collector.emit(new Values(upperWord));
	}

}
