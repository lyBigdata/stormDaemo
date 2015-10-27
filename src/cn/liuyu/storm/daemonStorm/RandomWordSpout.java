package cn.liuyu.storm.daemonStorm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


//消息源：spout，产生消息，并将其封装成tuple,以消息流
public class RandomWordSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;  //消息输出收集器
	
	Values values=new Values();
	
	String[]  words={"xiaomi","meizu","kupai"};
	
	@Override  //主要的业务逻辑，不断往下一个组件发送tuple消息
	public void nextTuple() {
		Random random=new Random(); 
		int index=random.nextInt(words.length);  //产生随机数
		
		//通过随机数，随机取出一个单词
		String word=words[index];
		
		//将数据封装成tuple,发送给下一个组件
		values.add(word);
		collector.emit(values);
		
		//每发送一个消息，休息500ms
		Utils.sleep(500);
	}

	@Override   //初始化,spout对象初始化时调用一次
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
	}

	@Override  //声明本Spout组件发送出去的tuple的字段
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name"));
	}
	
}
